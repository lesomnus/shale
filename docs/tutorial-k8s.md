# Tutorial — Kubernetes, on a disk you are borrowing

Two things, in the order they happen. First, standing a deployment up on
storage that is not yours to keep: a dataset on a file server, because the
disks you ordered have not arrived. Then changing the disk under it when
they do, without stopping the recording, throwing the old recordings away
because they were only ever a test.

This is a walk-through. The reference behind each step is
[`deploy/k8s/README.md`](../deploy/k8s/README.md) for the manifests,
[§34.5](11-deployment.md#345-kubernetes) for the deployment,
[§22.2](07-storage-node.md#222-sinks-and-devices) for sinks and devices, and
[§28.4](09-operations.md#284-changing-the-disk-under-a-node) for the swap.

It assumes a cluster you can label nodes in, `kubectl`, the image somewhere
the nodes pull it from, and the CLI on your own machine (`go build
./cmd/shale`). The storage here is a ZFS dataset on a machine that is also a
node of the cluster; [the deployment's
README](../deploy/k8s/README.md#a-disk-that-is-not-on-a-kubernetes-node) has
what to do when it is not.

## 1. The disk, before Kubernetes hears about it

A sink is a directory, and Shale never mounts anything: the machine has the
disk mounted before the pod starts.

```sh
zfs create -o recordsize=1M -o compression=off -o xattr=sa -o dnodesize=auto \
           -o atime=off -o primarycache=metadata -o logbias=throughput \
           -o quota=2T -o mountpoint=/srv/shale/borrowed tank/shale
```

[§22.2](07-storage-node.md#222-sinks-and-devices) says what each property is
for. Two of them decide whether this works at all:

- **`xattr=sa`.** A lamina describes itself in a user xattr, which is how a
  sink is read back without a database ([§23.1](07-storage-node.md#231-self-describing-laminae)).
  ZFS stores extended attributes as hidden directories unless told
  otherwise — an extra seek per lamina. A filesystem with no user xattrs at
  all is refused outright: the node says so and does not start.
- **`quota`.** It is the sink's capacity: the node reads it when it opens
  the sink, so nothing has to be declared twice. Without a quota Shale counts
  the whole pool as its own and fills it.

Check what the node is about to check:

```sh
setfattr -n user.shale -v 1 /srv/shale/borrowed && getfattr -n user.shale /srv/shale/borrowed
```

**Leave headroom in the quota.** Garbage collection reclaims laminae that
have expired and nothing else ([§21](06-retention-gc.md#21-lazy-gc)), so a
sink does not free space on demand; below 3% free it stops taking writes
altogether. Size the retention so the recordings settle at about 90% of the
quota, not at 100% of it.

One more thing to know before it matters: on ZFS the **pool** is the device,
not the dataset. Two datasets of one pool are two sinks on one device, which
buys no failure-domain spread, and the node says so in its heartbeat.

## 2. Say which machine, and where the disk is

Kubernetes has no idea of a disk. Three places name it and they have to
agree — the label, the hostPath, the path in the node's configuration:

```sh
kubectl label node fileserver shale.io/storage=true
```

```yaml
# deploy/k8s/storage.yaml
          volumeMounts:
            - { name: sink-0, mountPath: /srv/shale/sinks/0 }
      volumes:
        - name: sink-0
          hostPath:
            path: /srv/shale/borrowed   # the dataset's mountpoint
            type: Directory             # not DirectoryOrCreate
```

```yaml
# deploy/k8s/storage-config.yaml
    storage:
      sinks:
        - path: /srv/shale/sinks/0      # where it is mounted in the pod
```

`type: Directory` is the one to get right. With `DirectoryOrCreate` — which
the manifests ship, because their sink is a lab directory — a dataset that
is not mounted is created as an empty directory on the OS disk, and the node
registers *that* as a new, empty sink and starts filling the system's own
filesystem. With `Directory` the pod refuses to start, which is the answer
you want.

No `capacity:` is written here because the dataset's quota is one. A
directory on a filesystem shared with anything else needs one
([§22.2](07-storage-node.md#222-sinks-and-devices)).

The DaemonSet and the ConfigMap are shared by every node the label matches,
so all of them must have the disk at the same path. A machine whose disks
are laid out differently gets its own label, DaemonSet and ConfigMap.

## 3. Apply, and watch it come up

The rest of the first apply — the image, the names in `control-config.yaml`,
the database password — is
[the deployment's README](../deploy/k8s/README.md#before-applying).

```sh
kubectl apply -k deploy/k8s
kubectl -n shale get pods -w
kubectl -n shale logs job/shale-init      # the CA fingerprint, and where the passwords are
kubectl -n shale get secret shale-control-passwords -o jsonpath='{.data.cluster\.ops}' | base64 -d
kubectl -n shale get secret shale-control-passwords -o jsonpath='{.data.acme\.admin}' | base64 -d
```

The two passwords are in a Secret of their own and not in the log, which
whatever collects logs would keep. Keep them and the CA fingerprint; the
console and every producer want them. Then delete the Secret
(`kubectl -n shale delete secret shale-control-passwords`), which nothing
reads, and sign in from a machine that reaches a node:

```sh
kubectl -n shale get secret shale-control -o jsonpath='{.data.ca\.crt}' | base64 -d > ca.crt
shale --addr https://10.1.2.74:30400 --config /dev/null login --password @acme/admin
```

The node joins by itself and is adopted at once (`auto_adopt: true`; with it
off, `shale node pending` then `shale node adopt`). What matters is the sink
it reports:

```sh
shale sink ls
shale sink get @sink-<id>
```

```text
path              /srv/shale/sinks/0
capacity          2199023255552
free              2198821928960
pressure          PRESSURE_NORMAL
capabilities
  xattr         true
  inline_record true
  odirect       false
  fallocate     false
  filesystem    zfs
attachment        SINK_ATTACHMENT_ATTACHED
accept_writes     true
```

Read the capabilities block once, here, rather than wondering later.
`xattr` must be true — nothing works without it. `inline_record` false means
`xattr=sa` was forgotten. On ZFS `odirect` is true only from OpenZFS 2.3
with `direct=standard`, and `fallocate` means nothing on a copy-on-write
filesystem: both false are expected there and cost nothing but the page
cache. Anything the node is unhappy about is in `warnings`.

## 4. Record into it

A set is the cameras of one producer. Give it a retention that fits the
quota — this is a borrowed volume, and retention is what keeps Shale inside
it. A patch carries the row's version, so read it first:

```sh
shale set add @acme/cam-set
V=$(shale set get -o json @acme/cam-set | jq -r .dateUpdated)
shale set patch @acme/cam-set "{\"retention\":{\"expire_seconds\":604800},\"date_updated\":\"$V\"}"
```

Seven days here. Laminae are given their dates when they are allocated, so
this applies to what is recorded from now on; `shale lamina reschedule`
moves what is already stored ([§20.3](06-retention-gc.md#203-rescheduling)).

Then a producer. The quickest first one has no camera in it at all
([§38.1](15-producer.md#381-inputs)) — run it anywhere that reaches the
cluster:

```yaml
# producer.yaml
cp: https://10.1.2.74:30400
ca_hash: sha256:<the fingerprint the init job printed>
```

```sh
shale --config ./producer.yaml serve producer --demo 3
shale producer pending
shale producer adopt @acme/<alias> '{"set":{"slug":{"alias":"cam-set","tenant":{"alias":"acme"}}}}'
```

Three patterns ffmpeg draws for itself start recording into the dataset at
2 Mbps each. The console — `https://10.1.2.74:30402/`, the tenant half as
`@acme/admin`, the operator half as `@cluster/ops` — shows them on Cameras,
the laminae landing on Segments, and the streams on Live. Real cameras
replace the `--demo 3` with `producer.sources` and change nothing else.

Leave it for an afternoon. `shale sink get` is the number that matters:
`free` falling, `pressure` still `PRESSURE_NORMAL`.

## 5. When the disks arrive

Nothing moves ([§28.1](09-operations.md#281-no-draining-no-migration)). The
new sink is added, the old one is retired, and only then is it taken away.
Each step is one you can stop after.

**Mount the new disk on the machine**, at `/srv/shale/hdd01` say, and list
it *beside* the borrowed one — a second volume in `storage.yaml`, a second
path in `storage-config.yaml`:

```sh
kubectl apply -k deploy/k8s
kubectl -n shale rollout restart daemonset/shale-storage   # one node at a time
shale sink ls                                              # two sinks now
```

A node reads its sinks when it starts, so the rollout is what registers the
new one. Both take writes from here: placement spreads sets across devices,
so a camera's recording is now on two disks.

**Stop writing to the borrowed one.** It keeps serving reads:

```sh
shale sink retire @sink-<borrowed>
shale sink get @sink-<borrowed>     # accept_writes false, attachment RETIRED
```

Watch `free` on the new sink fall and the borrowed one's stand still. Every
camera is recording to the new disk now; nothing was interrupted.

**Throw the old recordings away**, which is this deployment's case — they
were a test:

```sh
shale device declare-dead @dev-<borrowed> '{"reason":"the volume goes back"}'
```

Its laminae become LOST, and readers see that stretch as a gap with a reason
rather than as recordings that never arrive
([§19](05-read-path.md#19-reader-semantics)). The Segments page shows it,
and so does the timeline:

```sh
shale lamina timeline '{"set":{"slug":{"alias":"cam-set","tenant":{"alias":"acme"}}},
  "from":"2026-10-01T00:00:00Z","to":"2026-10-08T00:00:00Z","size":50}'
```

Skip this step and the rows stay: a reader asking for that stretch is told
the laminae are unavailable, which is the right answer for a disk that is
away being repaired and the wrong one for a disk that is not coming back.

**Take it out and let it go.** Remove the volume and the path from the two
files, roll again, and unmount:

```sh
kubectl apply -k deploy/k8s && kubectl -n shale rollout restart daemonset/shale-storage
zfs destroy tank/shale
```

The retired row stays, and nothing asks for it again. Had it been taken away
*without* being retired, it would be "pending adoption" instead
([§28.3](09-operations.md#283-node-failure-and-device-re-homing)) — the
right answer for a disk that was pulled, the wrong one for a disk that was
given back.

## Things that bite

| | |
|---|---|
| `DirectoryOrCreate` on a real disk | a missing mount becomes an empty sink on the OS disk |
| a sink path that is not there | the node refuses to start, deliberately: a node quietly up without one of its disks looks healthy while its laminae read as gaps |
| a new sink in the ConfigMap, no rollout | sinks are read at startup; nothing registers until the pod restarts |
| a filesystem without user xattrs | the sink is refused at startup (NFS carries them only from NFSv4.2) |
| two datasets of one pool | two sinks on one device: no spread, and the node says so |
| a sink unlisted before it was retired | pending adoption rather than retired |
| `set patch` without `date_updated` | `version not given`: read the row first |
| a quota with no headroom | GC reclaims only what has expired; below 3% free the sink stops taking writes |
