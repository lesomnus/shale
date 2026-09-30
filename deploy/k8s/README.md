# Shale on Kubernetes

The deployment of [§34.5](../../docs/11-deployment.md#345-kubernetes):
PostgreSQL, the tenant API and the cluster API as Deployments, and the
Storage Nodes as a DaemonSet on the host network of the nodes labeled for
storage. Applied with kustomize:

```sh
kubectl apply -k deploy/k8s
```

## Before applying

1. **The image.** `deploy/k8s/kustomization.yaml` names
   `ghcr.io/lesomnus/shale:dev`. Build it from the repository root and
   push it to a registry the nodes pull from, or load it into each node's
   containerd:

   ```sh
   docker build -t ghcr.io/lesomnus/shale:dev .
   docker save ghcr.io/lesomnus/shale:dev | ssh node 'sudo k3s ctr -n k8s.io images import -'
   ```

2. **Names.** `control-config.yaml` lists every name the APIs are dialed
   by: the CP certificate names them, and a producer outside the cluster
   verifies the one it dials. With no Ingress or LoadBalancer, the tenant
   API is a NodePort (30400, sign-in HTTP and the console on 30402, the
   operator's sign-in on 30403), so list the node IPs — and, under
   `cluster.http.origins`, every `https://<node IP>:30402` the console is
   opened by (§40.4).

3. **Sinks.** `storage-config.yaml` and `storage.yaml` describe one sink
   per node at `/srv/shale/k8s/sinks/0`, a directory on the OS disk with a
   declared capacity, which is a lab. A real node mounts each HDD at a path
   and lists it with no capacity; the device is detected (§22.2). See
   [Which disk, on which machine](#which-disk-on-which-machine) below.

4. **The database password** in `postgres.yaml`, twice.

5. **Label the storage nodes:**

   ```sh
   kubectl label node <node> shale.io/storage=true
   ```

## What happens

- The `shale-init` Job runs `shale init` against PostgreSQL once it
  answers, and puts the KEK, the CA, and the CP certificate into the
  `shale-control` Secret through its service account. The passwords of the
  first cluster operator and the first tenant admin are in its log,
  printed once:

  ```sh
  kubectl -n shale logs job/shale-init
  kubectl -n shale delete job shale-init      # afterwards
  ```

- roster, where people and tenants are (§33.1), runs inside the init job
  and every API pod, on the `roster` database the PostgreSQL manifest makes
  beside `shale`; nothing else reaches it. New people come from `shale
  holder add` and get a password from `shale holder issue-password`. A
  deployment that runs roster of its own sets `auth.roster.addr` and its
  tenant keys in `control-config.yaml` instead.

- Both API Deployments mount that Secret as their state directory and
  come up once it exists. Several CP processes share the database: one of
  them, elected through an advisory lock, runs the jobs and the directives
  (§34.9); the LISTEN/NOTIFY broker carries watch events between them.

- Each Storage Node joins through the cluster API's Service, is adopted at
  once (`auto_adopt: true` in `control-config.yaml`; set it to `false` and
  `shale node adopt` each one to keep the operator in the loop), and
  reports its sinks. Its state lives in `/var/lib/shale-k8s` on the host.

## Which disk, on which machine

Kubernetes has no idea of a disk, so "this disk on that machine" is said in
three places that have to agree:

| | where | what it says |
|---|---|---|
| 1 | `kubectl label node <node> shale.io/storage=true` | which machines run a Storage Node — the DaemonSet's `nodeSelector` |
| 2 | `storage.yaml`, the `hostPath` volume | where the disk is mounted **on that machine** |
| 3 | `storage-config.yaml`, `storage.sinks[].path` | where that volume is mounted **in the pod**, which is what Shale calls the sink |

Mount the disk on the machine first, in `/etc/fstab` or as the ZFS dataset's
`mountpoint`; Shale never mounts anything. Then:

```yaml
# storage.yaml
volumeMounts:
  - { name: sink-0, mountPath: /srv/shale/sinks/0 }
volumes:
  - name: sink-0
    hostPath:
      path: /mnt/hdd01      # the mount point on the machine
      type: Directory       # not DirectoryOrCreate: see below
```

```yaml
# storage-config.yaml
storage:
  sinks:
    - path: /srv/shale/sinks/0     # a whole disk: capacity and device detected
```

Three things are worth knowing before the first apply:

- **`type: Directory`, not `DirectoryOrCreate`, for a real disk.** With
  `DirectoryOrCreate` a missing mount is created as an empty directory on the
  OS disk, and the node registers it as a new, empty sink and starts filling
  the system's own filesystem. With `Directory` the pod refuses to start,
  which is the answer you want. The manifests here ship
  `DirectoryOrCreate` because their sink is a lab directory.
- **One DaemonSet describes one layout.** The DaemonSet and the ConfigMap are
  shared by every node that matches the label, so every one of them must have
  the disk at the same path. Machines whose disks differ get their own label,
  DaemonSet and ConfigMap (`shale.io/storage: ssd-box`, and so on).
- **A shared filesystem needs `capacity`** — a directory on a volume Shale
  does not own, an NFS or iSCSI mount, a ZFS dataset without a quota
  ([§22.2](../../docs/07-storage-node.md#222-sinks-and-devices)). Whatever the
  filesystem, it must support **user xattrs**: the node refuses a sink
  without them, because a lamina's record lives there. `setfattr -n user.x -v
  1 <dir>` on the machine answers that in one line. NFS carries them only
  from NFSv4.2, and an export that does not is a sink that never registers;
  a zvol over iSCSI formatted ext4 or XFS behaves like a local disk.

### A disk that is not on a Kubernetes node

Storage that lives on a machine of its own — a NAS, a file server with a ZFS
pool — has three shapes, in the order they are worth trying:

1. **Make that machine a node of the cluster** and label it: the disk is then
   a hostPath like any other, the filesystem is local, and the device is the
   pool or the disk itself.
2. **Run `shale serve storage` on it outside Kubernetes**, from the deb
   package (§34.6), with the control plane in the cluster. A Storage Node
   speaks to the cluster API and the cluster API's gRPC port is a ClusterIP
   here, so it needs a NodePort or an internal LoadBalancer of its own, and
   the address the node dials has to be a name the CP certificate carries
   (`control-config.yaml`). The CP dials back to the node's control API
   (7421), so that has to be reachable from the CP pods.
3. **Attach the volume to a Kubernetes node** and treat it as a disk there —
   a zvol over iSCSI formatted ext4 or XFS behaves like one. An NFS export is
   the weakest of the three: user xattrs need NFSv4.2, `O_DIRECT` and
   `fallocate` mean nothing over it, and every lamina's bytes cross the
   network twice.

### Changing the disk under a node

[§28.4](../../docs/09-operations.md#284-changing-the-disk-under-a-node) in
Kubernetes terms. Nothing moves: the new sink is added, the old one is
retired and then taken away.

```sh
# 1. mount the new disk on each storage node, then add the volume and the
#    path to storage.yaml and storage-config.yaml, and roll.
kubectl apply -k deploy/k8s
kubectl -n shale rollout restart daemonset/shale-storage   # one node at a time
shale sink ls                                             # both sinks, both attached

# 2. stop writing to the old one; what is on it stays readable.
shale sink retire @sink-<old>

# 3. when the recordings on it are not being kept: their rows go too, and
#    readers see gaps rather than laminae that never arrive.
shale device declare-dead @dev-<old> '{"reason":"the volume goes back"}'

# 4. take it out of both files and roll again; then unmount it.
kubectl apply -k deploy/k8s && kubectl -n shale rollout restart daemonset/shale-storage
```

The old sink's row stays, retired, and nothing asks for it again. A sink
that is taken away **without** being retired first is "pending adoption"
instead ([§28.3](../../docs/09-operations.md#283-node-failure-and-device-re-homing)),
which is the right answer for a disk that was pulled and the wrong one for a
disk that was given back.

## Behind an Ingress

A producer outside the cluster can reach the tenant API through an
ingress controller that passes the port through and says who is calling
with the PROXY protocol ([§34.10](../../docs/11-deployment.md#3410-node-addresses)).
With ingress-nginx:

```sh
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.13.3/deploy/static/provider/baremetal/deploy.yaml
kubectl -n ingress-nginx create configmap tcp-services --from-literal=7400=shale/shale-control:7400::PROXY
kubectl -n ingress-nginx patch deploy ingress-nginx-controller --type=json \
  -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--tcp-services-configmap=$(POD_NAMESPACE)/tcp-services"}]'
kubectl -n ingress-nginx patch svc ingress-nginx-controller --type=json \
  -p '[{"op":"add","path":"/spec/ports/-","value":{"name":"shale-control","port":7400,"targetPort":7400,"nodePort":30740}}]'
```

Run the controller on the host network of the nodes set aside for it
(`hostNetwork: true`, a `nodeSelector`), and activate an `AddressPolicy`
whose `trusted_proxies` are those hosts' addresses, node IP and pod-CIDR
host address alike, and nothing wider: a trusted pod CIDR makes the CP
expect a header from every pod and every kubelet probe. Producers then
dial the NodePort (30740 above) on any node the CP certificate names.

## Using it

Sign in from a machine that reaches a node IP, with the CA the init job
made:

```sh
kubectl -n shale get secret shale-control -o jsonpath='{.data.ca\.crt}' | base64 -d > ca.crt
shale --addr https://10.1.2.74:30400 --cluster-addr https://10.1.2.74:30400 \
      --config /dev/null login --password @acme/admin
```

The cluster API's gRPC is not exposed: operate it from inside the cluster,
or port-forward `svc/shale-cluster` 7401 and dial `https://127.0.0.1:7401`
(its certificate does not name `127.0.0.1`; add a name to
`control-config.yaml` or use `client.ca_file` with a name that resolves).
Its sign-in listener is (`shale-cluster-web`, NodePort 30403), for the
console.

The console (§40) is at `https://10.1.2.74:30402/` — any node, with the CA
above imported or its warning clicked through: the tenant half signs in as
`@acme/admin`, the cluster half (hosts to adopt, devices) as `@cluster/ops`,
both with the passwords the init job printed.

A producer outside the cluster:

```yaml
cp: https://10.1.2.74:30400
ca_hash: <the CA fingerprint the init job printed>
producer:
  sources:
    - alias: door
      input: v4l2:/dev/video0
```

It appears in `shale producer ls --pending`, and `shale producer adopt`
binds it to a set. Its uploads go straight to the Storage Nodes on the
host network (port 7420), never through the cluster's Services.

## Known wrinkles

- A Storage Node pod that starts before the `shale-cluster` Service exists
  (the first apply) may resolve a stale address and keep retrying against
  it. Restart the pod: `kubectl -n shale delete pod <pod>`.
- A sink that is a directory on the OS disk reports the disk's free space;
  declare its `capacity` (as `storage-config.yaml` does), or a nearly full
  disk puts it at CRITICAL pressure and placement skips it.
- The tenant API's readiness probe is a TCP connect; the node's data plane
  drops the resulting handshake noise from its log.

## Applying again with a new image

`shale-init` is a Job, and a Job's template is immutable: delete it before
applying a kustomization that names a new image tag (`kubectl -n shale
delete job shale-init`). It does nothing on the next run while the Secret
exists.

## Rolling upgrades

`storage.yaml` rolls one node at a time (`maxUnavailable: 1`). While a
node restarts its laminae are UNAVAILABLE and placement skips it through
missed heartbeats; nothing moves. The API Deployments roll normally.

The relay (`relay.yaml`) runs on the host network of one amd64 node;
`shale live @acme/<set>` from a machine that reaches it is the quickest
check that a set plays.

## Not here yet

- The reader agent: no manifests until it exists.
- A Helm chart: none, on purpose. The kustomization is the deliverable;
  a deployment overlays it (`kustomize edit`, a patch for its names,
  storage class and node selector) rather than templating it.
- An Ingress with TLS passthrough: this cluster has no ingress controller;
  the NodePort stands in for it.
