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
   API is a NodePort (30400, sign-in HTTP on 30402), so list the node IPs.

3. **Sinks.** `storage-config.yaml` and `storage.yaml` describe one sink
   per node at `/srv/shale/k8s/sinks/0`, a directory on the OS disk with a
   declared capacity, which is a lab. A real node mounts each HDD at a path
   and lists it with no capacity; the device is detected (§22.2).

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

- Both API Deployments mount that Secret as their state directory and
  come up once it exists. Several CP processes share the database: one of
  them, elected through an advisory lock, runs the jobs and the directives
  (§34.9); the LISTEN/NOTIFY broker carries watch events between them.

- Each Storage Node joins through the cluster API's Service, is adopted at
  once (`auto_adopt: true` in `control-config.yaml`; set it to `false` and
  `shale node adopt` each one to keep the operator in the loop), and
  reports its sinks. Its state lives in `/var/lib/shale-k8s` on the host.

## Using it

Sign in from a machine that reaches a node IP, with the CA the init job
made:

```sh
kubectl -n shale get secret shale-control -o jsonpath='{.data.ca\.crt}' | base64 -d > ca.crt
shale --addr https://10.1.2.74:30400 --cluster-addr https://10.1.2.74:30400 \
      --config /dev/null login --password @acme/admin
```

The cluster API is not exposed: operate it from inside the cluster, or
port-forward `svc/shale-cluster` 7401 and dial `https://127.0.0.1:7401`
(its certificate does not name `127.0.0.1`; add a name to
`control-config.yaml` or use `client.ca_file` with a name that resolves).

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

## Rolling upgrades

`storage.yaml` rolls one node at a time (`maxUnavailable: 1`). While a
node restarts its objects are UNAVAILABLE and placement skips it through
missed heartbeats; nothing moves. The API Deployments roll normally.

## Not here yet

- The relay (E6) and the reader agent: no manifests until they exist.
- A Helm chart: the kustomization is the deliverable for now.
- An Ingress with TLS passthrough: this cluster has no ingress controller;
  the NodePort stands in for it.
