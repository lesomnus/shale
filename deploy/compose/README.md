# Single machine with Docker Compose

`shale serve all` (§34.7) beside PostgreSQL. The state directory is a named
volume, the sink a named volume by default or a bind mount of a whole HDD
with `SHALE_SINK=/mnt/hdd01`, and TLS is on: the container is not
development mode, so `init` creates the CA and the process serves the
tenant API on 7400 and the data plane on 7410 with certificates from it.
The configuration is inline in the compose file (`configs:`).

```sh
cd deploy/compose
docker compose up -d
docker compose logs init          # the first operator and admin
docker compose logs shale         # the node joining and serving
```

Build the image yourself with `docker build -t shale:dev .` at the
repository root and `SHALE_IMAGE=shale:dev docker compose up -d`.

Producers on the network dial `https://<host>:7400`; set `SHALE_ADVERTISE`
to `<host>:7410` so the node hands out the host's address rather than the
container's. The CA is `state/control/ca.crt` in the volume:

```sh
docker compose cp shale:/var/lib/shale/control/ca.crt ./ca.crt
```

The CLI from the same image, signed in as the tenant admin with the
password `init` printed (§33.1):

```sh
docker compose exec shale shale --config /etc/shale/shale.yaml \
    --addr https://127.0.0.1:7400 --cluster-addr https://127.0.0.1:7401 \
    login --password @acme/admin
docker compose exec shale shale --config /etc/shale/shale.yaml \
    --addr https://127.0.0.1:7400 set ls
```

The console (§40) is at `https://<host>:7402/`, with the same sign-in for
the tenant half and the operator's (`@cluster/ops`, also in the init log)
for the cluster half. Set `SHALE_CONSOLE_HOST=<host>` before the first
`up`: the CP certificate names it, and the cluster listener names the
page's origin as one it answers ([§40.4](../../docs/17-console.md#404-serving-it)).
