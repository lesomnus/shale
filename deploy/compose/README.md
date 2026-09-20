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

The CLI from the same image, as the tenant admin:

```sh
docker compose exec shale shale --config /etc/shale/shale.yaml \
    --addr https://127.0.0.1:7400 --as @acme/admin set ls
```

`--as` is the plain header, which the container refuses; a session sign-in
replaces it when #28 lands. Until then, administer through `--dev` on a
machine you trust or through the cluster API's operator.
