# Ubuntu with systemd

One template unit, `shale@.service`, runs any role (§34.6):

```sh
install -m 0755 shale /usr/bin/shale
install -m 0644 shale@.service /etc/systemd/system/
useradd --system --home /var/lib/shale --shell /usr/sbin/nologin shale
install -d -o shale -g shale -m 0700 /var/lib/shale
install -d -m 0755 /etc/shale
# /etc/shale/shale.yaml: see below
systemctl daemon-reload
systemctl enable --now shale@storage       # or all, control, cluster, producer, reader, relay
journalctl -fu shale@storage
```

A three-machine cluster with one Control Plane process:

```yaml
# the first machine: both APIs and a node in one process (§34.7)
state: /var/lib/shale
db:
  driver: sqlite3
  dsn: "file:/var/lib/shale/control/shale.db?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"
  migrate: true
watch:
  broker: memory
server:
  addr: ":7400"
cluster:
  addr: ":7401"           # the other nodes dial this
control:
  auto_adopt: false       # adopt each node once: shale node adopt <alias>
  names: ["cp.example.com"]
storage:
  sinks:
    - path: /mnt/hdd01    # a whole HDD: capacity and device detected
```

```yaml
# every other machine: a Storage Node
state: /var/lib/shale
cp: https://10.1.2.73:7401
storage:
  sinks:
    - path: /mnt/hdd01
    - path: /mnt/hdd02
    # a directory with no disk of its own (a lab): declare its identity
    # - path: /srv/shale/sink
    #   capacity: 50GiB
    #   device: lab-davy-1
```

```yaml
# a relay, one per site (§39): producers dial :7430, viewers :7431 (WHEP)
state: /var/lib/shale
cp: https://cp.example.com:7401
relay:
  ingest_addr: ":7430"
  whep_addr: ":7431"
  # ice: ["stun:stun.example.com:3478"]   # for viewers behind NAT
```

```yaml
# the producer beside the cameras (§38.3)
state: /var/lib/shale
cp: https://10.1.2.73:7400
producer:
  sources:
    - alias: door
      input: v4l2:/dev/video0
      format: mjpeg
      size: 1920x1080
      fps: 30
      encoder: auto
      max_bitrate: auto
```

Run `shale init` on the first machine before its unit starts, then adopt
each host after its first join: `shale node adopt <alias>`, `shale producer
adopt <alias> --set <set>`.
