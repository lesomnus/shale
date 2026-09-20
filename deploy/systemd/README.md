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

## The package

`deploy/deb/build.sh [version] [amd64|arm64]` builds a Debian package with
the binary, the template unit, and `/etc/shale/shale.yaml.example`; its
postinst makes the `shale` user and `/var/lib/shale`, and starts nothing.
CI attaches one per architecture to every run.

```sh
sudo dpkg -i shale_0.1.0_amd64.deb
sudo cp /etc/shale/shale.yaml.example /etc/shale/shale.yaml   # then edit
sudo systemctl enable --now shale@storage
```

The service user needs the `video` group for cameras and `audio` for
microphones (the package adds both); `arecord -l` names the card.

## A Raspberry Pi as a producer

The arm64 package runs on Raspberry Pi OS (64-bit). `ffmpeg` from the
distribution carries the `h264_v4l2m2m` encoder the Pi's hardware offers,
which `shale producer scan` picks when it opens; a USB camera at 1080p30
records at about 4 Mbps with the SoC around 50% busy. Give the producer
`cp:` (the tenant API), `tenant:`, and one source per camera, adopt it
with `shale producer adopt`, and it records from then on, surviving
ffmpeg restarts and losing nothing while the control plane is down that
its RAM buffer can hold (§38).

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
#     audio:
#       device: alsa:plughw:2,0  # the camera's microphone; plughw converts what hw: cannot
#       codec: opus              # Opus plays live as it is; aac, the default, plays through the live helper (§38.7)

```

Run `shale init` on the first machine before its unit starts, then adopt
each host after its first join: `shale node adopt <alias>`, `shale producer
adopt <alias> --set <set>`.
