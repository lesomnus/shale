# Camera LAN on a Raspberry Pi

Use a dedicated Ethernet port for an isolated PoE switch and cameras. The Pi's
existing Wi-Fi connection carries Shale control, uploads and live traffic. The Pi
terminates RTSP itself: this topology needs no bridge, NAT or IP forwarding.
Camera IP addresses do not identify logical Sources; central device/binding
identity survives DHCP address changes.

This recipe targets 64-bit Raspberry Pi OS using NetworkManager and systemd.
It renders files for review and never changes an interface, starts a service or
installs a package. A DHCP server on a corporate cable is not this topology:
disconnect that cable and attach only the camera switch to the selected port.

## Render and validate

Install `python3`, `iproute2`, `dnsmasq-base` and `network-manager` using the OS
package manager. `dnsmasq-base` supplies the daemon without enabling the generic
DHCP/DNS service. Existing independent DHCP configurations must be checked before
enabling another instance; this recipe does not modify them.

Choose a private camera subnet that overlaps neither the Wi-Fi LAN nor any VPN
route. The example below is an example, not a subnet allocation for your site.
The pool excludes the Pi's own address, network and broadcast addresses.

```sh
python3 deploy/edge/camera_lan.py \
  --interface eth0 --uplink wlan0 --address 192.168.250.1/24 \
  --first 192.168.250.20 --last 192.168.250.99 --output ./camera-lan
```

The default preflight reads `ip -j` state and `/sys/class/net`. It requires a
separate wired port, an existing wireless default route, no other IPv4 address
on the camera port, and no overlapping address/route on another interface. It
refuses a bridge/bond member, wildcard interface names and a camera-port default
route. It makes no network changes. `--offline` can render on another machine;
rerun the same command with `--validate-only` on the Pi before installation.
An existing output directory is refused rather than overwritten.

Review the four files:

- The NetworkManager keyfile gives only Ethernet a manual camera address, disables
  IPv6 on that connection and sets `never-default=true`. It uses neither the
  `shared` method nor a gateway/DNS override. Wi-Fi is untouched.
- `camera-dhcp.conf` binds DHCP to the exact camera interface, explicitly excludes
  the uplink and loopback, disables DNS and suppresses default router/DNS options.
  It advertises no NTP server. Cameras receive an address for local communication
  and are not promised Internet, DNS or time service from the Pi.
- `shale-camera-dhcp.service` runs a separate maintained dnsmasq process with its
  own persistent lease directory. It does not depend on a Shale process lifetime.
  DHCP holds only the network capabilities it needs; Worker/Producer do not need
  those capabilities to perform ordinary UDP discovery or connect to RTSP.
- `camera-network.json` records the exact CIDR/interface for central network
  enrollment and discovery configuration. It is descriptive input, not an
  authentication or automatic device-adoption grant.

## Opt-in installation

Keep a working Wi-Fi management session. Review active Ethernet profiles with
`nmcli connection show`; disable autoconnect for any conflicting old Ethernet
profile explicitly. Do not disable or replace the Wi-Fi profile. The installer
does not guess which unrelated profiles can be removed.

```sh
sudo install -m 0600 camera-lan/shale-camera-lan.nmconnection \
  /etc/NetworkManager/system-connections/shale-camera-lan.nmconnection
sudo install -d -m 0755 /etc/shale
sudo install -m 0644 camera-lan/camera-dhcp.conf /etc/shale/camera-dhcp.conf
sudo install -m 0644 camera-lan/shale-camera-dhcp.service /etc/systemd/system/
sudo dnsmasq --test --conf-file=/etc/shale/camera-dhcp.conf
sudo systemd-analyze verify /etc/systemd/system/shale-camera-dhcp.service
sudo nmcli connection reload
sudo nmcli connection up shale-camera-lan
sudo systemctl daemon-reload
sudo systemctl enable --now shale-camera-dhcp.service
```

The unit's dynamic systemd user owns `/var/lib/shale-camera-dhcp`. Leases survive
daemon and OS restarts. DHCP uses a separate unit, so stopping or restarting the
Shale edge service leaves DHCP running. The interface and address may disappear
and reappear: dnsmasq's Linux `bind-dynamic` mode follows the selected interface.
Neither this unit nor the NetworkManager profile enables forwarding or installs
NAT/firewall rules. Existing host firewall policy must allow camera-interface
UDP 67/68 and required discovery/RTSP, without opening DHCP on Wi-Fi.

Inspect `ip -4 route`, `ip -4 address show dev eth0`,
`journalctl -u shale-camera-dhcp` and
`sudo cat /var/lib/shale-camera-dhcp/leases`. The Wi-Fi default route must remain
in place. A lease or advertised MAC/model is only a discovery hint: authenticated
observation, an approved network assignment and central policy still gate camera
initialization and credential use. Do not attach a root DHCP hook that passes
untrusted lease strings to shell commands.

## Rollback

```sh
sudo systemctl disable --now shale-camera-dhcp.service
sudo nmcli connection down shale-camera-lan
sudo nmcli connection delete shale-camera-lan
sudo rm /etc/shale/camera-dhcp.conf /etc/systemd/system/shale-camera-dhcp.service
sudo systemctl daemon-reload
```

Restore only the old Ethernet profile you explicitly changed. The recipe does
not touch Wi-Fi or another dnsmasq service. Retain the lease directory for a
reinstall, or remove that directory explicitly once its leases are no longer
needed. Do not erase Shale identity, central credentials, Source bindings or
recordings as part of network rollback.

## Capacity and outages

Dimension Wi-Fi for simultaneous storage and live forwarding. With default
always-live behavior these can send roughly two copies of each encoded stream;
headers, audio, retries and transcoding can change the exact total. On-demand
live delivery reduces traffic when nobody watches. For example, three cameras
at 4 Mbps each need about 12 Mbps for recording and another 12 Mbps for continuous
live forwarding before overhead. Use measured throughput in the actual location.

The video buffer is finite volatile RAM. A larger buffer absorbs brief outages;
it cannot fix an average upload rate below the camera input rate and does not
survive Pi power loss. DHCP continuing during an uplink outage does not imply
new central policy/credential operations can proceed or that a cold-started
Producer can record without CP. Offline capture requires an explicitly supported
applied-state/credential cache and its expiry/revocation rules; this recipe adds
neither such a cache nor a durable video spool.

## References and checks

The settings follow the [dnsmasq manual](https://dnsmasq.org/docs/dnsmasq-man.html),
[NetworkManager IPv4 reference](https://networkmanager.dev/docs/api/latest/settings-ipv4.html)
and [Raspberry Pi networking documentation](https://www.raspberrypi.com/documentation/computers/configuration.html).
Run `python3 -m unittest discover -s deploy/edge -p 'test_*.py'` for range,
interface, route-collision and generated-scope checks. Physical Pi/camera
qualification is an explicit separate deployment step; tests do not reset a
production camera.
