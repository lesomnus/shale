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
renderer=/usr/share/shale/edge/camera_lan.py
# From a source checkout instead: renderer=deploy/edge/camera_lan.py
python3 "$renderer" \
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

The Debian package installs this guide at
`/usr/share/doc/shale/deploy/edge/README.md` and the renderer at the path above.
It installs neither a network profile nor an enabled DHCP service. A checkout
is not needed to render the files on the Pi.

Review the seven files:

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
- `shale-camera-dhcp.sysusers.conf` declares a static, non-login DHCP account and
  the dedicated `shale-camera-discovery` group. DHCP does not join the `shale`
  group or gain access to machine identity/credential directories.
- `shale-edge-dhcp.conf` and `shale-camera-worker-dhcp.conf` are supplementary-group
  drop-in templates for the selected combined or independent Worker instance.
  Install only the template for the layout you run. They change no service user,
  capability or other instance's groups.
- `camera-network.json` records the exact CIDR/interface for central network
  enrollment and discovery configuration. It is descriptive input, not an
  authentication or automatic device-adoption grant.

## Opt-in installation

Keep a working Wi-Fi management session. Review active Ethernet profiles with
`nmcli connection show`; disable autoconnect for any conflicting old Ethernet
profile explicitly. Do not disable or replace the Wi-Fi profile. This
first-install recipe refuses existing target files or symlinks. For an upgrade,
back up the existing profile/configuration/unit and plan their explicit restore
before changing them; do not use the fresh-install rollback to delete a prior
installation. The recipe does not guess which unrelated profiles can be removed.

```sh
sudo sh -eu <<'INSTALL'
instance=edge # Use camera-worker for the independent Worker layout instead.
case "$instance" in
  edge) dropin=camera-lan/shale-edge-dhcp.conf ;;
  camera-worker) dropin=camera-lan/shale-camera-worker-dhcp.conf ;;
  *) exit 1 ;;
esac
for target in \
  /etc/NetworkManager/system-connections/shale-camera-lan.nmconnection \
  /etc/shale/camera-dhcp.conf \
  /etc/systemd/system/shale-camera-dhcp.service \
  /etc/sysusers.d/shale-camera-dhcp.conf \
  "/etc/systemd/system/shale@$instance.service.d/20-camera-dhcp.conf" \
  /var/lib/shale-camera-dhcp
do
  if [ -e "$target" ] || [ -L "$target" ]; then
    echo "Refusing to replace existing $target; preserve it before an upgrade." >&2
    exit 1
  fi
done
if getent passwd shale-camera-dhcp >/dev/null || getent group shale-camera-dhcp >/dev/null || getent group shale-camera-discovery >/dev/null; then
  echo 'Review existing service-account/group names before installation.' >&2
  exit 1
fi
install -m 0600 camera-lan/shale-camera-lan.nmconnection \
  /etc/NetworkManager/system-connections/shale-camera-lan.nmconnection
[ -d /etc/shale ] || install -d -m 0755 /etc/shale
install -m 0644 camera-lan/camera-dhcp.conf /etc/shale/camera-dhcp.conf
install -m 0644 camera-lan/shale-camera-dhcp.service /etc/systemd/system/
install -d -m 0755 /etc/sysusers.d "/etc/systemd/system/shale@$instance.service.d"
install -m 0644 camera-lan/shale-camera-dhcp.sysusers.conf /etc/sysusers.d/shale-camera-dhcp.conf
systemd-sysusers /etc/sysusers.d/shale-camera-dhcp.conf
install -m 0644 "$dropin" "/etc/systemd/system/shale@$instance.service.d/20-camera-dhcp.conf"
dnsmasq --test --conf-file=/etc/shale/camera-dhcp.conf
systemd-analyze verify /etc/systemd/system/shale-camera-dhcp.service
nmcli connection reload
nmcli connection up shale-camera-lan
systemctl daemon-reload
systemctl enable --now shale-camera-dhcp.service
INSTALL
```

The static `shale-camera-dhcp` user owns `/var/lib/shale-camera-dhcp`, with group
`shale-camera-discovery`. Its directory is 0750 and the lease file is 0640;
`UMask=0027` also covers newly created files. The selected Worker/edge instance
keeps `User=shale` and gains only supplementary read/traverse access. It cannot
write the file or directory. An already running role needs an explicit restart
after installing the drop-in to obtain the group; installing it alone does not
change a process's groups. Do not add DHCP to `shale`, run Worker as root, or make
`/var/lib/private` traversable. Leases survive
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

### Migrate the earlier DynamicUser DHCP unit

The earlier recipe stored state behind a systemd-owned symlink to
`/var/lib/private/shale-camera-dhcp`, whose private ancestor is 0700. A
non-root Worker could not read those leases. Changing only the file mode or
adding a supplementary group does not fix that ancestor. Migrate only this DHCP
unit and state after reviewing its rendered replacement. Keep the Wi-Fi session
and existing camera address/profile intact; no NetworkManager or networkd
restart is part of this procedure.

The commands below deliberately require the known legacy symlink and an existing
regular lease file. They stop DHCP briefly so the snapshot cannot change during
copying. The original private directory is retained unchanged, and its symlink,
lease snapshot and old unit/config are retained in a new root-only backup. If a
guard or later step fails, reconcile that partial state using the backup rather
than blindly rerunning or deleting a directory. Inspect existing static account
names separately; the earlier DynamicUser identity must be stopped before
`systemd-sysusers` creates the persistent account.

```sh
sudo sh -eu <<'MIGRATE'
instance=edge # Or camera-worker, matching the one active Worker layout.
case "$instance" in
  edge) dropin=camera-lan/shale-edge-dhcp.conf ;;
  camera-worker) dropin=camera-lan/shale-camera-worker-dhcp.conf ;;
  *) exit 1 ;;
esac
state=/var/lib/shale-camera-dhcp
test -L "$state"
test "$(readlink -f "$state")" = /var/lib/private/shale-camera-dhcp
test -f "$state/leases"
test ! -L "$state/leases"
test ! -e /etc/sysusers.d/shale-camera-dhcp.conf
test ! -L /etc/sysusers.d/shale-camera-dhcp.conf
test ! -e "/etc/systemd/system/shale@$instance.service.d/20-camera-dhcp.conf"
test ! -L "/etc/systemd/system/shale@$instance.service.d/20-camera-dhcp.conf"
systemctl stop shale-camera-dhcp.service
if getent passwd shale-camera-dhcp >/dev/null || getent group shale-camera-dhcp >/dev/null || getent group shale-camera-discovery >/dev/null; then
  echo 'Review remaining static service-account/group names before migration.' >&2
  exit 1
fi
backup=$(mktemp -d /var/backups/shale-camera-dhcp.XXXXXXXX)
chmod 0700 "$backup"
install -m 0600 "$state/leases" "$backup/leases"
cp -a /etc/systemd/system/shale-camera-dhcp.service "$backup/service"
cp -a /etc/shale/camera-dhcp.conf "$backup/config"
mv -- "$state" "$backup/state-link" # Move the symlink only, not its private target.
install -d -m 0755 /etc/sysusers.d "/etc/systemd/system/shale@$instance.service.d"
install -m 0644 camera-lan/shale-camera-dhcp.sysusers.conf /etc/sysusers.d/shale-camera-dhcp.conf
systemd-sysusers /etc/sysusers.d/shale-camera-dhcp.conf
getent -s files passwd shale-camera-dhcp >/dev/null
install -d -m 0750 -o shale-camera-dhcp -g shale-camera-discovery "$state"
install -m 0640 -o shale-camera-dhcp -g shale-camera-discovery "$backup/leases" "$state/leases"
cmp -- "$backup/leases" "$state/leases"
install -m 0644 camera-lan/shale-camera-dhcp.service /etc/systemd/system/
install -m 0644 "$dropin" "/etc/systemd/system/shale@$instance.service.d/20-camera-dhcp.conf"
systemd-analyze verify /etc/systemd/system/shale-camera-dhcp.service
systemctl daemon-reload
systemctl start shale-camera-dhcp.service
systemctl restart "shale@$instance.service"
printf 'Preserved DHCP rollback state: %s\n' "$backup"
MIGRATE
```

Verify that the selected role can read the lease file through its new group and
cannot append to it; check its supplementary groups and the 0750/0640 modes after
an actual DHCP lease update. Confirm the DHCP account cannot read Shale's 0700
machine-state directories. Keep all CP keys, camera credentials, bindings and
recordings outside the migration. To roll back, stop the DHCP unit, preserve the
new lease file as well, restore the saved old unit/config and original state
symlink, then reload/start the old unit. Do not remove either state's lease data
or change the private ancestor's ownership/mode.

## Run the managed Shale roles

Follow the [managed edge guide](../../docs/managed-edge.md) for machine approval,
configuration and migration from standalone services. The Debian example is
`/etc/shale/shale-edge.yaml.example`. It connects the Worker and Producer to the
tenant API, normally port **7400**, and deliberately has no USB/manual Sources,
local presets or password files. Both roles receive their own approved central
assignments. Use `shale@edge` for the combined process or the separate supported
role commands; do not run both layouts against the same role state directories.

Enable read-only DHCP hints in that role's configuration:

```yaml
camera_worker:
  dhcp_leases:
    - interface: eth0
      file: /var/lib/shale-camera-dhcp/leases
```

The supplementary-group drop-in above makes this path readable to the packaged
non-root Worker. It remains an unauthenticated hint source, not device trust.

`camera-network.json` is only a description to review for the central network
assignment. Rendering it, obtaining a DHCP lease or starting `shale@edge` does
not create or authorize that assignment. The network recipe and Shale unit are
enabled separately, and the package starts neither automatically.

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
For a fresh installation, remove only the supplementary-group drop-in you
installed for the selected role, then reload/restart that role. Retain the static
account/group while it still owns retained state. An upgrade uses its saved unit
and symlink instead of the fresh-install removal commands.

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
new central policy/credential operations can proceed. Already-running authorized
captures may continue while their finite RAM queue and existing sessions allow;
a process restart or a new capture attempt still requires the current CP catalog
and a new BeginApply ticket. The optional streaming credential cache is off by
default and does not supply either authority, authorize offline cold startup or
replace current assignment checks. This recipe adds no durable video spool.

## References and checks

The settings follow the [dnsmasq manual](https://dnsmasq.org/docs/dnsmasq-man.html),
[NetworkManager IPv4 reference](https://networkmanager.dev/docs/api/latest/settings-ipv4.html)
and [Raspberry Pi networking documentation](https://www.raspberrypi.com/documentation/computers/configuration.html).
From a source checkout, run
`python3 -m unittest discover -s deploy/edge -p 'test_*.py'` for range,
interface, route-collision and generated-scope checks. Physical Pi/camera
qualification is an explicit separate deployment step; tests do not reset a
production camera.

The protocol fixture uses fresh Linux network namespaces inside an explicitly
privileged container with no external network. It runs dnsmasq as a non-root
user with the unit's three ambient capabilities and sends actual DHCP requests
from synthetic camera and uplink peers. It checks address acquisition, absent
router/DNS/NTP options, lease recovery across daemon restart, camera access
during a simulated uplink flap and refusal to serve the uplink. NetworkManager's
Wi-Fi route restoration is simulated; physical reboot/radio qualification is
not inferred from this test.

Before the DHCP protocol run, `lease_permissions_test.py` uses distinct Unix
UIDs/groups in that same disposable container. The Worker can read the lease
file but cannot append or create files in DHCP state; the DHCP account can
write its own state but cannot read a synthetic Producer machine key; an
unrelated UID cannot read leases. This checks actual kernel permissions without
creating host accounts or reading real machine keys. The ordinary Python unit
suite also checks the emitted sysusers/unit/drop-in contract.

```sh
# Run from a source checkout on a dedicated test runner.
docker build -t shale-camera-lan-test -f deploy/edge/test.Dockerfile deploy/edge
docker run --rm --network=none --privileged shale-camera-lan-test
```

No physical interface, existing namespace, camera or host-network container is
used. Namespace/process cleanup runs on failure as well. Run this only on a
dedicated test runner that permits the required namespace capabilities.

## Topology runner for assembled end-to-end tests

`topology.py` reuses the generated DHCP configuration and builds three fresh
network namespaces inside the same disposable, network-disabled test container:

```text
camera eth0 <-- dedicated camera LAN --> eth0 edge wlan0 <-- simulated Wi-Fi --> eth0 CP
DHCP lease                              192.168.250.1     10.23.0.2           10.23.0.1
```

The camera obtains its address from an actual DHCP exchange before a test starts.
The runner checks server DORA log entries, a persisted lease and absent
router/DNS/NTP options. It configures the WS-Discovery multicast route on the
camera LAN and disables edge IPv4 forwarding. There is no bridge, NAT, physical
interface or route from CP to the camera LAN. The Wi-Fi link is a veth simulation;
it does not model radio behavior.

The runner executes one coordinator command in the CP namespace and exports
`SHALE_TOPOLOGY_FILE`, the absolute path to a version-1 JSON manifest. The manifest
contains `roles.edge`, `roles.camera` and `roles.cp`, their addresses/interfaces,
`exec_prefix` argument arrays, the multicast destination, and DHCP evidence paths.
The coordinator runs as root solely to launch test processes in the prepared
namespaces. DHCP is a separate process with its own limited network capabilities.
Every application's `exec_prefix` uses UID/GID 65534, empty supplementary groups,
no capabilities (including an empty bounding set), and `no_new_privs`.

Append a command to a role's `exec_prefix` and execute the resulting argument array
directly. Do not join a Go runtime thread to a namespace, pass the prefix through
a shell, or give the application a privileged prefix. The `ip netns exec` helper
performs namespace entry before executing the unprivileged application. The root
coordinator keeps access to the named namespaces and may use `exec.Command` to
start, stop or restart each role as the scenario requires. For example:

```go
argv := append(append([]string{}, topology.Roles["edge"].ExecPrefix...),
    "/test/shale", "serve", "edge", "--config", configPath)
edge := exec.CommandContext(ctx, argv[0], argv[1:]...)
```

This is an execution-contract example; the future assembled test supplies actual
CLI arguments, central services, credentials, readiness checks and assertions.
Tests must create writable state/config directories for UID/GID 65534, including
traversable parents. Prefer a dedicated subdirectory below a test temporary
directory, and chown that subdirectory explicitly. Ordinary unprivileged sockets
can perform discovery and connect to camera RTSP; camera fixture listeners should
use ports above 1023. Host filesystem mounts are unnecessary: build or copy test
binaries and fixtures into a test image with `COPY`.

```sh
docker run --rm --network=none --privileged shale-camera-lan-test \
  python3 topology.py -- python3 topology_smoke.py
# A future image can inject a compiled coordinator instead:
# python3 topology.py -- /test/e2e.test -test.run '^TestAssembledEdge$'
```

`--timeout` bounds the coordinator's lifetime (default 120 seconds), and its exit
code becomes the runner's exit code. `--artifacts /tmp/run-name` creates a fresh
directory and retains `topology.json`, DHCP logs/lease and `result.json` inside
the container. `result.json` records the coordinator exit code and whether cleanup
completed; the manifest's namespace prefixes are valid only while the runner is
active. Without `--artifacts`, temporary evidence is deleted after cleanup. Use
`docker cp` from a stopped container when evidence must be retained outside it.

The supervisor terminates child process groups and remaining processes in its
namespaces, reaps detached descendants and deletes its namespaces/veth pairs on
success, coordinator failure, timeout or SIGTERM/SIGINT. SIGKILL still requires
removing the disposable container. It refuses to start unless explicitly enabled
by the test image, running as root, and the outer namespace contains only loopback.

`python3 topology_test.py` explicitly exercises this contract in the test
container. Its unprivileged socket peers check a multicast probe/reply, RTSP
OPTIONS routing and the outbound CP path; these are transport checks, not the
production ONVIF adapter, RTSP video, central policy or an assembled Go end-to-end
test. It also checks success/failure/timeout/SIGTERM cleanup, including a detached
role child. No hardware or media qualification is inferred from these checks.

## Two-camera qualification topology

From a source checkout, the opt-in `qualification_topology.py` wrapper uses the
same isolation gate and coordinator contract with two independent camera peers:

```text
camera  eth0 (DHCP .20) --+-- edge eth0 bridge (.1)  wlan0 -- CP
camera2 eth0 (DHCP .21) --+
```

The bridge models the isolated PoE switch inside the owned edge namespace;
Wi-Fi stays on a separate link. Both camera clients keep distinct fixed MACs,
and ordinary application processes use the same UID/GID 65534, zero-capability
prefix as the original runner. CP has no camera-LAN route and edge forwarding
remains disabled. The original `topology.py` defaults to its three-namespace
layout. No production interface or network configuration changes for this test.

The manifest keeps its version-1 fields and adds `roles.camera2`. Each camera
role includes `mac` and a stable `logical_identity`. The outer wrapper owns both
DHCP clients and the daemon. Camera/edge application processes can read the
public manifest; only the root coordinator can traverse the `0700` control
directory. Requests and responses are fixed `0600` JSON files at the manifest's
`control.request_file` and `control.response_file` paths.

Write one request at a time by atomically renaming a completed temporary file to
`request.json`. Match the response's `version` and `id`; an older response may
still occupy `response.json` until the new operation completes. Remove the
previous response before submitting a retry with the same ID, as the Python
helper does. For example:

```json
{"version":1,"id":"camera-move-1","op":"move_camera","role":"camera","address":"192.168.250.30"}
```

| Operation | Additional fields | Effect |
| --- | --- | --- |
| `status` | none | Current client/daemon PIDs, observed MACs/IPs and DHCP leases |
| `move_camera` | `role`, `address` | Release and reacquire an in-pool address using the same client/MAC |
| `renew_camera` | `role` | Renew that client's lease |
| `restart_dhcp` | none | Restart the owned daemon while retaining its leases and clients |
| `uplink_down` / `uplink_up` | none | Toggle only the prepared edge Wi-Fi simulation |

Roles are exactly `camera` or `camera2`; requested addresses must be unused by
the other camera and lie in `192.168.250.20`–`192.168.250.99`. Unknown fields,
versions and operations are rejected. Requests accept no command, interface,
path, credential or SQL. IDs are 1–64 ASCII letters/digits/underscores/dashes.
Requests are at most 4 KiB and responses at most 64 KiB. Each operation has a
20-second limit bounded further by the coordinator deadline. The Python
`control_request` helper waits at most 30 seconds and checks response correlation.
Up to 128 completed IDs are retained for exact-response retries during one run;
reusing a retained ID with different contents is rejected. Use a fresh `status`
ID to obtain current state after a previous receipt.

An address move releases the client's existing lease, removes its previous owned
IPv4 address in the DHCP hook, updates the private reservation file, signals
dnsmasq to reload it and reacquires through DHCP. The daemon and other camera
continue running. The wrapper updates the public manifest after the new address
is bound. An already-running camera fixture should listen on its namespace's
wildcard address and keep its logical identity while its advertised endpoint is
updated by the assembled test. The control API does not restart that fixture.

```sh
docker run --rm --network=none --privileged shale-camera-lan-test \
  python3 qualification_topology.py -- python3 qualification_smoke.py
docker run --rm --network=none --privileged shale-camera-lan-test \
  python3 qualification_topology_test.py
```

The protocol checks cover both DORA exchanges, renewal, a same-MAC/client-PID
address move with the old IP actually gone, the other client's unchanged lease,
unprivileged simultaneous multicast/TCP peers, private-control denial to apps,
lease persistence across daemon restart, uplink loss/recovery and full resource
cleanup on success/failure/timeout/SIGTERM. These are network/control checks;
actual camera enrollment, encoded video and live/archive decoding belong to the
assembled Go qualification.

See the [complete generated-media qualification guide](https://github.com/lesomnus/shale/blob/main/deploy/edge/QUALIFICATION.md)
for the combined/independent runtime matrix, recovery faults, CI evidence and
the separate optional physical PoE/Pi procedure.
