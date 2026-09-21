# Shale — Relay

## 39. Relay

The Relay is the live path: it takes a producer's streams and fans them out
to viewers over WebRTC. It exists so that nobody watches a camera *through
the producer*, which is a small machine on a small uplink, and so that live
viewing and recording never share a path. The Relay holds no state, reads no
lamina, and is never between a producer and a Storage Node
([§1](01-overview.md#1-what-shale-is-for)).

```text
                     ┌──────────── Control Plane ────────────┐
                     │  assigns a relay to each producer      │
                     │  signs publish tokens and view tokens  │
                     └───────┬───────────────────────┬────────┘
                             │                       │
        the same TS bytes    ▼                       ▼
Producer ──────────────► Relay ═══ WebRTC ═══► viewers: people (a browser)
   │      gRPC stream,             WHEP           and Reader hosts
   │      on demand
   └──────────────────► Storage Node   (recording, unchanged, §12)
```

### 39.1 What it is

- A **host** like a Storage Node: `shale serve relay --cp <url>`, adopted by
  an operator, holding a certificate the CP issues and renews
  ([§33.4](10-security.md#334-joining-and-adoption)). It is a global entity
  (`Relay`, domain 24) and talks to the cluster API for heartbeats and the
  key set.
- It knows nothing about tenants, sites, or people. Like a Storage Node it
  trusts one thing: a CP signature on a token
  ([§33.2](10-security.md#332-access-tokens)). A producer presents a
  **publish token**, a viewer a **view token**.
- It moves bytes and re-packetizes them. Nothing is transcoded here. The
  one transcode in the whole system is audio to Opus, because browsers
  play no other audio over WebRTC ([§39.4](#394-viewers)), and the producer
  does it on its side, in an ffmpeg it runs only while the camera is
  watched ([§38.7](15-producer.md#387-live-output)).
- Two or more relays are the normal case. Each producer is assigned one
  ([§39.2](#392-assignment)), so a whole set is always on one relay.

### 39.2 Assignment

The CP assigns every producer a relay and records it on the `Producer` row.
The assignment is **sticky**: it changes only when the relay is down, when
an operator moves the producer, or when a relay is erased.

- **Which relay.** A `Site` may carry a `relay_selector`, a set of labels
  (e.g. `region: seoul`). Relays carry `labels`. A producer gets a relay
  whose labels match its site's selector, and any relay when the site has
  none. Among the candidates the CP takes the one with the least attached
  bitrate (the sum of its producers' `max_bitrate_total`s), so load spreads
  without a scheduler.
- **How the producer learns it.** The `Negotiate` answer and every
  `ProducerService.Heartbeat` answer carry the current assignment: the
  relay's endpoints from the address resolver
  ([§34.10](11-deployment.md#3410-node-addresses)) and a **publish token**.
  `ProducerService.Relay` asks for it on demand, which a producer does the
  moment its relay connection breaks.
- **Relay down.** Relays heartbeat like nodes (`heartbeat_interval`,
  `node_down_after`, [§27](09-operations.md#27-node--device--sink-health-and-quarantine)).
  When one is down the CP reassigns its producers at once. A producer whose
  stream broke is already asking, so it attaches to the new relay within a
  few seconds. Viewers follow by asking `Live` again
  ([§39.4](#394-viewers)). Recording is untouched throughout.

### 39.3 From the producer

The producer dials the relay, never the other way round: producers sit
behind NAT and relays are servers. It keeps **one gRPC stream** to its relay
for as long as it runs (`RelayIngest.Attach`,
[§35.8](12-api.md#358-relay-ingest-and-whep)):

```text
Producer  Hello {publish token}          aud = relay, the producer's sources
Relay     Start {source}                 someone is watching this camera
Producer  Data {source, bytes} ...       the video it stores, the audio as Opus
Relay     Stop {source}                  nobody has watched for relay_idle_stop
```

- **Only while someone watches.** No viewer, no bytes: a producer's uplink
  carries its recordings and nothing else. When a viewer arrives the relay
  sends `Start`; the producer begins at the next keyframe, so the first
  bytes are playable. When the last viewer leaves the relay waits
  `relay_idle_stop` (10 s), which absorbs a page reload, then sends `Stop`.
- **The same video.** The producer tees the source's TS stream
  ([§38.1](15-producer.md#381-inputs)): the video that goes to the relay is
  the video that goes into the lamina, never encoded twice. Audio that is
  not Opus is transcoded on the producer's side by its live helper
  ([§38.7](15-producer.md#387-live-output)), so what the relay gets is the
  same video remuxed with Opus. On the uplink it costs the watched cameras'
  bitrate on top of recording, and the link check in
  [§12.2](04-write-path.md#122-resumable-part-uploads) must allow for the
  cameras that are usually watched.
- **Codecs for live.** A camera that will be watched live should record
  H.264 Main or High profile, which every browser plays; the relay serves
  H.265 only to a viewer whose offer includes it and refuses the others.
  Audio arrives as Opus either way: recorded so (`audio.codec: opus` in
  the producer's configuration) it passes through; recorded as AAC or
  anything else, the producer transcodes it while the camera is watched.
  The relay follows the tables of whatever it is sent, so the helper's
  stream need not share the camera's PIDs.
- **No keyframe on request.** A viewer joining mid-stream cannot make the
  encoder produce a keyframe (ffmpeg gives no way to), so the relay keeps
  the current group of pictures in memory instead ([§39.4](#394-viewers)).
- The publish token lives `publish_token_ttl` (24 h) and is checked at
  `Hello`. A producer re-attaching after that asks the CP for a fresh one.

### 39.4 Viewers

A viewer is a person in a browser or a system such as a monitoring wall.
The contract is the same for both; only how they prove themselves to the
tenant API differs: a person signs in, a system is a Reader host with a
certificate ([§33.1](10-security.md#331-trust-model)).

```text
1. Viewer   SetService.Live {set}   or   SourceService.Live {source}
2. CP       per source: relay endpoints (address resolver), view token,
            WHEP URL; every source of a set on the same relay
3. Viewer   POST <relay>/whep/<source>   Content-Type: application/sdp
            Authorization: Shale <view token>      body: SDP offer
   Relay    201, Location: /whep/<session>, body: SDP answer
4. Viewer   plays; DELETE <relay>/whep/<session> when done
```

- **WHEP** is the IETF WebRTC egress protocol, so any WHEP-capable player
  works, and a browser needs no Shale code beyond the `Live` call.
- The **view token** is an access token with `op = view`: `aud` is the
  relay, it names one source and the actor, and it lives `view_token_ttl`
  (1 h). A session already open outlives its token; a new session needs a
  fresh `Live`. The wall and site membership decide who gets one
  ([§33.1](10-security.md#331-trust-model)).
- **Instant start.** The relay keeps, per active source, every packet since
  the last keyframe (at most one keyframe interval, about a megabyte at
  4 Mbps). A joining viewer receives that group of pictures at once and
  starts within a fraction of a second instead of waiting for the next
  keyframe.
- **Latency** is the producer's pipe, one TCP hop, and the browser's jitter
  buffer: one to two seconds glass to glass. Enough for CCTV. If a wireless
  producer ever needs less, the producer-to-relay hop can move to QUIC
  without touching the viewer side.
- **NAT.** The relay offers host candidates and, when configured, STUN and
  TURN (`ice`, [§36.1](13-configuration.md#361-configuration-reference)).
  Inside one network no configuration is needed; viewers on the internet
  need a reachable relay or a TURN server.
- **Limits.** `max_viewers` per relay (500) and `viewers_per_actor` (16,
  from the token's actor), like a node's read sessions
  ([§17.4](05-read-path.md#174-read-locality-and-parallelism)).

Watching a set of eight cameras is eight WHEP sessions to one relay. That
is cheap for a browser and shares one ICE path. A single session carrying
several tracks is a later optimization, not part of the first relay.

### 39.5 Capacity

A relay does no encoding. Its cost is packetization and encryption, so its
limit is egress and viewer count:

```text
egress   = Σ over watched sources of (viewers × bitrate)
example  = 100 viewers × 4 Mbps = 400 Mbps, a fraction of one 10 GbE port
```

Hundreds of viewers per relay are ordinary. A relay per site is the usual
shape, both for the labels in [§39.2](#392-assignment) and so that a
site's viewers and producers share a network. What the **first relay leaves out**:
relay-to-relay cascades for one camera watched from many regions, a
lower-resolution sub-stream for viewers on poor links (many IP cameras
produce one; the producer could forward it), multi-track sessions, and
playing recordings through the relay. Recordings are read from Storage
Nodes as they are today ([§17](05-read-path.md#17-read-path)).

### 39.6 Failures

| Failure | Effect | Recovery |
|---|---|---|
| Relay process restarts | every session and attachment drops; the relay has no state to recover. A stopping relay is graceful for two seconds, then ends what is still open, so an attached producer never keeps a dying relay alive | producers re-attach: the same relay at other endpoints is a restart, and the link moves as soon as a heartbeat brings the new ones; viewers ask `Live` again |
| Relay down | as above, and the CP reassigns its producers | a few seconds of no live picture; recording unaffected |
| Producer's uplink saturated by viewers | live bytes and recording compete | the link check counts watched cameras; the operator sizes the uplink or limits which cameras are watchable |
| Producer down | its cameras are off for viewers and for recording alike | as in [§15](04-write-path.md#15-partial-laminae) |
| CP down | no new `Live` and no new publish tokens; open sessions and attachments continue | as for reads ([§12.1](04-write-path.md#121-flow)): run the CP highly available |

### 39.7 Security

- The relay authenticates to the cluster API with its host certificate and
  serves producers and viewers over TLS with the same certificate, whose
  names follow the address resolver like a node's
  ([§33.5](10-security.md#335-tls)).
- Producers and viewers are authorized by CP-signed tokens only. The relay
  never asks the CP anything about them.
- What a leaked view token buys is one camera for one hour. What a leaked
  publish token buys is the ability to feed false video for that producer's
  cameras to viewers for up to a day, which is why it names the relay and
  the sources and why erasing the producer revokes it at the next `Hello`.
- A compromised relay can serve any stream it carries to anyone and feed
  viewers anything: erase it and adopt the machine again. It cannot reach
  recordings, because it holds no token for a Storage Node.

### 39.8 What the relay does not do

- Record, replay, or read laminae.
- Transcode video, or change resolution or frame rate.
- Talk to cameras or producers on its own initiative: it only answers
  connections that carry a token.
- Decide who may watch. The tenant API decides; the relay checks a
  signature.
