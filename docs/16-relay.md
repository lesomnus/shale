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
        the same fragments   ▼                       ▼
Producer ──────────────► Relay ═══ WebRTC ═══► viewers: people (a browser)
   │      gRPC stream, at all        WHEP           and Reader hosts
   │      times or on demand
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
  does it on its side, as a second audio track its capture writes beside
  the archive's ([§38.7](15-producer.md#387-live-output)).
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
- **Live policy.** The `Producer` row carries `live`: **`always`**, the
  default, has the relay start every source of the producer the moment it
  attaches and stop none, so the relay holds each camera's recent window
  ([§39.4](#394-viewers)) whether or not anyone watches; **`on_demand`**
  starts a source when a viewer arrives and stops it `relay_idle_stop`
  after the last one leaves, for a producer whose uplink cannot carry its
  cameras twice ([§38.5](15-producer.md#385-choosing-the-ceiling)). The CP
  writes the policy into the publish token, which is how the relay learns
  it without asking ([§39.7](#397-security)), and into the assignment, so
  a producer whose policy changed attaches again within a heartbeat. A
  tenant admin sets it with `shale producer patch`, with the row's
  `date_updated` as any patch takes it:

  ```sh
  V=$(shale producer get -o json @acme/lobby-pi | jq -r .dateUpdated)
  shale producer patch @acme/lobby-pi "{\"live\":\"LIVE_POLICY_ON_DEMAND\",\"date_updated\":\"$V\"}"
  ```
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
Producer  Hello {publish token}          aud = relay, the producer's sources, its live policy
Relay     Start {source}                 at once under `always`; when someone watches, on demand
Producer  Data {source, bytes} ...       the fragments it stores, Opus among their audio
Relay     Stop {source}                  on demand only: nobody has watched for relay_idle_stop
```

- **Start and Stop follow the policy** ([§39.2](#392-assignment)). Under
  `always` the relay says `Start` for every source the token names as soon
  as it has welcomed the producer, and never `Stop`: the producer's uplink
  carries its cameras twice, once to the store and once to the relay, and
  the relay has every camera's recent window at all times. Under
  `on_demand` no viewer means no bytes: when a viewer arrives the relay
  sends `Start`, the producer begins at the next keyframe so the first
  bytes are playable, and when the last viewer leaves the relay waits
  `relay_idle_stop` (10 s), which absorbs a page reload, then sends
  `Stop`.
- **The same bytes.** The producer tees the source's fragments
  ([§38.1](15-producer.md#381-inputs)), the init segment first and then one
  message per fragment: what goes to the relay is what goes into the
  lamina, never encoded twice: the video, the archive's audio, and the
  Opus track its capture writes beside it for this purpose
  ([§38.7](15-producer.md#387-live-output)). The relay takes the samples
  out of each fragment for WebRTC, with the parameter sets from the init
  segment in front of every keyframe; a fragment the tee dropped for a
  slow relay is a skipped fragment number (`mfhd`), which the relay sees. Only a stream the producer
  does not encode is remuxed on its way, by the live helper, when its audio
  is not Opus. On the uplink it costs a camera's bitrate on top of
  recording, every camera under `always` and the watched ones on demand,
  and the uplink budget of [§38.5](15-producer.md#385-choosing-the-ceiling)
  counts it.
- **Codecs for live.** A camera that will be watched live should record
  H.264 Main or High profile, which every browser plays; the relay serves
  H.265 only to a viewer whose offer includes it and refuses the others.
  Audio arrives as Opus either way: as the track the producer's capture
  writes beside the archive's, as the only track (`audio.codec: opus`), or
  from the live helper for a stream the producer did not encode. The relay
  takes the Opus stream the tables name and ignores any other audio, and
  follows the tables of whatever it is sent, so the helper's stream need
  not share the camera's PIDs.
- **No keyframe on request.** A viewer joining mid-stream cannot make the
  encoder produce a keyframe (ffmpeg gives no way to), so the relay keeps
  the current group of pictures in memory instead ([§39.4](#394-viewers)).
  A keyframe is of no use without its parameter sets, and an encoder may
  write those once and never again ([§38.2](15-producer.md#382-segments)):
  the producer puts the last ones it saw in front of the keyframe an
  attachment starts at, and the relay keeps the last ones it saw and puts
  them in front of any keyframe that arrives without, across
  re-attachments — so what a joining viewer gets first decodes.
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
4. Viewer   plays; before the token expires, SetService.Live again and
            PATCH <relay>/whep/<session>   Authorization: Shale <fresh token>
   Relay    204: the session now ends with the fresh token
5. Viewer   DELETE <relay>/whep/<session> when done
```

- **WHEP** is the IETF WebRTC egress protocol, so any WHEP-capable player
  works, and a browser needs no Shale code beyond the `Live` call.
- The **view token** is an access token with `op = view`: `aud` is the
  relay, it names one source and the actor, and it lives `view_token_ttl`
  (1 h). The relay ends a session when its token expires, so a viewer still
  watching calls `Live` again before then and **renews** the session: a
  `PATCH` of the session with the fresh token and no body, answered 204,
  moves the session's end to the fresh token's, and the picture does not
  stop. The token has to name the session's source and its actor, or the
  answer is 403; a session already gone is 404, and the viewer opens a new
  one, as one does against a relay that answers `PATCH` with 404 because it
  predates renewal. WHEP's own `PATCH` carries an ICE fragment; this one
  carries no body and is Shale's. The console renews a minute ahead
  (§40). The wall and site membership decide who gets one
  ([§33.1](10-security.md#331-trust-model)). One an app asked for on a
  person's behalf names the app as `delegator` too and lives minutes
  ([§33.8](10-security.md#338-viewing-on-a-persons-behalf)); the relay
  logs it beside the actor when a session opens, and it grants nothing.
- **Instant start.** The relay keeps, per active source, every sample since
  the last keyframe (at most one keyframe interval, about a megabyte at
  4 Mbps). A joining viewer receives that group of pictures at once and
  starts within a fraction of a second instead of waiting for the next
  keyframe.
- **Paced.** A producer sends a fragment at a time, half a second of
  frames, and the relay sends each sample to the viewers when its
  timestamp says, not the fragment's frames at once. A burst every half
  second made a browser's jitter buffer, which starts small, stall at the
  fragments' pace until it had grown to a fragment: the picture stopped and
  went, worst right after joining. The clock is set by the first sample
  sent; a sample more than 100 ms late goes at once and the clock keeps its
  pace from there, and a jump of more than 3 s starts it over. With nobody
  watching nothing is paced. Measured by a WHEP viewer, arrival minus
  timestamp spread over 468 ms unpaced and 2 ms paced.
- **The recent window.** Nothing is read from a Storage Node before it
  commits ([§17](05-read-path.md#17-read-path)), so the one stretch of a
  camera nobody can read is the open lamina's, and that is what the relay
  keeps: per source, the last `rewind_seconds` of the fragments as the
  producer sent them, where `rewind_seconds` is what the publish token
  says ([§33.2](10-security.md#332-access-tokens)) and the CP sized it
  from the longest segment duration among the producer's sources plus
  15 s for the last part and the commit, so the window always reaches
  back past the end of the last lamina a Storage Node can serve. It is not
  a setting: at 2 Mbps and the default 64 MB target that is 256 s + 15 s,
  at a 4 MiB target 17 s + 15 s, and in bytes always about one lamina.

  ```text
  GET <relay>/recent/<source>?since=<seconds>   Authorization: Shale <view token>
      → 200  Content-Type: video/mp4
             Shale-Recent-Start: <when the first keyframe came, RFC 3339>
             Shale-Recent-Seconds: <how long the window from it is>
      → 404  nothing kept yet
  ```

  The answer is one fragmented MP4 in the shape of a lamina: the init
  segment first, then the fragments from the oldest key fragment kept, or
  from the oldest that came within `since` seconds, without the index a
  closed lamina ends with; a browser's Media Source Extensions take it as
  it is, and so does ffmpeg. It is a snapshot, not a stream: a viewer
  plays it, then joins live over WHEP, or asks again. The same encoder
  wrote it and the laminae, so the decode times run on across them, and a
  player that has the laminae from `Timeline` and this blob has the camera
  from the archive to now with nothing between. `Live` hands out the URL as
  `recent_url` beside `whep_url`, good for the same view token. The relay
  never grows into a playback server: what is older than the window is the
  Storage Nodes' to serve, as it is today.

  What was never sent is not in it: under `on_demand` the window fills
  from the first viewer on. A fragment the producer dropped for a slow
  relay, or a stream that broke, leaves a tear, which the relay sees as a
  skipped fragment number, and the window is handed out from the next key
  fragment after the last tear, never across one. A relay that
  restarts, or a producer that attaches again, begins an empty window.
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
- **Browsers.** A browser trusts the public CAs, not the shale CA, and a
  page's `fetch` to a certificate it does not trust fails without a word.
  So the WHEP listener, which serves `/whep/` and `/recent/` both, can
  serve an **external certificate** instead of the host's
  (`whep_cert_file`, `whep_key_file`, e.g. Let's Encrypt from cert-manager
  mounted as files), read again when the files change, so a renewal needs
  no restart ([§33.5](10-security.md#335-tls)). `whep_advertise` names the
  listener as that certificate does, a host or host:port, and the CP hands
  a name out as it is rather than through the address resolver
  ([§34.10](11-deployment.md#3410-node-addresses)): `Live` answers
  `https://live.example.com:7441/whep/<source>`. Ingest keeps the host
  certificate and its own address, since producers verify it against the
  shale CA ([§39.7](#397-security)).
- **Cross-origin.** The console is on another origin than the relay. The
  WHEP listener answers any origin, as the data plane does
  ([§40.5](17-console.md#405-playback)): the view token is the credential,
  not a cookie. The preflight allows `Authorization` and `Content-Type`,
  and a `POST` exposes `Location` and `Link`, so a page can end its
  session; `/recent/` exposes `Shale-Recent-Start` and
  `Shale-Recent-Seconds`.

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
site's viewers and producers share a network.

Memory is the recent windows ([§39.4](#394-viewers)): per source, its
bitrate × its window, which is about one lamina, 64 MB at the default
target; sixteen cameras are a gigabyte. `rewind_budget` (1 GiB) bounds
what the windows hold together: over it, the largest window loses its
oldest bytes, so a relay never grows past what it was given, and its
heartbeat reports both numbers (`rewind_bytes`, `rewind_budget`). The CP
passes over a relay whose windows are at their budget when another fits
([§39.2](#392-assignment)), and takes it only when there is no other.

What the **first relay leaves out**: relay-to-relay cascades for one camera
watched from many regions, a lower-resolution sub-stream for viewers on
poor links (many IP cameras produce one; the producer could forward it),
multi-track sessions, and playing recordings through the relay. Recordings
are read from Storage Nodes as they are today
([§17](05-read-path.md#17-read-path)); the relay serves the one stretch
they cannot, the open lamina's.

### 39.6 Failures

| Failure | Effect | Recovery |
|---|---|---|
| Relay process restarts | every session and attachment drops; the relay has no state to recover, and the recent windows start empty. A stopping relay is graceful for two seconds, then ends what is still open, so an attached producer never keeps a dying relay alive | producers re-attach: the same relay at other endpoints is a restart, and the link moves as soon as a heartbeat brings the new ones; viewers ask `Live` again; the windows fill again from the re-attach on |
| Relay down | as above, and the CP reassigns its producers | a few seconds of no live picture; recording unaffected |
| Producer's uplink saturated by live | live bytes and recording compete; the tee drops live bytes rather than hold the recording, the producer's heartbeat counts them, and each drop tears the recent window, which is handed out from the next keyframe after it | the uplink budget counts every camera once more under `always` ([§38.5](15-producer.md#385-choosing-the-ceiling)); the operator sizes the uplink, or sets the producer `on_demand` ([§39.2](#392-assignment)) |
| Producer down | its cameras are off for viewers and for recording alike | as in [§15](04-write-path.md#15-partial-laminae) |
| CP down | no new `Live` and no new publish tokens; open sessions and attachments continue | as for reads ([§12.1](04-write-path.md#121-flow)): run the CP highly available |

### 39.7 Security

- The relay authenticates to the cluster API with its host certificate and
  serves producers and viewers over TLS with the same certificate, whose
  names follow the address resolver like a node's
  ([§33.5](10-security.md#335-tls)). The WHEP listener may serve an
  external certificate instead, for browsers ([§39.4](#394-viewers));
  ingest always serves the host certificate.
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

- Record, replay, or read laminae. It keeps the open lamina's fragments as
  the recent window and hands them out as one fragmented MP4
  ([§39.4](#394-viewers)), and nothing older.
- Transcode video, or change resolution or frame rate.
- Talk to cameras or producers on its own initiative: it only answers
  connections that carry a token.
- Decide who may watch. The tenant API decides; the relay checks a
  signature.
