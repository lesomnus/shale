# Shale — Satellite

## 41. Satellite

A Satellite is a host producers upload through. It speaks a Storage Node's
upload protocol to them ([§35.6](12-api.md#356-storage-node-http-data-plane))
and forwards every request to the node the request's put token names, at the
address the Control Plane gives for that node. To a producer it looks like a
Storage Node; to a node, every request still comes from the producer that
holds the token.

It answers three limits of a producer that uploads by itself:

1. **Its uplink carries live twice.** With live on, the same stream goes once
   to the node that takes the lamina and once to the relay
   ([§39.3](16-relay.md#393-from-the-producer)). A satellite on the far side
   of that uplink takes the stream once and feeds the relay from the upload
   ([§41.8](#418-live-fan-out)).
2. **It must reach every node.** Placement may choose any node
   ([§11](03-placement.md#11-placement)), so every node's data plane must be
   routable from every edge network ([§34.10](11-deployment.md#3410-node-addresses)).
   Through a satellite, a producer reaches one host, and only the satellite
   reaches the nodes.
3. **Its RAM is the outage budget.** Under `retain: committed` a producer
   holds whole segments until they are committed
   ([§12.2](04-write-path.md#122-resumable-part-uploads)). A satellite that
   holds bytes for it would free the edge's RAM sooner. Not yet:
   [§41.9](#419-not-yet).

It solves (1) and (2) and lays the ground for (3): the satellite moves
bytes, and reads the live out of them as they pass, so it costs the write
path nothing it has today.

```text
                       ┌──────────── Control Plane ─────────────┐
                       │  assigns a satellite to each producer   │
                       │  that asks; tells each satellite whom   │
                       │  it serves, with a feed token for the   │
                       │  live they hand it; resolves nodes      │
                       └───────┬─────────────────────┬───────────┘
                               │ allocations, tokens │ producers, routes, feed tokens
                               ▼                     ▼
Producer ── PUT/HEAD, mTLS ──► Satellite ── PUT/HEAD ──► the node the token names
         ◄── the node's answer, unchanged ──────────────
                                   │
                                   └── RelayIngest, mTLS ──► the producer's relay
                                       the live it was handed, from the upload
```

### 41.1 What it is

- A **host** like a relay: `shale serve satellite --cp <url>`, joined and
  adopted ([§33.4](10-security.md#334-joining-and-adoption)), holding a
  certificate the CP issues and renews and following the CA through a
  rollover ([§33.5](10-security.md#335-tls)). It is a global entity
  (`Satellite`, domain 43) and talks to the cluster API: heartbeats, routes,
  and the key set ([§33.3](10-security.md#333-signing-keys-and-rotation)),
  which it holds and reports as a node does.
- It is **trusted like a node**: it sees the bytes and carries the tokens.
  It is **not a placement target**. Allocations, put tokens and the index
  name the real node; laminae never move, and a satellite holds none.
- Its state directory holds its key, certificate and CA bundle, the cached
  key set, and the last list of producers it serves, so a satellite that
  restarts while the CP is away serves as it did
  ([§34.3](11-deployment.md#343-state-on-disk)).
- It is optional. A cluster that runs none, and a producer that does not ask
  for one, upload as they always have.

### 41.2 Assignment

The CP assigns a satellite to every producer that asks for one, as it assigns
a relay ([§39.2](16-relay.md#392-assignment)), and records it on the
`Producer` row. The assignment is **sticky**: it changes when the satellite
is down, erased, or no longer adopted, when an operator moves the producer
(`shale satellite assign`), or when the producer stops asking.

- **Who asks.** A producer whose `producer.upload_via` is `satellite` or
  `auto` ([§41.5](#415-from-the-producer)) says so in its `Negotiate` request
  (`satellite: true`) and in every heartbeat (`upload.via`). One that says
  nothing, which is every producer of an earlier release, or `direct`, is
  assigned none, and one that stops asking is let go by the satellite it had.
- **Which satellite.** A `Site` may carry a `satellite_selector`, labels a
  satellite must carry, as `relay_selector` is for relays. Among the live
  satellites that match, the CP prefers those whose last heartbeat said
  their in-flight pool had room ([§41.4](#414-memory)), and takes the one
  with the least attached bitrate (the sum of the `max_bitrate_total`s of the
  sets whose producers it serves). A satellite that is full is assigned only
  when no other matches. The CP never assigns one that is erased, not
  adopted, or silent: unheard for `control.satellite_down_after` (6 s,
  three of its 2 s heartbeats, as a relay).
- **How the producer learns it.** The `Negotiate` answer and every
  `ProducerService.Heartbeat` answer carry a `SatelliteAssignment`: the
  satellite's id, its upload listener resolved for the producer's network
  by the address resolver ([§34.10](11-deployment.md#3410-node-addresses)),
  and an expiry 30 minutes on. Every answer renews it; one that carries none
  means none, and the producer uploads directly. The expiry outlasts an
  allocation's TTL, so a producer that cannot reach the CP goes on through
  its satellite for as long as its tokens let it.
- **How the satellite learns it.** Every `SatelliteService.Heartbeat` answer
  carries the whole list of adopted producers assigned to it, each with the
  serial of the certificate its row records. The list replaces the last one.
  A caller the satellite does not know, or whose certificate is not the one
  in the list, has it ask again at once, at most once a second, before it is
  refused: a producer assigned a moment ago, or one that renewed its
  certificate, is served within a request.
- **Satellite down.** The CP stops naming it once it has not been heard for
  `satellite_down_after`; each of its producers is assigned another at its
  next negotiation or heartbeat, or none. A satellite that comes back is
  assigned producers again as any other.

### 41.3 The upload proxy

A satellite serves the upload half of a node's data plane, with the same
paths, headers, trailers and status codes:

```text
PUT   /laminae/{lamina_key}   as on a node (§35.6)
HEAD  /laminae/{lamina_key}   as on a node, with a put token
GET   /healthz
```

Each request is checked, routed and forwarded in this order, and refused
at the first check it fails. Nothing of it reaches a node before every check
passed.

1. **Who calls.** The upload listener is TLS with the satellite's host
   certificate, HTTP/1.1 and HTTP/2, and requires a client certificate
   verified against the CA bundle the satellite holds at each handshake. The
   caller is the producer its certificate names, and it must be one in the
   list the CP gave, with the certificate serial the CP records, as the CP
   checks its own callers ([§33.4](10-security.md#334-joining-and-adoption)).
   A node's, a relay's or another satellite's certificate, an unassigned or
   erased producer's, and a replaced certificate are refused `403`.
2. **The token.** `Authorization: Shale <token>` or `?token=`, checked as
   the node it names would check it
   ([§33.2](10-security.md#332-access-tokens)): signature, key, expiry with
   `satellite.token_skew`, a **put** token (`HEAD` takes a put token too),
   the key in the path. And one check more: the token's actor is the calling
   producer. A token another producer leaked is useless here. A missing or
   bad token is `401`, a token for something else `403`.
3. **Where.** The node is the token's audience, and its address is the one
   `SatelliteService.Route` answers for that node: its data plane as the
   address resolver gives it for the satellite's own network, kept for the
   expiry the answer gives (a minute), and used for up to an hour past it
   while the CP cannot be asked. Nothing a caller sends names a host: not
   the URL's authority, `Host`, `Forwarded` or any other header. The path
   is the token's key. A satellite is **not an open proxy**: it reaches the
   data planes of adopted nodes, at the upload paths, for requests a CP
   token authorized for that node. A node the CP has no route to is `502`.
4. **Room.** A `PUT` takes its share of the in-flight pool
   ([§41.4](#414-memory)), or is refused `503` with `Retry-After`, as a busy
   node refuses one. A `HEAD` carries no body and takes none.
5. **Forwarding.** The body is streamed to the node through a buffer of
   `satellite.request_buffer`, never held whole. The request's headers go on
   as they came, but the hop's own (`Connection` and those it names,
   `Transfer-Encoding`, `Te`, `Trailer`, `Upgrade`, `Proxy-*`) and the token,
   which goes in `Authorization` whichever way it came, so it is in no URL
   the node sees. The trailers of a live upload (`Shale-Date-Ended`) follow
   the body. The node's answer comes back unchanged: the status, every
   header but the hop's own and `Alt-Svc` (which describes the node's
   origin, not the satellite's), the body, and the trailers. `Upload-Offset`,
   `Upload-Complete`, `Shale-Incomplete`, `Retry-After`, `409`, `413` and
   `503` are the node's own.

**The satellite's own answers** carry `Shale-Satellite`, which a node never
sends, with the reason: `unassigned` (the caller is not a producer it
serves), `token`, `busy`, `unreachable` (no route to the node, or the node
did not answer: `502`), and `request` (a path or a method the upload paths
do not have). A producer tells the satellite's refusals from the node's by
it ([§41.5](#415-from-the-producer)).

**A node's limits per caller.** A node counts uploads per actor
(`uploads_per_actor`, [§12.2](04-write-path.md#122-resumable-part-uploads))
by the token's actor, which is the producer, and nothing on a node's data
plane is counted by the caller's address. Behind a satellite every producer
is still itself to the node, so nothing is forwarded about who the producer
is, and no header a satellite could set would be believed: the token says
it, and the CP signed it.

**Never logged:** tokens, and the query strings and URLs that could carry
them.

**Development mode** is plaintext, without client certificates
([§34.7](11-deployment.md#347-single-machine)): the producer names itself in
`Shale-Producer`, as the plain header names a caller to the CP, and the
satellite believes it there and nowhere else. The header goes no further.

### 41.4 Memory

A satellite's memory is **one budget**, `satellite.memory_budget` (256 MiB),
split into pools. Today there is one, the **in-flight pool**: the bytes of
requests passing through under `retain: committed` and `written`.

- A `PUT` reserves twice `satellite.request_buffer` (256 KiB): the buffer its
  body is copied through, and what HTTP/2 lets the producer send ahead of
  it, which the listener bounds to the same size per stream. So a 256 MiB
  budget is 512 uploads passing through at once, about one per source.
- A request that finds no room is refused `503` with `Retry-After: 5` and
  `Shale-Satellite: busy`. Nothing is queued or evicted: the producer keeps
  its bytes and asks again, as for a busy node.
- Every heartbeat reports the pool, what it holds and its bound, the
  requests passing through, the producers assigned, the bytes forwarded and
  the requests refused (`SatelliteStatus`), and the CP prefers satellites
  with room when it assigns ([§41.2](#412-assignment)). The same are the
  metrics `shale.satellite.*` ([§31](09-operations.md#31-observability)).

The retained pool of `retain: accepted` is to sit beside this one, in the
same budget ([§41.9](#419-not-yet)).

The live fan-out ([§41.8](#418-live-fan-out)) holds no pool. What it holds
is bounded otherwise: per source it feeds, the fragment being read out of
the upload (32 MiB at most, as the relay takes one); per producer stream to
a relay, 32 fragments waiting to be sent, beyond which they are dropped, as
the producer's tee does. It never reserves room in the in-flight pool and
never refuses a request.

### 41.5 From the producer

`producer.upload_via` says where a producer's upload requests go:

| `upload_via` | Requests go to | While no satellite is assigned | When the satellite cannot be reached |
|---|---|---|---|
| `direct` (default) | the node an allocation names, as before | — | — |
| `satellite` | the assigned satellite | the node, and the log says so once | it waits, and has its heartbeat ask the CP again; the CP assigns another, or none, once it holds the satellite down |
| `auto` | the assigned satellite | the node | the node at once, for a minute, doubling with every failure in a row up to 15 min, as a failed HTTP/3 handshake does ([§35.6](12-api.md#356-storage-node-http-data-plane)) |

- **Same requests.** Through a satellite a request is the one the node would
  be sent: the path, the token, the headers, the body. Nothing of the upload
  client changes: resume from `HEAD`, `409` offsets, `Retry-After`, failover
  across candidates and `Reallocate` ([§13](04-write-path.md#13-retry-and-reallocation))
  are the producer's, as they always were. The path is chosen again at every
  request, so a satellite that fails or is replaced is left at the next.
- **Whose failure.** A node's answer that comes through the satellite is the
  node's and is handled exactly as today: a relayed `503` is waited out on
  the same candidate, a `409` resumes from the node's offset, a `413` or a
  node's `403` fails the candidate. The satellite's own failure is the
  satellite's and never counts against the candidate: a connection or TLS
  failure, no answer, or its refusal of this producer
  (`Shale-Satellite: unassigned`). Then the producer asks where the upload
  stands where its requests go now (the node directly, on `auto`), and
  resumes from there. Its `busy` `503` is a busy node's: waited out.
- **Back to the satellite.** On `auto`, a request sent directly that does not
  reach the node either means the producer's own link may be what failed,
  and the satellite is tried again at once, as HTTP/3 is after TCP fails as
  well.
- **What it reports.** A producer on `satellite` or `auto` reports in its
  heartbeat the path asked for, the satellite it holds, and the requests
  that went through it and directly, and the fallbacks, since the last
  heartbeat (`UploadReport`, kept on the row's status). The metrics are
  `shale.producer.upload_requests` by `path` and
  `shale.producer.satellite_fallbacks`. A producer on `direct` reports
  nothing new. It reports too the sources whose live it hands the
  satellite, and `live_feed` ([§41.8](#418-live-fan-out)).
- **Live.** A live request through the satellite may hand it the source's
  live, so the producer's uplink carries it once
  ([§41.8](#418-live-fan-out)).
- **Certificates.** A producer that renews its certificate closes its idle
  upload connections, so the next request presents the certificate the CP
  now records, which is the one the satellite lets in.
- **HTTP/3.** A satellite serves HTTP/1.1 and HTTP/2. `producer.http3: auto`
  never takes it to one, since the satellite passes no node's `Alt-Svc`
  on; `on` tries, and falls back to TCP after the handshake fails, as it
  does for a node that does not serve it
  ([§35.6](12-api.md#356-storage-node-http-data-plane)).

### 41.6 Failures

Under `retain: committed` and `written` the satellite moves bytes and holds
none a producer does not hold too: a satellite that dies loses nothing.

| Failure | Effect | Recovery |
|---|---|---|
| Satellite dies under an upload | the producer's request breaks; the node's request from the satellite ends with it, and the node keeps the aligned prefix it had, the upload resumable, as for any broken request ([§12.2](04-write-path.md#122-resumable-part-uploads)) | `auto`: the producer asks the node directly where the upload stands and resumes there at once. `satellite`: it waits, the CP holds the satellite down after `satellite_down_after` and answers another or none at the next heartbeat (the failure wakes it), and the producer resumes from the node's offset. Under `committed` nothing is lost; under `written` nothing more than without a satellite |
| Satellite stops | graceful for two seconds, then it ends what is still open: a live upload lasts its segment, and a satellite that waited would not stop | as above |
| Satellite full | `503 Retry-After`, `Shale-Satellite: busy` | the producer waits it out on the same candidate, as for a busy node; the CP assigns satellites with room first |
| Node down behind a satellite | `502 Shale-Satellite: unreachable` | the node's failure: the producer resumes, then moves to the next candidate after `resume_timeout`, as without a satellite |
| CP down | no new assignments or routes; the satellite serves the producers it last heard of, at the routes it holds, an hour past their expiry | tokens bound it, as everywhere ([§12.1](04-write-path.md#121-flow)) |
| A producer not yet in the satellite's list | the satellite asks the CP at once, then serves or refuses `403 unassigned` | the producer's failure is the satellite's: `auto` goes directly, `satellite` asks again |
| Satellite erased | the CP stops assigning it and its certificate is refused at the cluster API | its producers are assigned another, or none, at their next heartbeat |
| Satellite dies while it feeds the relay | its relay streams end; the relay hands each source back to the producer's own stream at once ([§41.8](#418-live-fan-out)) | the producer's tee feeds from its next keyframe: a viewer waits a keyframe interval at most; the producer's uploads go as the rows above say |
| A live upload through it breaks | it stops feeding the source at once, and the relay hands it back to the producer's stream | it feeds again from the request that resumes, past any backlog older than 2 s |
| Relay behind | the satellite drops live fragments beyond 32 queued, counted (`live_dropped`); the uploads are not held | the relay sees the gaps as a torn fragment sequence, as from a producer's tee ([§39.3](16-relay.md#393-from-the-producer)) |

### 41.7 Security

- The satellite is trusted like a node: it carries put tokens and sees the
  bytes. What it may do on the cluster API is its own heartbeat and
  certificate, `Route`, and reading the key set; nothing of any tenant's
  rows (`server/core/gatepolicy.go`). The producers it serves come in its
  heartbeat's answer.
- A put token is a bearer secret for one key on one node for a few minutes
  ([§33.2](10-security.md#332-access-tokens)). A satellite never stores one,
  never logs one, and only sends one on to the node it names.
- The proxy is closed three ways: who calls (an adopted producer assigned to
  it, by mTLS), what for (a CP-signed put token for a key, issued to that
  producer), and where to (the token's node, at the CP's address for it). A
  stolen producer certificate without a token, or a token without the
  producer's certificate, buys nothing.
- Key rotation waits for every live satellite, as for every live node, to
  report the new key before the CP signs with it
  ([§33.3](10-security.md#333-signing-keys-and-rotation)), and a CA rollover
  waits for satellites as for every host ([§33.5](10-security.md#335-tls)).
- A satellite's **feed token** is a publish token in the name of a producer
  it serves, for the sources that producer hands it and no others, for the
  producer's relay, and only from the satellite's own certificate
  ([§39.7](16-relay.md#397-security)); it lives ten minutes at most. The
  CP issues one only when the producer's last heartbeat names this
  satellite and those sources, and its relay says it takes satellites.
- A compromised satellite can read and alter what passes through it, and
  write with the tokens it sees, which name keys the CP allocated: no more
  than a compromised node holding the same uploads. Through its feed tokens
  it can show viewers what it likes of the cameras its producers hand it,
  as a compromised producer can of its own. Erase it and adopt the machine
  again.

### 41.8 Live fan-out

With live on, a producer that uploads by itself sends each camera twice
over its uplink: once as the lamina, once over its tee to the relay
([§38.7](15-producer.md#387-live-output)). Through a satellite, under the
`live` upload mode, the satellite sees every byte of the lamina as it is
made. So it feeds the relay from the upload, and the producer's tee stays
off for that camera: the uplink carries each byte once.

```text
Producer ── PUT, Shale-Live: <offset> ──► Satellite ──► node
                                           │ reads fragments out as they pass
                                           ▼
Producer's tee (stopped) ···► Relay ◄── Feed {sources}, Data ── Satellite
```

**Which sources.** The producer decides, per source and per request. A
source's live is handed to the satellite while all of these hold:

- its live uploads go through the satellite (`upload_via: satellite` or
  `auto`, a satellite assigned and not paused), in the `live` upload mode;
- its stream needs no live helper: video with Opus audio, or none
  ([§38.7](15-producer.md#387-live-output)). A satellite runs no ffmpeg; a
  camera whose audio the helper transcodes stays on the producer's tee, and
  so does a raw input;
- `producer.live_feed` is `auto`, the default; `producer` keeps the tee for
  every source.

A live request through the satellite of such a source carries
`Shale-Live: <offset>`: where in the lamina its live begins. That is where
the request begins, unless the bytes there were written more than 2 s ago:
a request that resumes after an outage, or a lamina whose upload began
late, starts with a backlog, which goes to the node and not to viewers, and
its live begins at the first byte written since. The satellite takes the
header off before the request goes on to the node. The producer's heartbeat
names the sources it hands over (`UploadReport.live_sources`, and
`live_feed`), and says so at once when they change.

**Authority.** For each producer it serves whose last heartbeat hands it
sources, the satellite's heartbeat answer carries the producer's relay and
a **feed token** (`SatelliteProducer.relay`): the producer's publish token
([§39.2](16-relay.md#392-assignment)) in all but three things: the
operation is `feed`, it names only the sources the producer hands over
that its own token names, and `feeder` names the satellite. The relay
takes it only from the satellite's client certificate
([§39.7](16-relay.md#397-security)); it lives ten minutes and every
heartbeat brings a fresh one, which the satellite sends on its stream when
it says something new, or half its life is gone. None is given for a
relay that predates satellites (it says, in its heartbeat, that it takes
them); such a relay would refuse the operation anyway.

**The stream.** The satellite keeps one stream per such producer to its
relay, the producer's tee's protocol ([§39.3](16-relay.md#393-from-the-producer)):
`Hello` with the feed token, `Start` and `Stop` from the relay, `Data` one
init segment or one whole fragment at a time. And `Feed`: the sources it
feeds now. It feeds a source while the live of a request that hands it
over passes through it, from that request's `Shale-Live` offset on:

- the relay takes it as the source's feeder over the producer's own
  stream, tells the tee `Stop` and the satellite `Start`, if the source is
  started ([§39.3](16-relay.md#393-from-the-producer));
- after `Start` it sends from the next key fragment, the init segment in
  front, as the tee does;
- the bytes are read as they pass: a fragment goes to the relay the moment
  its last byte is read, never batched with the next (about a millisecond
  on a host, measured), and a fragment it was not asked for passes kept
  nowhere;
- **lamina boundaries are seamless.** A source's next lamina is the next
  request, and its feed continues: an init segment that is the one sent
  last is not sent again, and the fragments go on in their sequence. A new
  init segment, or a break in the fragments' sequence, is a stream that
  began again: from the next key fragment, with the init segment in front;
- a request that resumes where one broke is read on from where the
  satellite was; what it read already is passed over, and the feed goes on
  from the next key fragment once the relay starts it again;
- **capture epochs** are the upload's: each `Data` carries the capture
  epoch its put token's record names, which the relay checks against the
  feed token's grant; a token that grants another capture is not used to
  feed, and the satellite asks the CP for a fresh one at once. `Reset`
  stays the producer's, on its own stream, which the relay takes while the
  satellite feeds ([§39.3](16-relay.md#393-from-the-producer)).

**Handoff.** One feeder at a time, decided by the relay and nothing else:
a satellite that says it feeds a source, the last to say so, else the
producer's own stream ([§39.3](16-relay.md#393-from-the-producer)). The
producer's tee stays attached throughout and sends only what the relay
starts, so no source is ever sent twice.

| Event | The relay's feeder | A viewer |
|---|---|---|
| The producer's live upload of a source reaches the satellite with `Shale-Live` | the satellite, at its `Feed`; the tee is told `Stop` | the next keyframe, from the satellite |
| A lamina ends and the next begins within 3 s | the satellite, unchanged | nothing: the feed goes on |
| No next lamina within 3 s (a node's slow commit, a dark segment held back, §38.10) | the producer's stream, started | the next keyframe, from the tee |
| A live upload through the satellite breaks | the producer's stream, at once | the next keyframe, from the tee |
| The producer goes directly (`auto` after a failure, no satellite assigned) | the producer's stream: the satellite's upload ended, and the next one does not reach it | the next keyframe, from the tee |
| The satellite stops or dies | the producer's stream, the moment the satellite's relay stream ends | the next keyframe, from the tee |
| The producer is assigned another satellite | the one its live uploads pass through, the newer to say so | the next keyframe |
| A new capture epoch | the producer's stream until the satellite's token grants the new one, then the satellite | the next keyframe |

**Bounded and never in the way.** The upload is what matters. The tap reads
the bytes the proxy copies anyway, under a lock held for the parsing of
what was just read and nothing else; a fragment is queued without waiting.
Each producer's stream queues 32 fragments at most, as the producer's tee
does; beyond that, a relay that is behind loses fragments, which are
counted, and the upload goes on at its own pace. The heartbeat reports the
sources fed, the bytes sent and the fragments dropped (`SatelliteStatus`
`live_sources`, `live_bytes`, `live_dropped`), as do
`shale.satellite.live_*`; the relay counts handoffs
(`shale.relay.feeder_handoffs`).

**Compatibility.** With no satellite, or `upload_via: direct`, nothing of
this happens. A producer that predates it sends no `Shale-Live` and hands
nothing over; a satellite that predates it ignores the header and the feed
token, and the producer's tee feeds as before; a relay that predates it is
given no feed token, and would refuse one.

### 41.9 Not yet

- **`retain: accepted`.** Store-and-forward: the satellite holding the bytes
  in a **retained pool** beside the in-flight one, in the same
  `memory_budget`, with a share per producer, so an edge host frees its RAM
  as soon as the satellite has the bytes. Opt-in per producer, refusable per
  satellite; what it accepted and has not delivered is lost if it dies, and
  is counted as lost. It needs a delegation scoped to the allocation, so the
  satellite completes the lamina at the CP for a producer that moved on.
  `SatelliteStatus` keeps fields 9 and 10 for that pool.
- A satellite in the process of `shale serve all`, where both its hops are
  loopback; HTTP/3 to the satellite; reads through a satellite for viewers
  who reach it but not the nodes.
