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
   of that uplink could take the stream once and fan it out over the LAN.
   Not yet: [§41.8](#418-not-yet).
2. **It must reach every node.** Placement may choose any node
   ([§11](03-placement.md#11-placement)), so every node's data plane must be
   routable from every edge network ([§34.10](11-deployment.md#3410-node-addresses)).
   Through a satellite, a producer reaches one host, and only the satellite
   reaches the nodes.
3. **Its RAM is the outage budget.** Under `retain: committed` a producer
   holds whole segments until they are committed
   ([§12.2](04-write-path.md#122-resumable-part-uploads)). Under `retain:
   accepted` the satellite holds them instead, and delivers them itself
   ([§41.9](#419-retain-accepted)): the edge's RAM is freed as soon as the
   satellite has the bytes, at the price that a satellite that dies loses
   what it held.

Under `retain: committed` and `written` the satellite moves bytes and nothing
else, so it costs the write path nothing it has today; that solves (2).
`retain: accepted` is opt-in per producer and solves (3).

```text
                       ┌──────────── Control Plane ─────────────┐
                       │  assigns a satellite to each producer   │
                       │  that asks; tells each satellite whom   │
                       │  it serves; resolves nodes for it       │
                       └───────┬─────────────────────┬───────────┘
                               │ allocations, tokens │ producers, routes
                               ▼                     ▼
Producer ── PUT/HEAD, mTLS ──► Satellite ── PUT/HEAD ──► the node the token names
         ◄── the node's answer, unchanged ──────────────
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
  whether it takes `retain: accepted` ([§41.9](#419-retain-accepted)),
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
   `503` are the node's own. The headers of `retain: accepted`
   (`Shale-Retain`, `Shale-Handoff`, `Shale-Accepted`, `Shale-Pass`,
   [§41.9](#419-retain-accepted)) go to no node and are taken from none:
   the handoff carries every candidate's token, and a node is sent its own.

A request under `retain: accepted` that the satellite takes is answered by
the satellite after step 4, and stored rather than forwarded
([§41.9](#419-retain-accepted)); one it does not take goes on through step 5
as any other.

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
split into two pools:

| Pool | What it holds | Size |
|---|---|---|
| **in flight** | the buffers of requests passing through: every `PUT`, and the whole of one under `retain: committed` and `written` | `memory_budget` − `retained_budget` |
| **retained** | the bytes of segments taken under `retain: accepted` and not yet delivered ([§41.9](#419-retain-accepted)) | `satellite.retained_budget`, half the budget by default; none under `satellite.accept: false`, which gives the in-flight pool the whole budget |

One number sizes the machine, as a node's `part_buffer_pool` does, and the
retained pool, the only memory whose loss loses data, has a bound of its
own that pass-through traffic cannot take from it, nor it from that.

- A `PUT` reserves twice `satellite.request_buffer` (256 KiB) of the
  in-flight pool: the buffer its body is copied through, and what HTTP/2
  lets the producer send ahead of it, which the listener bounds to the same
  size per stream. So 128 MiB of it is 256 requests passing through at once.
- A request that finds no room in it is refused `503` with `Retry-After: 5`
  and `Shale-Satellite: busy`. Nothing is queued or evicted: the producer
  keeps its bytes and asks again, as for a busy node.
- Every heartbeat reports both pools, what each holds and its bound, the
  retained pool by producer, the requests passing through, the producers
  assigned, the bytes forwarded and the requests refused (`SatelliteStatus`),
  and the CP prefers satellites whose in-flight pool has room when it
  assigns ([§41.2](#412-assignment)). The same are the metrics
  `shale.satellite.*` ([§31](09-operations.md#31-observability)).

The retained pool's own rules, its shares and what a full one does, are
in [§41.9](#419-retain-accepted).

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
  nothing new.
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
Under `retain: accepted` it holds what the producer let go, and its own
failures are in [§41.9](#419-retain-accepted).

| Failure | Effect | Recovery |
|---|---|---|
| Satellite dies under an upload | the producer's request breaks; the node's request from the satellite ends with it, and the node keeps the aligned prefix it had, the upload resumable, as for any broken request ([§12.2](04-write-path.md#122-resumable-part-uploads)) | `auto`: the producer asks the node directly where the upload stands and resumes there at once. `satellite`: it waits, the CP holds the satellite down after `satellite_down_after` and answers another or none at the next heartbeat (the failure wakes it), and the producer resumes from the node's offset. Under `committed` nothing is lost; under `written` nothing more than without a satellite |
| Satellite stops | graceful for two seconds, then it ends what is still open: a live upload lasts its segment, and a satellite that waited would not stop | as above |
| Satellite full | `503 Retry-After`, `Shale-Satellite: busy` | the producer waits it out on the same candidate, as for a busy node; the CP assigns satellites with room first |
| Node down behind a satellite | `502 Shale-Satellite: unreachable` | the node's failure: the producer resumes, then moves to the next candidate after `resume_timeout`, as without a satellite |
| CP down | no new assignments or routes; the satellite serves the producers it last heard of, at the routes it holds, an hour past their expiry | tokens bound it, as everywhere ([§12.1](04-write-path.md#121-flow)) |
| A producer not yet in the satellite's list | the satellite asks the CP at once, then serves or refuses `403 unassigned` | the producer's failure is the satellite's: `auto` goes directly, `satellite` asks again |
| Satellite erased | the CP stops assigning it and its certificate is refused at the cluster API | its producers are assigned another, or none, at their next heartbeat |

### 41.7 Security

- The satellite is trusted like a node: it carries put tokens and sees the
  bytes. What it may do on the cluster API is its own heartbeat and
  certificate, `Route`, and reading the key set; nothing of any tenant's
  rows (`server/core/gatepolicy.go`). The producers it serves come in its
  heartbeat's answer.
- Under `retain: accepted` it also records itself as a lamina's holder
  (`SatelliteService.Accept`) and makes four of the producer's calls on the
  lamina: `Renew`, `ReportAttempt`, `Reallocate` and `ReportFailure`
  ([§41.9](#419-retain-accepted)). The CP takes them only for a lamina whose
  set's producer is assigned to that satellite at the time of the call, and
  makes them in that producer's name: the tokens they sign are issued to
  the producer, for its tenant, and what they write says it was the
  satellite's call. It reads no tenant's rows and allocates nothing.
- A put token is a bearer secret for one key on one node for a few minutes
  ([§33.2](10-security.md#332-access-tokens)). A satellite never stores one,
  never logs one, and only sends one on to the node it names. The handoff
  of `retain: accepted` carries every candidate's token, and each is
  checked as the request's own is.
- The proxy is closed three ways: who calls (an adopted producer assigned to
  it, by mTLS), what for (a CP-signed put token for a key, issued to that
  producer), and where to (the token's node, at the CP's address for it). A
  stolen producer certificate without a token, or a token without the
  producer's certificate, buys nothing.
- Key rotation waits for every live satellite, as for every live node, to
  report the new key before the CP signs with it
  ([§33.3](10-security.md#333-signing-keys-and-rotation)), and a CA rollover
  waits for satellites as for every host ([§33.5](10-security.md#335-tls)).
- A compromised satellite can read and alter what passes through it, and
  write with the tokens it sees, which name keys the CP allocated: no more
  than a compromised node holding the same uploads. Erase it and adopt the
  machine again.

### 41.8 Not yet

- **Live fan-out.** The satellite as the one continuous feeder of a source's
  relay, from the upload stream, so the producer's uplink carries each byte
  once. Its seam: the producers in the heartbeat's answer are where each
  producer's relay assignment is to come, and the relay's ingest does not
  change.
- **A durable spill** for `retain: accepted`: the satellite writing what it
  accepted to disk under a backlog. Lamina data stays off any spool
  ([§22.3](07-storage-node.md#223-no-ssd-in-the-data-path)); that would be a
  decision of its own.
- A satellite in the process of `shale serve all`, where both its hops are
  loopback; HTTP/3 to the satellite; reads through a satellite for viewers
  who reach it but not the nodes.

### 41.9 `retain: accepted`

Store and forward. Under `retain: accepted` a producer hands each segment
to its satellite, which takes the bytes into its RAM and delivers them to
the allocated node itself; the producer frees them as soon as the
satellite says it holds them. `retain` says whose acknowledgement releases
a producer's bytes:

| `retain` | Released on | Producer RAM | If the satellite dies |
|---|---|---|---|
| `committed` (default) | the real node's `201`, passed back through the satellite | as without a satellite | nothing is lost; the producer sends again, through another satellite or directly |
| `written` | the real node's reported offsets, passed back | as without a satellite | as without a satellite under `written` |
| `accepted` | the satellite holding the bytes in its RAM | about a request's part (1 MiB) of a live segment | what it accepted and had not delivered is lost, **for every producer it serves**, and counted as the satellite's |

`accepted` is opt-in per producer (`producer.retain`), and a satellite may
refuse it (`satellite.accept: false`). The `SatelliteAssignment` says
whether the assigned satellite takes it, as its last heartbeat did
(`accept`). A producer under it with no satellite assigned, on
`upload_via: direct`, or assigned one that does not take it uploads as
under `committed`, and its log says so once. A satellite of an earlier
release says nothing, so it is sent no handoff, which it would pass on to a
node with every candidate's token. The satellite never writes what it
accepted to disk ([§41.8](#418-not-yet)).

**The handoff.** The protocol is a node's ([§12.2](04-write-path.md#122-resumable-part-uploads)),
with four headers a node never sends nor is sent:

```text
PUT  /laminae/<first candidate's key>   (its token, as always)
     Shale-Retain: accepted
     Shale-Handoff: <Handoff, base64url>          (while the satellite holds nothing)
     Upload-Offset: o   Upload-Complete: ?0 | ?1   body: from o
  → 204  Shale-Accepted: n   the satellite holds bytes below n; more to come
  → 202  Shale-Accepted: n   it holds the whole segment: handed off
  → 409  Shale-Accepted: n   it stands at n: from there
  → 503  Retry-After          no room for more now ([below](#memory-limits))
  → the node's own answer, Shale-Pass: <why>   it did not take the segment
HEAD /laminae/<key>   Shale-Retain: accepted
  → 200  Shale-Accepted: n, Upload-Complete: ?1 when it holds all of it
```

- A live segment goes as under `written`: requests of about 1 MiB with
  `Upload-Complete: ?0`, each answered with what the satellite holds, which
  the producer frees; then the completing request once the segment
  closed. The satellite holds the bytes exactly, so there
  is no 4 KiB rounding.
- The **Handoff** is the whole allocation, every candidate with its token,
  the lamina, key, `max_length`, `date_started` and capture incarnation, and
  the producer's `resume_timeout`, the link's `abandon_timeout`, its
  `retry_after_cap` and `placement_retries`. The satellite checks every
  candidate's token as it checks a request's (§41.3): a put token its node
  would take, issued to the calling producer, for this lamina, attempt and
  capture; and the request's own key is one of them. One that fails is
  refused `403 Shale-Satellite: token` before anything is held.
- Before it answers that it holds anything, the satellite has the CP record
  it as the lamina's holder (`SatelliteService.Accept`, which the CP takes
  for a PENDING lamina of a producer assigned to it). The lamina's row keeps
  the satellite. A CP that cannot be asked means the segment passes through.
- **`202` is a new outcome** beside stored, lost and cut: the segment is
  **handed off**. It leaves the producer's queue and its RAM, it is not the
  producer's to store or to report any more, and the producer counts it
  (`handed_off`, `shale.producer.segments{outcome}`).
- A request that breaks is resumed as any other: `HEAD` at the satellite
  says what it holds, and the producer goes on from there. A satellite that
  holds less than the producer already freed, that cannot be reached for
  `resume_timeout`, or that refuses once it holds the start, ends the
  segment there: **cut short** at what the satellite holds, as `written`
  cuts at a node's offset. While it holds nothing, the producer goes on as
  under `committed`, wherever `upload_via` sends it.

**Delivery.** The satellite delivers with the producer's own upload client
(`internal/upload`), so it fails over as the producer would have
([§13](04-write-path.md#13-retry-and-reallocation)):

- the candidates in order, from the first byte as they arrive, a live
  segment as one live request; `HEAD` and resume on a broken request,
  `Retry-After` on `503`, the next candidate after `resume_timeout` without
  progress, or at once on a refusal or `413`;
- `ReportAttempt` before it moves on, and `Reallocate` when the candidates
  run out; `Renew` while a delivery outlives two thirds of a token;
- rounds that fail are tried again after a backoff (1 s, doubling to
  30 s), until `abandon_timeout` passes without a byte reaching any node.
  Then the segment is **given up** and reported LOST (`ReportFailure`), as
  a producer reports one it gives up ([§13](04-write-path.md#13-retry-and-reallocation)).
- The satellite holds the whole segment until a node's `201`, never a part
  of it: a node that fails mid-delivery is followed by the next candidate
  from the start, as under `committed`.
- A producer that stops sending before its segment ended (it died, or lost
  the satellite) is waited for `resume_timeout`, after which the producer
  gives the segment up itself. Then the satellite ends its request to the
  node, whose abandon rule makes an incomplete lamina of what it has
  ([§15](04-write-path.md#15-partial-laminae)); what no node took is
  reported LOST. What the producer was never told was held stays the
  producer's, and goes with nothing said.

**Delegated calls.** `Renew`, `ReportAttempt`, `Reallocate` and
`ReportFailure` are the producer's ([§35.4](12-api.md#354-tenant-api-custom-rpcs)). A
satellite makes them on the cluster API, and the CP takes them only for a
lamina whose set's producer is **assigned to that satellite now**; for any
other it answers `PERMISSION_DENIED`. They are made in that producer's name:
the tokens are issued to the producer for its tenant, a node's failure
score counts against the producer's daily cap ([§27](09-operations.md#27-node--device--sink-health-and-quarantine)),
and the attempt's reason says whose call it was: `satellite <alias>, for
its producer: <reason>`.

#### Memory limits

The **retained pool** is `satellite.retained_budget` of the one memory
budget ([§41.4](#414-memory)).

- **A segment reserves** what it is expected to hold when it is taken: its
  `Upload-Length`, else its size hint, at least a part (1 MiB), at most
  `max_length`. It grows past that as its bytes come.
- **A producer's share** is the pool over the producers using it, those
  that hold bytes or asked for acceptance in the last two minutes, capped
  by `satellite.retained_per_producer`. A new segment is taken only within
  its producer's share and the pool, so a producer stuck behind a failing
  node fills its share and no more, and the others still get theirs. A
  segment already taken grows within the pool alone: shares move as
  producers come and go, and a segment half taken is not refused for that.
- **Full, it never evicts.** Bytes leave the pool when their segment is
  delivered or given up, and in no other way.
  - A new segment that does not fit the pool or the producer's share is
    **passed through**: the request goes on to the node as under
    `committed`, the node's answer comes back with `Shale-Pass: full` (or
    `share`), and the producer keeps its bytes and goes on with the node.
    Nothing waits, and the segment is no worse off than without
    `accepted`.
  - A segment the satellite holds the start of, and has no room to grow, is
    answered with what it holds (`204`) and `Retry-After`, and its next
    requests `503 Retry-After` (`Shale-Satellite: busy`) until there is
    room: only the satellite can deliver its start, so its rest cannot pass
    through. The producer keeps the rest meanwhile, as under `committed`,
    and cuts the segment at what the satellite holds if no room comes
    within `resume_timeout`. The reservation makes this rare.
  - A satellite on `satellite.accept: false` is handed nothing, its
    assignment saying so; a request that asks it all the same, sent before
    the producer heard, passes through (`Shale-Pass: refused`), and so does
    one the CP would not record it the holder of (`unrecorded`).
- **What it reports.** Every heartbeat carries the retained pool's use and
  bound, each producer's part of it and its share, the laminae it holds,
  the bytes it accepted, and the segments it delivered, gave up and passed
  through (`SatelliteStatus`); the same are `shale.satellite.retained_*`,
  `held`, `accepted_bytes`, `delivered`, `given_up` and `passed`. A
  producer reports its handoffs, their bytes and the requests passed
  through (`UploadReport`: `handed_off`, `handed_off_bytes`, `passed`), and
  as `shale.producer.handed_off_bytes` and `accept_passed`.

Sizing: under `accepted` the satellite holds what the producers would
have, each source's segment in progress until its node commits it, about
`sources × max_bitrate × segment_duration × 1.2`, and an outage's backlog
on top. Under a long node outage a full pool passes segments through, and
the producers' own `buffer` takes over.

#### Loss

What a satellite accepted and had not delivered is in its RAM alone. If it
dies, it is lost, for every producer it serves, and the loss is the
satellite's:

| How | Those laminae |
|---|---|
| The satellite stops | it reports each one LOST before it exits (`ReportFailure`, in its producers' names), for a few seconds; what it could not report, the CP finds as below |
| It dies, and comes back | every heartbeat carries an **incarnation**, drawn when the process starts. One with another incarnation than the last is a satellite that restarted: every PENDING lamina recorded as held by it that the new process does not say it holds is made LOST at once |
| It dies, and does not come back | once it is down (unheard for `satellite_down_after`, erased, or no longer adopted) and a lamina it held has no attempt left whose token is good, the leader's jobs make it LOST, within `allocation_ttl` and a minute. A satellite only cut off from the CP delivers with the tokens it has until then |
| It gives up a delivery | `ReportFailure`, as above |

Either way the lamina's open attempts fail with a reason that names the
satellite (`satellite <alias> lost what it accepted: it restarted`, or
`satellite <alias>, for its producer: …` when it reports one itself), the
row keeps the satellite as the lamina's holder, and
`shale.cp.laminae_lost` counts it `by="satellite"`; a producer's own
give-up is counted `by="producer"`. None is left PENDING: these run before
the rule that removes a lamina nobody stored ([§12.1](04-write-path.md#121-flow)).
A LOST lamina a node turns out to have after all, the start of a live
delivery that the node's abandon rule finalized, is brought back by its
`LaminaStored`, as any late event ([§14](04-write-path.md#14-duplicates-and-orphans)).
The producer counts none of these as its own losses: it handed them off.
