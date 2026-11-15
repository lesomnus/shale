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
   ([§12.2](04-write-path.md#122-resumable-part-uploads)). A satellite that
   holds bytes for it would free the edge's RAM sooner. Not yet:
   [§41.8](#418-not-yet).

This first version solves (2) and lays the ground for (1) and (3): the
satellite moves bytes and nothing else, so it costs the write path nothing it
has today.

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
same budget ([§41.8](#418-not-yet)).

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
