# Shale — Console

## 40. Console

The console is the page an operator works from: what waits to be adopted,
which disks are quarantined, what every camera is doing, the segments as
they arrive, and the cameras live. It does nothing the CLI cannot
([§32](09-operations.md#32-cli--processes)); it is the same calls with the
rows in view, so the decision of [§33.4](10-security.md#334-joining-and-adoption)
— *is this host mine?* — is made with the hostname, the hardware identity
and the key fingerprint on the screen.

```text
ts/                        the console: Vite + React + payday's client layer
  gen/                     generated from proto/ by `pd gen --ts`  (do not edit)
  src/                     the pages
  vendor/                  payday's TypeScript package, from the commit go.mod pins
wasm/main.go               the sandbox: the control plane compiled into the page
internal/sandbox/          the hosts the sandbox plays
```

### 40.1 Two surfaces, two sign-ins

Shale has two APIs ([§35](12-api.md#35-api)): the **tenant** API, where a
tenant's admin sees its sets, cameras, producers and laminae, and the
**cluster** API, where an operator sees nodes, devices, sinks and relays.
The console signs in to each on its own, and a page that needs one it does
not have asks for it. Against a real server a sign-in is `POST /session`
with the person's tenant, alias and password, answered with a cookie the
script cannot read (payday's `auth/authsession`); every call after that
carries the cookie. Both listeners need `http.allow_web: true`, and a page
served from anywhere but the listener itself needs its origin in
`http.origins` ([§36.1](13-configuration.md#361-configuration-reference)).

| page | surface | what it shows, and does |
|---|---|---|
| Cameras | tenant | every set, its producer's last heartbeat (up/down, CPU, temperature, uplink), and per camera: recording / dark / input down, the measured rate against the ceiling, frame rate, keyframe interval, restarts and early cuts, the suggestion a starved source earns ([§38.5](15-producer.md#385-choosing-the-ceiling), [§38.10](15-producer.md#3810-dark-scenes)) |
| Segments | tenant | one set's laminae as they are allocated, committed, skipped or lost, and the last hour of each camera as a strip: laminae and gaps with their reasons ([§19](05-read-path.md#19-reader-semantics)) |
| Live | tenant | every camera of a set through the relay: `SetService.Live`, then WHEP against the relay ([§39.4](16-relay.md#394-viewers)), asked again before the tokens lapse |
| Hosts | both | what waits for adoption — nodes and relays for the operator, producers and readers for the tenant — with the identity to check, and `adopt`; a producer is adopted for a set. Then every adopted host with when it was last seen, its status and when its certificate lapses |
| Devices | cluster | the quarantine queue with why and since when, and the operator's answers (`release`, `retire`, `declare dead`, `locate`, each with a reason for the record); then every disk with its score, errors, latencies, SMART and the sinks on it ([§27](09-operations.md#27-node--device--sink-health-and-quarantine)) |

### 40.2 How a page stays current

payday's client layer keeps every row a page drew current on its own: a
read that goes through it is a read the framework knows about, and the
rows it answered with are watched for as long as they are on the screen
(`watch:` in the schema; a producer's heartbeat, a device's score, a
lamina's commit reach the page this way). What a watch cannot carry is a
row that was not there when the stream opened — a host that joined, a
segment that was cut — because a watch names rows rather than a predicate.
Those arrive by reading the list again: the pages that see arrivals read
their lists every five seconds, a page of a few hundred rows, which is
cheap, and the rows in it are watched from then on. The timeline strip is
asked again each minute.

### 40.3 The sandbox

The console is developed and tested against the **sandbox**: the control
plane compiled to WebAssembly and started inside the page, with SQLite in
a Web Worker, calls over a message port instead of HTTP, and the CA and the
key that wraps signing keys made in memory. It is the same server, the same
generated services, the same wall and gate policies from the same schema
(payday's `pd sandbox`, [client.md §2](https://github.com/lesomnus/payday/blob/main/docs/client.md)).
What a control plane has on the other side of a socket is played by
`internal/sandbox`, through the same calls the real hosts make: two storage
nodes with disks (one disk going bad, quarantined within a minute) and a
third node waiting to be adopted, a relay, a producer recording three
cameras — segments allocated, stored by the node of the candidate, one
camera's scene going dark and its segments skipped, once in a long while a
segment lost — and a second producer and a reader waiting. Adopting the
waiting host from the page starts it. The live wall draws each camera on a
canvas: there is no byte of video behind the sandbox's relay, and the wall
is what is being looked at.

```sh
cd ts && npm install
npm run gen               # ts/gen from proto/, needs protoc-gen-es (installed with the rest)
npm run sandbox:build     # ts/public/app.wasm and wasm_exec.js, ~140 MB raw, 11 MB brotli
npm run dev               # then open http://localhost:5173/?sandbox
npm run test:sandbox      # headless Chromium through every page, adopting a host on the way
```

Two things the sandbox settles that a deployment would not have shown:

- **A read inside a transaction reads through the transaction.** SQLite's
  memdb, which the sandbox runs on, lets a writer exclude every reader, so a
  read through the pool from inside a transaction is refused outright;
  PostgreSQL and a file-backed SQLite would have answered it from before
  the transaction began. The core layer's transactions carry a bound client
  and server in the context (`Core.ent(ctx)`, `Core.own(ctx)`), and every
  read in the layer goes through them.
- **Calls take turns.** The sandbox's engine is one thread with no busy
  handler, so a write that meets another connection's transaction fails at
  once; the page's calls and the hosts' are serialized on one lock, which
  is what a busy handler would have done without a thread to wait on.

Fidelity has a boundary: bytes. The sandbox's nodes report sizes and
commit laminae but store nothing, so a page that reads a recording (none
yet) would need a node in the page too, on OPFS; nothing in the console's
contract needs one today.

### 40.4 Serving it

`npm run build` writes `ts/dist/`, a static page, served by anything that
serves files. A deployment that serves it from the control plane's own
HTTP listener is one origin and needs no `origins:`; served elsewhere, the
two listeners name the page's origin. The cluster API's listener is a
different origin from the tenant API's in either case (7402 and 7403), so
the console reaches the cluster API cross-origin and that listener names
the console's origin.
