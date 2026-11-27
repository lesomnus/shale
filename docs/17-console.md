# Shale — Console

## 40. Console

The console is the page an operator works from: what waits to be adopted,
which disks are quarantined, what every camera is doing, the segments as
they arrive, the cameras live, and any camera at any moment
([§40.5](#405-playback)). It does nothing the CLI cannot
([§32](09-operations.md#32-cli--processes)); it is the same calls with the
rows in view, so the decision of [§33.4](10-security.md#334-joining-and-adoption)
— *is this host mine?* — is made with the hostname, the hardware identity
and the key fingerprint on the screen.

```text
ts/                        the console: Vite + React + payday's client layer
  gen/                     generated from proto/ by `pd gen --ts`  (do not edit)
  src/                     the pages
wasm/main.go               the sandbox: the control plane compiled into the page
internal/sandbox/          the hosts the sandbox plays
```

### 40.1 Two surfaces, two sign-ins

Shale has two APIs ([§35](12-api.md#35-api)): the **tenant** API, where a
tenant's admin sees its sets, cameras, producers and laminae, and the
**cluster** API, where an operator sees nodes, devices, sinks and relays.
The console signs in to each on its own, and a page that needs one it does
not have asks for it. It first asks the surface's listener how
(`GET /session/ways`): where the deployment signs people in through its
issuer ([§33.1](10-security.md#331-trust-model), `auth.oidc`), the page
offers **Sign in with SSO**, which sends the browser to `/sso/login` on
that listener with the page as `next`, and the browser comes back signed
in; where it takes a password, the form posts `POST /session` with the
person's tenant, alias and password; where it takes both, both are drawn,
and with `auth.sso_only` only the button is. Either way the answer is a
cookie the script cannot read (payday's `auth/authsession`); every call
after that carries the cookie, and on load the page asks each listener who
its cookie names (`GET /session`) rather than trusting what it remembered,
which is also how a page coming back from the issuer knows it is signed
in. Signing out of a session the issuer made signs out of both surfaces:
the other surface's session ends with `DELETE /session`, and this one goes
the two hops of `/sso/logout`, through the issuer and back to the page. The
operator half is signed in on the cluster listener's own origin
(`ops.shale.hday.dev` beside `shale.hday.dev`): cross-origin, but the same
site, so the cluster cookie still goes with the page's calls. Both listeners need `http.allow_web: true`, and a page
served from anywhere but the listener itself needs its origin in
`http.origins` ([§36.1](13-configuration.md#361-configuration-reference)).
Each surface's cookie has a name of its own (`__Host-shale_tenant`,
`__Host-shale_cluster`): the two listeners are neighbouring ports of one
host, and a browser keeps cookies by host rather than by port, so under one
name the second sign-in would overwrite the first. A session the server no
longer holds, one that idled out or was minted before an upgrade, does not
strand the page: the call carrying it is served as nobody where nobody is
asked for, so the sign-in is reachable, and it is answered with the cookie
that clears the dead one.

What a page keeps of a surface, the rows it drew (kept in the browser, so
a reload draws them at once) and the writes it has not sent yet, is a store
named by what the session lets its caller see: `GET /session` answers that
name (`identity`, payday's `authsession.IdentityOf`), every call the store
makes carries it, and a call carrying it under a cookie that is somebody
else's session is refused as unauthenticated. A page left holding one
person's store after another signed in, in another tab, neither reads their
rows into it nor sends what it kept as them; its writes wait for the first
person to sign in again. Opening a store deletes every other store of its
surface on that browser, and signing out forgets it, and says so on the
sign-in when the browser would not let go of it.

| page | surface | what it shows, and does |
|---|---|---|
| Cameras | tenant | every set, its producer's last heartbeat (up/down, CPU, temperature, uplink), and per camera: recording / dark / input down, the measured rate against the ceiling, frame rate, keyframe interval, restarts and early cuts, the suggestion a starved source earns, with the limit that holds it back and its amounts (upload policy, set, uplink) or, in a set with `auto_raise`, why it is left to a person, and the last raise `auto_raise` applied; and cameras waiting to start, with the producer's reason and its amounts (uplink, RAM, set or upload policy) ([§38.5](15-producer.md#385-choosing-the-ceiling), [§38.10](15-producer.md#3810-dark-scenes)) |
| Segments | tenant | one set's laminae as they are allocated, committed, skipped or lost, and the last hour of each camera as a strip: laminae and gaps with their reasons ([§19](05-read-path.md#19-reader-semantics)) |
| Live | tenant | every camera of a set through the relay: `SetService.Live`, then WHEP against the relay ([§39.4](16-relay.md#394-viewers)), asked again a minute before the tokens lapse, and each camera's session renewed with its fresh token, since the relay ends a session with its token; one the relay will not renew is opened again. Where the browser has encoded transforms and `requestVideoFrameCallback` (Chrome 141, Firefox 117, Safari 15.4), each session is timed: the page reads the relay's timing SEI in a worker, reports every ten seconds how long frames took to reach the screen (`POST /whep/<session>/timing`), and shows under the camera how far behind the producer it is, the median of t6 − t1 ([§39.4](16-relay.md#394-viewers)) |
| Hosts | both | what waits for adoption — nodes and relays for the operator, producers and readers for the tenant — with the identity to check, and `adopt`; a producer is adopted for a set. Then every adopted host with when it was last seen, its status and when its certificate lapses. Somebody who holds only the basics (*Somebody who holds the basics*, below) is told instead that they hold no role of Shale's that reads hosts, what a host's row shows, and what role it takes |
| Devices | cluster | the quarantine queue with why and since when, and the operator's answers (`release`, `retire`, `declare dead`, `locate`, each with a reason for the record); then every disk with its score, errors, latencies, SMART and the sinks on it ([§27](09-operations.md#27-node--device--sink-health-and-quarantine)) |
| Sites | tenant | the sites the signed-in person sees, each with its name and alias, how many sets are in it, who sees it -- *Its members*, *The company's people* or *Everybody*, each with one line saying who then sees its cameras ([§33.1](10-security.md#331-trust-model)) -- and its members on demand, a person by `@tenant/alias` and a reader by its alias where the person may read them. For somebody who may write a site (what they hold at Shale, as *Tokens* reads it, or anybody where roster runs inside Shale and names no operators): the three kinds to choose from and *save*, whose confirmation says who then sees it and that it is so within ten seconds; *remove* on each member; and *add* by the alias a person signs in with, typed in, since a person reads no list of people without a grant to -- one Shale has not seen sign in is not found, and the page says so. Somebody who may not is shown the list and no controls, and told that they hold no role of Shale's that writes a site or its members and to ask an administrator. A camera in no site is in none of the sites listed; nobody but whoever sees every site sees it. **Placing a set** ([§7](02-data-model.md#7-source-set-zone-epoch)): the sets in no site, which only whoever sees every site sees, under their own heading with one line saying so; each site's sets on its row, on demand; and for somebody who holds `SetService/Place` (or anybody where roster runs inside Shale and names no operators), beside each set a choice of the other sites, each with its kind, and *place* or *move*. It asks with a dry run first, and its confirmation says who will then see the set (the site's kind, in the words above), how many recordings move with it and its cameras, upload attempts and producer, who stops seeing it, and that it is so within ten seconds; for a set whose cameras Shale manages, it says its camera network moves with it and that its producer is to be restarted after the move; where the dry run says the placing would be refused, it says why instead of asking; then it moves it and says how many rows moved. The server alone decides who may: somebody who does not see every site is told its refusal in its words |
| Tokens | tenant | the signed-in person's own API tokens for scripts, keys for Shale at roster ([§33.1](10-security.md#331-trust-model)): each with its name, its cameras (*all*, or its list's name with what it names on demand, or that its list is gone and it reaches nothing), what it may do (*View and read* or *Everything I may do here* where it names exactly that, otherwise how many methods, listed on demand), when it ends or *never*, when it was last used and made, and *end*, whose confirmation says scripts using it stop within `auth.operators.ttl`. *Make a token*: a name in roster's spelling of one; *which cameras* -- *All I can see*, or *A list of cameras*: one of the person's lists, or a new one made there, a name and the sites and cameras they see, shown by site with each camera's name ([§33.1](10-security.md#331-trust-model), *A key narrowed to some cameras*); then what it may do is offered only of what a list confines, and the page says why: one that could change things, or read anything else, would not be narrowed; what it may do -- *View and read* (the console's reads the person holds, `Get` beside every `Watch`: for somebody who holds the basics alone, the eighteen, said as cameras, live, recordings and their export, and sites), *Everything I may do here*, or *Choose*, by service -- offering only what the person holds at Shale, and saying where they hold nothing that a token can do only that, so to ask an administrator for a role of Shale's -- or, where roster runs inside Shale and `auth.operators` is unset, that nobody can hold one and what an operator sets; and when it ends: in 30 days, 90 days (the default), a year, on a date, or never. Where roster is elsewhere *make* opens the identity service's page to say yes in a new tab, shows its address and code, and asks every five seconds; a no, a code that lapsed and a refusal are each said, with what to do -- roster's refusal after a yes at once, in roster's sentence. The token is then shown once, with *copy* and the line a script writes (`export SHALE_CLIENT_TOKEN=…`), and kept nowhere: not in the address, the browser's storage or a log. Below, *your lists of cameras*: each with its name, what it names, the tokens that carry it, *change* (the confirmation says which tokens it changes and that they follow within `auth.operators.ttl`) and *erase* (it says which tokens will then reach nothing). `test/tokens.mjs` drives it against a deployment, a token of a list of one camera included |

**Somebody who holds the basics** -- every person, where `auth.operators`
is set and its reads are everybody's, and nothing else bound to them
([§33.1](10-security.md#331-trust-model)) -- reads every page about
cameras: *Cameras* (the sets and their cameras, and in place of each
producer's heartbeat one line that it is not shown and what reading hosts
takes; a camera's state is *state not shown* rather than *producer
down*), *Segments*, *Live*, *Playback* and *Sites*, each within the sites
they see; and *Tokens*, which offers them the eighteen methods of the
basics, and of them what a list of cameras confines. *Hosts* says what it
takes to read hosts instead of a list. Nothing on these pages breaks for
a refusal of the hosts' reads.

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
npm run sandbox:build     # ts/public/app.wasm and wasm_exec.js, ~114 MB raw, 14 MB brotli
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
commit laminae but store nothing, so the Playback page ([§40.5](#405-playback))
shows the axis in the sandbox and plays nothing; a node in the page, on
OPFS, would be what it takes, and nothing needs one today.

### 40.4 Serving it

The control plane serves the console itself, at the root of the tenant
API's HTTP listener: `https://<cp>:7402/`. `npm run build` in `ts/` writes
the page into `web/console/dist`, which the binary embeds; the Dockerfile
and `deploy/deb/build.sh` do that before `go build`, so the image and the
package carry it, and a binary built without it (a checkout with no node)
answers that path with a line saying so. The page opened this way is the
listener's own origin, so the tenant API needs only `allow_web: true`.

The cluster API's listener is another origin (7403 beside 7402, or 30403
beside 30402 on a NodePort), so the console reaches it cross-origin and
that listener names the console's origin in `origins:` — every name the
page is opened by. The two must be the same *site* (the same host, or one
registrable domain): the cookie is `SameSite=Lax`, which a browser sends
across ports but not across sites. Where the cluster listener is, the page asks the tenant listener first:
`GET /session/ways` names it in `cluster` when the deployment says
(`auth.oidc.cluster_origin`), and the port beside is only the guess for a
server that names nothing. Behind an Ingress the two listeners are two names
on 443, and the guess -- the tenant listener's host on 444 -- is a port
nothing answers on: a call that hangs until it times out. A cluster API reachable only from
inside the network keeps the operator half of the console inside too; the
tenant half works on its own, and the pages that need the other say so.

Development mode (`--dev <dir>`) writes all of that down for itself: both
listeners answer what a page speaks, and each names loopback at the
console's port and at vite's, by both spellings, since a browser tells
`localhost` from `127.0.0.1`. It is already plaintext on loopback, and a
console that loads and then cannot call anything is not a mode worth
having.

`npm run build:sandbox` is the page with the sandbox in it, `ts/dist/`, for
a static host: a demo that needs no server at all.

### 40.5 Playback

Playback is one camera at any moment: a time axis with what is stored and
what is missing (the strip of [§19](05-read-path.md#19-reader-semantics)),
a playhead, and a video that plays what the axis points at. Everything it
plays is fragmented MP4 as Shale stores it ([§38.1](15-producer.md#381-inputs)),
which the browser's Media Source Extensions take as it is: nothing is
transmuxed, and the page holds no player library.

- **A moment in the store.** `Timeline` ([§17](05-read-path.md#17-read-path))
  names the lamina covering it and its URL, with the token in the URL. The
  page reads the lamina's last 16 bytes (`mfro`), then its index (`mfra`:
  every keyframe's decode time and byte offset,
  [§23.1](07-storage-node.md#231-self-describing-laminae)), then the init
  segment, and appends the init segment and a Range from the keyframe at
  or before the moment; fragments stream in a megabyte at a time, twenty
  seconds ahead of the playhead, and the next lamina follows with its own
  init segment. A lamina's first key fragment came at its `date_started`,
  and that is where its decode times sit on the wall clock
  (`timestampOffset`); the axis is the wall clock, so a lamina from
  another encoder session, whose decode times start again, lands where it
  belongs. A gap is shown as one and skipped to the next recording.
- **The last minutes.** Past the last committed lamina the page asks the
  relay's `recent_url` ([§39.4](16-relay.md#394-viewers)) for the window
  since the moment, which is the same shape with the init segment first,
  placed by `Shale-Recent-Start`; when the playhead reaches the window's
  end the page hands the element to WHEP, and the camera is live. "10 s
  ago" is that, for the last ten seconds.
- **Export.** Two marks on the axis become one MP4: one init segment and
  the fragments from the keyframe at or before the first mark to the
  keyframe after the second, across laminae. Where exports are enabled
  (`export.enabled`), the page asks `LaminaService.Export` for the range
  and the browser downloads the file the Control Plane streams, which
  starts at once and holds none of it in the page
  ([Streaming MP4 downloads](mp4-export.md)); a range the export refuses
  fails as a download. Elsewhere the page joins the same file itself and
  downloads it. What `timestampOffset` does in play is written into the
  file: every fragment's `tfdt` is moved to where its lamina sits on the
  wall clock, the first sample at zero, so a lamina of another encoder
  session, whose decode times start again, follows the one before it
  instead of overlapping it. A lamina that goes on from the one before it
  in the same session keeps the session's own decode times, which are
  exact where `date_started` is not. A gap stays a gap in the file, a jump
  in `tfdt` that costs no bytes, and a player that reads `tfdt` shows it
  as one. The file says when it begins (`mvhd`'s creation time, the first
  sample's) and how long it is (`mehd` and the track durations, which an
  `empty_moov` leaves out). A range across a change of the encoder's
  configuration (another codec, profile or picture size) would need two
  init segments in one track, which is not a portable MP4, so the export
  refuses it and says where the change is: the two sides export apart.
  The media comes in one Range request per lamina, its whole span, which
  the node streams in its read chunks; playback instead asks a megabyte at
  a time ahead of the playhead.
- An export the page joins measures where its time went and says so under the button:
  reading the laminae's index and init segments, then the media requests
  one after another, summed as waiting for each response's first byte,
  receiving its body, and the page's own time between them, then joining
  the fragments into the file. It posts the same numbers to the node that
  served the first lamina (`POST /reports/export`, that lamina's read
  token), which records them as `shale.node.export_*`, beside its own side
  of every read: waiting for the device, reading it, and writing to the
  client ([§31](09-operations.md#31-observability)). A lost report loses
  only that sample.
- The Storage Node's data plane answers a browser's `OPTIONS` and marks
  its `GET` for any origin, since the token is in the URL and a Range is
  what a player asks for ([§17](05-read-path.md#17-read-path)).
- A browser trusts the public CAs, not the shale CA, so for the page to
  read a lamina at all the data plane serves a certificate from one
  (`storage.cert_file`, `storage.key_file`) and the node names itself as
  that certificate does (`storage.advertise: storage.example.com:7410`),
  which `Timeline` hands out as it is
  ([§34.10](11-deployment.md#3410-node-addresses)). It is the listener and
  the address producers upload to: they trust the public CAs as well as
  the shale CA ([§33.5](10-security.md#335-tls)). The relay's WHEP
  listener needs the same for the last minutes and live
  ([§39.4](16-relay.md#394-viewers)).

`test/playback.mjs` drives it in headless Chromium against a deployment:
two minutes back plays, ten seconds ago comes from the relay, live takes
over, and a range downloads as an MP4 ffprobe reads whole.
`test/export.mjs` joins recordings ffmpeg makes on the spot, with no
deployment: an encoder restart, a gap, one session cut in two with a late
`date_started`, a range from the middle, and a changed picture size; ffprobe
reads the decode times and Chromium plays the files through.
