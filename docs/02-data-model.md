# Shale — Data Model

## 7. Source, Set, Zone, Epoch

A **Tenant** owns sets, sources, laminae, and the people and hosts that act
on them. It is payday's tenant, and the wall around it is always on
([§33.1](10-security.md#331-trust-model)). A single organization runs a
cluster with exactly one tenant, created by `shale init`, and never has to
name it: slugs leave it out and everything belongs to it. Adding tenants
later needs no change to code, schema, or clients. Storage infrastructure
(nodes, devices, sinks) is not owned by any tenant.

A **Source** is what produces laminae: one camera, or more generally one
stream of data. Its `content_type` says what the bytes of its laminae are
to whoever reads them: `video/mp4` for a camera, which an empty value
means, or what a pushed stream of records declares
([§38.9](15-producer.md#389-pushed-sources-and-raw-frames)). Sets and
sources carry `labels`, payday's field 7, for whatever an operator groups
them by: a robot, a fleet, a building.

A **Set** is a group of Sources behind one producer. Every Source belongs to
exactly one Set; a single camera is a set of one. Its members:

- are **fate-shared**: they power on and off together, and a producer failure
  stops them all;
- are **read together**: a reader usually wants every member for the same
  time span;
- are registered and removed as a unit.

Each member has a stable, never-reused **ordinal** within its set (0, 1, 2, …),
assigned at registration. Placement uses it to spread members apart ([§11](03-placement.md#11-placement)), and
producers use it to stagger segment boundaries ([§12.2](04-write-path.md#122-resumable-part-uploads)).

A **Producer** is the host that receives a set's streams, cuts them into
segments, and uploads them: one machine, one certificate, one set
([§5](01-overview.md#5-components), [§33.4](10-security.md#334-joining-and-adoption)).
It is also assigned a **Relay**, the host that shows its cameras live to
viewers ([§39.2](16-relay.md#392-assignment)). A **Reader** is a host that
queries and reads laminae, typically a media server, and may watch live
too. Producers and readers are rows of their tenant, adopted by an
operator; relays are cluster infrastructure like nodes
([§35.3](12-api.md#353-entities)).

A **Site** groups sets within a tenant, e.g. a building or a branch. It is
payday's second permission axis (field 3): a person or a reader sees the
cameras of the sites it may see and no others, so a guard at one building
cannot read another's cameras ([§33.1](10-security.md#331-trust-model)). A
set belongs to at most one site, said when the set is added and changed
only by placing the set (below). Its sources, laminae, attempts, and
producer carry the same site. A site may also name,
by labels, which relays its producers should use
([§39.2](16-relay.md#392-assignment)).

A site says who sees it, its `visibility`, one of three kinds:

- `MEMBERS`: the people and readers its **SiteMember** rows name. A site
  that says nothing is this, and so is every site from before the field.
- `COMPANY`: the company's people, and those it names. **The company's
  people** are those of the tenant with a proved address at a mail domain
  the tenant names at roster: an address roster stamped as checked (a
  directory's word, or a link they confirmed), whose domain is one of the
  tenant's `MailDomain` rows, exactly. Somebody whose address is not
  proved, or is elsewhere, sees the sites said `EVERYBODY` and the ones
  that name them. Where roster runs inside Shale everybody of the tenant is
  of the company, so `COMPANY` and `EVERYBODY` show the same people: such a
  deployment has no outsiders to tell apart, and its roster holds no
  addresses and no mail domains
  ([§33.1](10-security.md#331-trust-model)).
- `EVERYBODY`: every person signed in to the tenant, and the readers it
  names. A reader is a host and nobody: a kind gives it nothing.

Whoever sees every site sees each of them whatever it says
([§33.1](10-security.md#331-trust-model)). The kind is written when the
site is added and by a patch, by whoever may write a site; a kind there is
none of is refused.

**A row with no site** -- a set added without one, and its sources,
laminae, attempts and producer -- is seen by whoever sees every site and
by nobody else: no kind and no membership reaches it. A tenant that does
not use sites therefore shows its cameras only to whoever sees every site.
To show a camera to others, its set is put in a site.

**Placing a set.** A set's site is said when the set is added, and a set
that exists is put into a site, or moved from one to another, by
`SetService/Place` (`shale set place <set> <site>`, the console's
*Sites* page). In one transaction it rewrites the site of every row that
carries it because of the set, each because the wall reads a row by its
own site:

- the **set**;
- each of its **sources**, by `set`;
- each **lamina** of the set, by `set`, and each **attempt** of those
  laminae, by `lamina`: every recording the set ever made, before the
  move as after;
- each **producer** adopted for the set, by `set`, as adoption writes it.

**A set whose cameras Shale manages** (one with a `CameraAssignment`) is
placed with its camera network and everything of it, in the same
transaction, since every camera operation checks that the network, its
devices, the assignment and its bindings and jobs, the worker, the policy,
the preset and the credentials are of one site with the set, its sources
and its producer. What ties each kind to the set:

| kind | tied by | moves |
|---|---|---|
| `CameraAssignment` | `set` | every one of the set's |
| `CameraBinding` | `assignment` | every one of those assignments' |
| `CameraJob` | `assignment` | every one of those assignments' |
| `CameraNetwork` | named by a live assignment of the set | yes |
| `CameraDevice` | `network` | every device of the network |
| `CameraCredential` | `device` | every credential of those devices |
| `CameraCredentialVersion` | `credential` | every version of those credentials |
| `CameraPolicy` | `network` | every policy of the network |
| `CameraWorker` | named by a live assignment of the set (`worker_id`); its site is its adoption's | yes |
| `CameraPreset` | the site alone; named by a policy of the network (`preset`) or by a job of the set (`inputs.preset`) | one in the site the set leaves, when nothing outside the network names it; one in no site while the set leaves a site is the tenant's, usable from any site, and stays |

What a placing refuses instead, writing nothing, in sentences the dry run
answers too:

- a network another live set is assigned to: it names the sets, which move
  together or not at all, and placing several sets at once is not built;
- a worker another live set is assigned;
- a preset another network's policy or another set's job names (it says
  how many), or whose rules name a site;
- a job of the set under way, since a job checks at every step the scope
  it began with.

In a tenant whose managed sets each have a network of their own and were
all added in no site, the policies are each network's own (a policy names
its network), and so are the credentials (a credential names its device);
a preset and a worker are of no site alone, and each moves with the one
network that names it. That is told by what names each row, never by its
site. A credential's sealed payload is bound to its tenant, device,
binding, credential, purpose and version, not to a site, so rewriting its
row's site is the whole of moving it. The set's producer reads the site
it records in once, when it starts: after a managed set is placed it is
refused its cameras' next credential (each says the site it is delivered
in) until it is restarted ([§32.4](09-operations.md#324-placing-a-set-in-a-site)).
A worker reads its site and its assignments at each call and goes on.

Nothing else carries the set's site because of the set:

- **A key's lists of cameras** (`Reach`) name sites and sources and are not
  rewritten: a list that named one of the set's sources still reaches it;
  a list that named the site it left no longer does, and one that names
  the site it is in now does.
- **A site's members** are the site's, and a reader's sites its members.

Placing it where it is writes nothing. Out of a site into none is not
offered: a set in no site is seen by whoever sees every site alone, and
putting one there is hiding it -- somebody who wants that makes a site of
kind `MEMBERS` with nobody in it and places the set there, which says so.
Only whoever sees every site places a set, even between two sites they
see: a move changes who sees every recording the set ever made
([§33.1](10-security.md#331-trust-model)). What it locks and how long it
takes: [§32.4](09-operations.md#324-placing-a-set-in-a-site).

A **Reach** is a person's own list of cameras: a `name`, the person
(`holder`), and `cameras`, the sites and the sources it names by
identifier. It reaches every source of each site it names and each source
it names; one that names nothing reaches nothing. A person's API token
carries one by its identifier and then reaches, of what its person sees,
only that ([§33.1](10-security.md#331-trust-model), *A key narrowed to
some cameras*). The person adds, reads, changes and erases their own and
nobody else's, and puts in one only sites and sources they see; changing
one changes what every token that carries it reaches, and erasing one
(a hard erase) leaves them reaching nothing. It carries no site of its own
(field 3 is empty), since it may name several.

A **Zone** is an optional label, assigned by people, for Sources whose fields
of view overlap. A zone may cross sets. It describes redundancy between angles,
not fate-sharing. The first scheduler does not use it, and zone-aware placement is
not planned ([§11.2](03-placement.md#112-scheduler-interface)). The label is
stored so a future scheduler could. Shale needs no camera geometry beyond
these labels: the only placement question is which Sources should not share a
failure domain.

An **Epoch** is a fixed time bucket (default 1 hour). The epoch of a lamina is
derived from its `date_started`. During one epoch, all laminae of a Source go to
the same sink as long as that sink stays eligible. When the epoch changes, the
Source moves to another sink.

```text
camera-17
10:00–10:59 → sink S-0412
11:00–11:59 → sink S-0087
12:00–12:59 → sink S-0339
```

## 8. State Model

Two words for the same bytes, on purpose. A **segment** is what a producer
makes: the bytes of one source between two cuts and the time span they
cover, and nothing else. It has no state; the producer keeps it until a
node has it, then forgets it ([§38.2](15-producer.md#382-cutting-segments)).
A **lamina** is what the cluster keeps about a segment: a row that exists
from the moment it is allocated, before the segment is cut, through the
attempts to store it, to `STORED`, `LOST` or `DELETED`, with its dates and
its place on a sink. A complete upload makes the lamina's bytes the
segment's; a cut-short one makes them a prefix
([§15](04-write-path.md#15-partial-laminae)). One segment, one lamina, and
the lamina outlives the producer's memory of the segment. The word is the
geologist's: shale splits along its laminae, and a recording splits along
its laminae, each one playable on its own.

Lamina state and attempt state are separate machines.

### Lamina

```text
PENDING ──► COMMITTED ──► DELETING ──► DELETED
   │            │  ▲
   │            └──┼──► LOST     (file found missing on read / device declared dead)
   ├──► LOST ──────┘             (producer gave up; a late LaminaStored still
   │                              brings the lamina back, §14)
   ├──► SKIPPED                 (the producer stored nothing on purpose: the
   │                              scene was dark, §38.10; the row keeps the span)
   └──► (removed)                (no attempt stored anything within its TTL
                                  plus abandon_grace; recreated by a late event)
```

`DELETED` is also reached without any state change: a lamina whose
`date_deleted` has passed is deleted, whatever its stored state says
([§20.1](06-retention-gc.md#201-date_deleted-is-an-expiry-not-an-event)).
`DELETING` already reads as `DELETED` ([§19](05-read-path.md#19-reader-semantics)).

A lamina whose allocations were never used is removed. It reads as
`NOT_RECEIVED`, not `LOST`, because Shale was never given any data ([§19](05-read-path.md#19-reader-semantics)).
Rows of deleted and lost laminae are pruned after a while
([§20.4](06-retention-gc.md#204-row-retention)).

A committed lamina may carry the `incomplete` flag ([§15](04-write-path.md#15-partial-laminae)). It is still
`COMMITTED`; the flag only says its tail is missing.

`UNAVAILABLE` is not stored. It is derived at read time from the health of the
lamina's sink, device, and node.

### Write attempt

```text
ALLOCATED ──► STORED
    │            ▲
    ├──► FAILED ─┤    (reported by the producer, with failure_reason)
    ├──► ABANDONED    (no event within allocation_ttl)
    └──► DUPLICATE    (stored, but another attempt won)
```

`FAILED` and `ABANDONED` are the CP's guesses. A `LaminaStored` that arrives
later moves the attempt to `STORED` or `DUPLICATE`, because the node's word
about what is on its device is final ([§14](04-write-path.md#14-duplicates-and-orphans)).

```text
write_attempts
  attempt_id
  lamina_id
  sink_id
  node_id          (the node the sink was on when the attempt was allocated)
  state
  failure_reason
  date_created
  date_finished
```

## 9. Identity

Every entity's ID is payday's UUIDv8: time-ordered, with a byte naming the
entity kind ([§35.1](12-api.md#351-conventions)). People name rows by slug,
e.g. `cam-03#source`, or `@acme/cam-03#source` when there are several tenants.

### Host

A Storage Node, a Producer, and a Reader are hosts. Each is identified by a
**hardware identity** that survives a reinstall: the DMI product UUID, or the
device-tree serial number on boards without DMI
([§33.4](10-security.md#334-joining-and-adoption)). The row's ID (`node_id`
for a node) is assigned at adoption and never changes; the hardware identity
is how a machine that lost its state finds its row again.

### Node

`node_id` is independent of hostname and IP.

### Device

`device_id` comes from the hardware (WWN, or serial). For volumes Shale cannot
see through, it is the filesystem UUID, or the pool GUID for a ZFS dataset
([§22.2](07-storage-node.md#222-sinks-and-devices)). It is independent of
`/dev/sdX`.

### Sink

`sink_id` is assigned when the sink is created and stored in its label
(`.shale-sink`), together with the device identity seen at that time. A sink
whose label and current device disagree is flagged rather than trusted (e.g.
a directory copied onto another device). A sink is attached to one node at a
time; moving it is an adoption ([§28.3](09-operations.md#283-node-failure-and-device-re-homing)).

### Logical slot

The physical bay is separate:

```text
logical_slot = chassis-01/bay-17
old HDD:      device_id = WWN-A, sink_id = S123
replacement:  device_id = WWN-B, sink_id = S987
```

A replacement HDD gets a new sink and never reuses the old `sink_id`.

## 10. Time Semantics

- Stored times are UTC.
- **System times** are stamped by Shale: `date_created` (allocation, CP clock),
  `date_committed` (commit, node clock, carried in the `LaminaStored` event),
  `date_finished` (attempt end).
- **Data times** are `date_started` and `date_ended`: the time span the
  lamina's data covers, **declared by the producer** in the upload's headers
  ([§12.2](04-write-path.md#122-resumable-part-uploads)). Shale does not
  interpret them. It indexes them for time-range queries and derives the
  epoch from `date_started`. A producer that declares nothing gets the write
  times instead (allocation and commit).
- The two differ whenever an upload is not live. A buffered segment arrives a
  segment-length late, and a backlog after a link outage can arrive hours
  late. A pre-allocated lamina's `date_created` even precedes its data. Data
  times keep time-range queries and epochs correct in all these cases, with no
  knowledge of what the data is.
- Clocks are NTP-synchronized. The CP accepts a `date_started` up to
  `allocation_horizon + clock_tolerance` (5 minutes) in the future, which
  covers pre-allocation and a producer's clock error, and as far in the past
  as `max_backlog_age` (default: the set's `retention.expire`), which covers
  any backlog worth storing. Anything else is refused with an error, never
  clamped ([§12.1](04-write-path.md#121-flow)).
