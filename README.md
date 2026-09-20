# Shale

Shale is a distributed object store for **continuous CCTV recordings** and
similar large, immutable, loss-tolerant data. It keeps exactly one copy of each
object: no replicas, no erasure coding, no RAID. It spends nearly all raw HDD
capacity on data, keeps HDD I/O sequential, and treats a lost object as a gap
in the recording rather than a failure. Shale does not repair failures. It
isolates them, and it uses source, set, and time information to shape what a
failure takes away.

Around the store, Shale ships the two ends a CCTV deployment needs: a
**producer** that turns cameras into objects, and a **relay** that shows a
camera live to viewers over WebRTC without touching the store.

This file is the entry point to the design. The design itself lives in
[`docs/`](docs/).

## Reading order

Read top to bottom for the full picture. Each document stands on its own once
you know the overview and data model.

| # | Document | Sections | What it covers |
|---|---|---|---|
| 1 | [Overview](docs/01-overview.md) | §1–§6 | Target workload (CCTV), design principles, architecture, the roles of Control Plane, Storage Node, Producer, and Reader, and the project's philosophy. **Start here.** |
| 2 | [Data Model](docs/02-data-model.md) | §7–§10 | Tenants, sites, sources, sets, producers, readers, zones, and epochs; object and write-attempt state machines; host/node/device/sink identity; time semantics. |
| 3 | [Placement](docs/03-placement.md) | §11 | How a new object is assigned to a sink: weighted rendezvous hashing over all nodes with eligibility as fallback, set spread across nodes and devices, and `epoch` as the loss-shape knob. |
| 4 | [Write Path](docs/04-write-path.md) | §12–§16 | Allocation and pre-allocation horizon, live and buffered resumable uploads, commit semantics, retries, duplicates and orphans, partial objects, and producer backpressure. |
| 5 | [Read Path](docs/05-read-path.md) | §17–§19 | Paged set and time-range queries, read chunks and prefetch, reading while GC deletes, and the states and gap reasons readers see. |
| 6 | [Retention and GC](docs/06-retention-gc.md) | §20–§21 | `date_expired` and `date_deleted` (read like a certificate expiry), rescheduling instead of holds, the daily sweep, row retention; watermark-driven lazy GC with node-proposed, CP-approved deletions and tenant fair share. |
| 7 | [Storage Node](docs/07-storage-node.md) | §22–§25 | HDD layout and filesystem requirements, sinks and devices, no SSD in the data path, `O_DIRECT`, self-describing storage format, the starvation-free Device Queue, and object size. |
| 8 | [Sizing and Hardware](docs/08-sizing.md) | §26 | NIC, HDD-bandwidth, capacity × retention, and RAM ceilings per node, and which hardware to add when each one binds. |
| 9 | [Operations](docs/09-operations.md) | §27–§32 | Health and quarantine, failure semantics and sink adoption, metadata index rebuild, integrity, observability, and CLI. |
| 10 | [Security](docs/10-security.md) | §33 | People and hosts, the tenant wall, CP-signed access tokens, key storage and rotation, joining and adoption of hosts, the built-in CA and its rollover, and what each compromise costs. |
| 11 | [Deployment](docs/11-deployment.md) | §34 | One binary, one command per role; Kubernetes, Ubuntu with systemd, a single machine, and containers; events from nodes and directives to nodes without a queue; node addressing. |
| 12 | [API](docs/12-api.md) | §35 | payday conventions, the two gRPC surfaces, entities and their custom RPCs, the HTTP/QUIC data plane, and the node control API. |
| 13 | [Configuration](docs/13-configuration.md) | §36 | Every tunable with its default, bounds, and who sets it; open decisions; rejected alternatives. |
| 14 | [Glossary](docs/14-glossary.md) | §37 | Definitions of all terms. |
| 15 | [Producer](docs/15-producer.md) | §38 | The producer program: inputs and the TS contract, cutting at keyframes, managed capture with ffmpeg and its three tiers of tuning, camera discovery and registration, choosing the bitrate ceiling as a feedback loop, heartbeats, live output. The client side Shale ships; the storage design does not depend on it. |
| 16 | [Relay](docs/16-relay.md) | §39 | Live viewing: a stateless host that takes a producer's streams on demand and serves viewers over WebRTC (WHEP); assignment of producers to relays, tokens, instant start, capacity, failures. Never touches storage. |
| — | [Placement Decisions](docs/placement-decisions.md) | D1–D9 | Decision report behind §11: scenario, requirements, the options considered, and why each one was chosen or rejected. |
| — | [Producer Bench](docs/producer-bench.md) | — | Measurements of a Raspberry Pi 400 recording one camera: encoder quality and bitrate, audio in the same segment, CPU load and temperature. Reference for producer hardware; not part of the storage design. |

## Reading paths by role

| If you are… | Read |
|---|---|
| Evaluating whether Shale fits | Overview → Sizing → Configuration |
| Implementing the Control Plane | Overview → Data Model → Placement (+ decision report) → Write Path → Retention and GC → Security → Deployment → API |
| Implementing the Storage Node | Overview → Data Model → Write Path → Read Path → Storage Node → Retention and GC → Operations → Security → API (§35.6, §35.7) |
| Building or running a producer | Overview → Data Model → Write Path → Security (§33.2, §33.4) → API → Producer |
| Building a media server (reader) | Overview → Data Model → Read Path → Security (§33.2, §33.4) → API |
| Building a live viewer or a monitoring wall | Overview → Security (§33.1, §33.2) → Relay → API (§35.4, §35.8) |
| Operating a cluster | Overview → Sizing → Deployment → Security (§33.4) → Storage Node (sinks, §22.2) → Operations → Retention and GC |

## Conventions

- **Section numbers are global** across `docs/`, so a reference such as
  §12.2 is unambiguous in every document, and all references are links.
- **Terms.** Objects live in **sinks**. A sink sits on a **device**, the
  physical unit that fails and that serializes I/O. **HDD** is used only for
  hardware and media behavior (sizing, seeks, sequential throughput). "Disk"
  is not used as a term. In production each HDD is one device holding one
  sink ([§22.2](docs/07-storage-node.md#222-sinks-and-devices)). A machine
  running Shale is a **host**, never a device: a Storage Node, a Producer, or
  a Reader ([§9](docs/02-data-model.md#9-identity)). A camera is a
  **source**; the host that uploads a set of sources is its **producer**.
- Defaults quoted in the text (object size, epoch, timeouts, watermarks) are
  decided values. [§36.1](docs/13-configuration.md#361-configuration-reference)
  lists every one of them with its scope and bounds, and
  [§36.2](docs/13-configuration.md#362-open-decisions) lists what is still open.

## Running it

The binary is `cmd/shale`; the design's commands are its subcommands (§34.1).
A single machine for development:

```sh
go build ./cmd/shale
./shale init --dev ./dev                       # CA, keys, first tenant and people
./shale serve all --dev ./dev                  # both APIs, a node, plaintext
./shale --dev ./dev --as @acme/admin set add @acme/cam-set
```

`shale serve all` also runs a relay (§39): `./shale --dev ./dev live
@acme/cam-set` watches a set as a WebRTC viewer and reports what arrived,
and `web/live.html` plays one source in a browser from what `Live`
answers. `deploy/compose/` is the same single machine as containers beside
PostgreSQL, with TLS on; `deploy/systemd/` is several machines under
systemd; `deploy/k8s/` is the Kubernetes deployment of §34.5. `go test
./...` runs the end-to-end harness (`internal/e2e`), which allocates,
uploads, and reads back through `Timeline`, on SQLite, or on PostgreSQL
with `SHALE_E2E_DB_DSN` set. `CLAUDE.md` says how the generated code and
the hand-written layers fit.
