# Shale

Shale is a distributed object store for **continuous CCTV recordings** and
similar large, immutable, loss-tolerant data. It keeps exactly one copy of each
object: no replicas, no erasure coding, no RAID. It spends nearly all raw HDD
capacity on data, keeps HDD I/O sequential, and treats a lost object as a gap
in the recording rather than a failure. Shale does not repair failures. It
isolates them, and it uses source, set, and time information to shape what a
failure takes away.

This file is the entry point to the design. The design itself lives in
[`docs/`](docs/).

## Reading order

Read top to bottom for the full picture. Each document stands on its own once
you know the overview and data model.

| # | Document | Sections | What it covers |
|---|---|---|---|
| 1 | [Overview](docs/01-overview.md) | §1–§6 | Target workload (CCTV), design principles, architecture, the roles of Control Plane, Storage Node, Writer, and Reader, and the project's philosophy. **Start here.** |
| 2 | [Data Model](docs/02-data-model.md) | §7–§10 | Tenants, sources, sets, zones, and epochs; object and write-attempt state machines; node/device/sink identity; time semantics. |
| 3 | [Placement](docs/03-placement.md) | §11 | How a new object is assigned to a sink: weighted rendezvous hashing, set spread across nodes and devices, and `epoch` as the loss-shape knob. |
| 4 | [Write Path](docs/04-write-path.md) | §12–§16 | Allocation and pre-allocation horizon, live and buffered resumable uploads, commit semantics, retries, duplicates and orphans, partial objects, and Writer backpressure. |
| 5 | [Read Path](docs/05-read-path.md) | §17–§19 | Set and time-range queries, read chunks and prefetch, reading while GC deletes, and the states and gap reasons readers see. |
| 6 | [Retention and GC](docs/06-retention-gc.md) | §20–§21 | `expires_at`, `must_delete_by`, and holds; watermark-driven lazy GC with node-proposed, CP-approved deletions. |
| 7 | [Storage Node](docs/07-storage-node.md) | §22–§25 | HDD layout, sinks and devices, no SSD in the data path, `O_DIRECT`, self-describing storage format, the starvation-free Device Queue, and object size. |
| 8 | [Sizing and Hardware](docs/08-sizing.md) | §26 | NIC, HDD-bandwidth, capacity × retention, and RAM ceilings per node, and which hardware to add when each one binds. |
| 9 | [Operations](docs/09-operations.md) | §27–§32 | Health and quarantine, failure semantics and device re-homing, metadata index rebuild, integrity, observability, and CLI. |
| 10 | [Security](docs/10-security.md) | §33 | Trust model and the tenant wall, CP-signed access tokens, key rotation, enrollment of nodes and holders, the built-in CA, and what each compromise costs. |
| 11 | [Deployment](docs/11-deployment.md) | §34 | One binary in four roles (tenant API, cluster API, Storage Node, all-in-one); Kubernetes, Ubuntu with systemd, a single machine, and containers; commit event delivery without a queue. |
| 12 | [API](docs/12-api.md) | §35 | payday conventions, the two gRPC surfaces, entities and their custom RPCs, and the HTTP/QUIC data plane. |
| 13 | [Configuration](docs/13-configuration.md) | §36 | Every tunable with its default, bounds, and who sets it; and the few decisions still open. |
| 14 | [Glossary](docs/14-glossary.md) | §37 | Definitions of all terms. |
| — | [Placement Decisions](docs/placement-decisions.md) | D1–D9 | Decision report behind §11: scenario, requirements, the options considered, and why each one was chosen or rejected. |

## Reading paths by role

| If you are… | Read |
|---|---|
| Evaluating whether Shale fits | Overview → Sizing → Configuration |
| Implementing the Control Plane | Overview → Data Model → Placement (+ decision report) → Write Path → Retention and GC → Security → Deployment → API |
| Implementing the Storage Node | Overview → Data Model → Write Path → Read Path → Storage Node → Retention and GC → Operations → Security |
| Building a producer (Writer) | Overview → Data Model → Write Path → Security (§33.2, §33.4) → API |
| Building a media server (Reader) | Overview → Data Model → Read Path → Security (§33.2, §33.4) → API |
| Operating a cluster | Overview → Sizing → Deployment → Security → Storage Node (sinks, §22.2) → Operations → Retention and GC |

## Conventions

- **Section numbers are global** across `docs/`, so a reference such as
  §12.2 is unambiguous in every document, and all references are links.
- **Terms.** Objects live in **sinks**. A sink sits on a **device**, the
  physical unit that fails and that serializes I/O. **HDD** is used only for
  hardware and media behavior (sizing, seeks, sequential throughput). "Disk"
  is not used as a term. In production each HDD is one device holding one
  sink ([§22.2](docs/07-storage-node.md#222-sinks-and-devices)).
- Defaults quoted in the text (object size, epoch, timeouts, watermarks) are
  decided values. [§36.1](docs/13-configuration.md#361-configuration-reference)
  lists every one of them with its scope and bounds, and
  [§36.2](docs/13-configuration.md#362-open-decisions) lists what is still open.
