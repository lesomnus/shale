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
| 2 | [Data Model](docs/02-data-model.md) | §7–§10 | Sources, sets, zones, and epochs; object and write-attempt state machines; node/device/sink identity; time semantics. |
| 3 | [Placement](docs/03-placement.md) | §11 | How a new object is assigned to a sink: weighted rendezvous hashing, set spread across nodes and devices, and `epoch` as the loss-shape knob. |
| 4 | [Write Path](docs/04-write-path.md) | §12–§16 | Allocation and pre-allocation horizon, live and buffered resumable uploads, commit semantics, retries, duplicates and orphans, partial objects, and Writer backpressure. |
| 5 | [Read Path](docs/05-read-path.md) | §17–§19 | Set and time-range queries, read chunks and prefetch, reading while GC deletes, and the states and gap reasons readers see. |
| 6 | [Retention and GC](docs/06-retention-gc.md) | §20–§21 | `expires_at`, `must_delete_by`, and holds; watermark-driven lazy GC with node-proposed, CP-approved deletions. |
| 7 | [Storage Node](docs/07-storage-node.md) | §22–§25 | HDD layout, sinks and devices, no SSD in the data path, `O_DIRECT`, self-describing storage format, the starvation-free Device Queue, and object size. |
| 8 | [Sizing and Hardware](docs/08-sizing.md) | §26 | NIC, HDD-bandwidth, capacity × retention, and RAM ceilings per node, and which hardware to add when each one binds. |
| 9 | [Operations](docs/09-operations.md) | §27–§32 | Health and quarantine, failure semantics and device re-homing, metadata index rebuild, integrity and security, observability, and CLI. |
| 10 | [API](docs/10-api.md) | §33 | Control Plane and Storage Node endpoints. |
| 11 | [Open Decisions](docs/11-open-decisions.md) | §34 | Every default and unresolved choice in one table. Check it before implementing. |
| 12 | [Glossary](docs/12-glossary.md) | §35 | Definitions of all terms. |
| — | [Placement Decisions](docs/placement-decisions.md) | D1–D9 | Decision report behind §11: scenario, requirements, the options considered, and why each one was chosen or rejected. |

## Reading paths by role

| If you are… | Read |
|---|---|
| Evaluating whether Shale fits | Overview → Sizing → Open Decisions |
| Implementing the Control Plane | Overview → Data Model → Placement (+ decision report) → Write Path → Retention and GC → API |
| Implementing the Storage Node | Overview → Data Model → Write Path → Read Path → Storage Node → Retention and GC → Operations |
| Building a producer (Writer) | Overview → Data Model → Write Path → API |
| Building a media server (Reader) | Overview → Data Model → Read Path → API |
| Operating a cluster | Overview → Sizing → Storage Node (sinks, §22.2) → Operations → Retention and GC |

## Conventions

- **Section numbers are global** across `docs/`, so a reference such as
  §12.2 is unambiguous in every document, and all references are links.
- **Terms.** Objects live in **sinks**. A sink sits on a **device**, the
  physical unit that fails and that serializes I/O. **HDD** is used only for
  hardware and media behavior (sizing, seeks, sequential throughput). "Disk"
  is not used as a term. In production each HDD is one device holding one
  sink ([§22.2](docs/07-storage-node.md#222-sinks-and-devices)).
- Defaults quoted in the text (object size, epoch, timeouts, watermarks) are
  proposals. [§34](docs/11-open-decisions.md#34-open-decisions) is the single
  place that tracks them.
