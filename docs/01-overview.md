# Shale — Overview

## 1. What Shale Is For

Shale was built to archive **continuous CCTV recordings**. Every trade-off in
these documents is made for that workload first:

| Property | CCTV archive |
|---|---|
| Sources | Hundreds to tens of thousands of cameras, recording 24/7 at a bounded bitrate (a declared ceiling, typically 1–16 Mbps) |
| Producers | Cameras come in **sets** (e.g. 4–16) behind one producer host. A set powers on and off as a unit |
| Write pattern | Continuous, write-dominant, never modified |
| Unit of storage | A video segment of tens of MB (≈ 1–10 minutes of video) |
| Retention | Days to months, then delete the oldest ("ring buffer" of storage), with legal caps in most jurisdictions |
| Readers | A small number of media servers that play back or export a camera's time range and fan it out to many viewers |
| Read pattern | Rare, bursty, sequential within a time range, and usually **a whole set at once** (all angles of the same moment) |
| Loss tolerance | A lost segment is a gap in the recording, not a service failure |

Shale is therefore optimized for **ingest throughput, raw capacity, and
predictable HDD behavior**, and deliberately gives up durability and read
latency. The one kind of read parallelism it does keep is across the members
of a set ([§11](03-placement.md#11-placement)).

Other workloads with the same shape (log archives, sensor dumps, large
append-only artifacts) fit as well, but they are not the design target.

Shale is not a general POSIX filesystem, and not a general S3 replacement.

Shale is also **not a live-viewing path**. Watching cameras live goes over a
separate channel (typically camera → media server) that never touches Shale.
Shale stores recordings and serves them back after they are committed. Live
*upload* ([§12.2](04-write-path.md#122-resumable-part-uploads)) only gets
recordings onto disk sooner. It does not make them viewable sooner.

## 2. Overview

**Shale** is a distributed storage system for large immutable objects kept
for a bounded retention period.

Where typical distributed storage spends raw capacity on replicas, erasure
coding, or RAID to raise durability, Shale **explicitly accepts losing some
objects** and in exchange uses almost all raw capacity and keeps HDD I/O
sequential.

Assumptions:

- An object is written once and never modified.
- Writes arrive continuously.
- Objects are tens of MB or larger.
- Losing some objects is not a service failure.
- Old objects may be deleted according to retention policy.
- Reads exist but are far rarer than writes, or arrive in bursts.
- Source and time information can guide placement.
- The number of storage nodes and HDDs grows over time.

## 3. Design Principles

```text
no replica
no erasure coding
no RAID

one physical copy (best effort; an occasional duplicate is tolerated)
immutable object
partial loss acceptable
metadata loss acceptable (rebuildable from sinks, best effort)

object data never touches SSD/NVMe
stateless producers, stateless readers (w.r.t. cluster state)

control plane / data plane separation
resource-oriented gRPC for control (payday); HTTP/QUIC only for object bytes
tenant wall always on; a single organization is one tenant
storage nodes trust only CP signatures; they never know tenants or people
people sign in; hosts are adopted once, and the CP manages their certificates
one binary: Kubernetes, plain Linux, or a single machine; no external queue

one serialized, starvation-free I/O queue per physical device
set + source + epoch aware placement

lazy retention GC, hard deletion dates
```

## 4. Architecture

```text
                         ┌──────────────────────┐
                         │     Control Plane    │
                         │                      │
                         │ Placement            │
                         │ Metadata index       │
                         │ Access tokens        │
                         │ Retention            │
                         │ Retry/Reallocation   │
                         │ Sink/device health   │
                         │ Host adoption, CA    │
                         └──────────┬───────────┘
                                    │
                              Metadata DB
                                    │
                ┌───────────────────┴───────────────────┐
                │                                       │
            Producers                                Readers
        (set gateways)                          (media servers)
                │                                       │
           PUT object                              GET / Range
                │                                       │
                ▼                                       ▼
       ┌────────────────┐                     ┌────────────────┐
       │ Storage Node A │                     │ Storage Node B │
       │                │                     │                │
       │ RAM part       │                     │ RAM read       │
       │ buffers        │                     │ buffers        │
       │ Device Queues  │                     │ Device Queues  │
       │ HDD × N        │                     │ HDD × N        │
       └────────────────┘                     └────────────────┘
```

Object data never passes through the Control Plane. The Control Plane handles
metadata and placement only; Producers and Readers talk to Storage Nodes
directly. The Control Plane does talk to Storage Nodes, but only to give
orders: deletions, date changes, quarantine, reconciliation
([§35.7](12-api.md#357-storage-node-control-api)).

## 5. Components

### Control Plane

- Serve two gRPC surfaces: the **tenant API** for producers, readers, and
  tenant admins, and the **cluster API** for Storage Nodes and operators
  ([§35.2](12-api.md#352-two-api-surfaces))
- Adopt hosts and run the built-in CA: issue, renew, and refuse the
  certificates of nodes, producers, and readers
  ([§33.4](10-security.md#334-joining-and-adoption))
- Issue object IDs and attempt IDs
- Decide placement down to the **sink**, a storage directory on a physical
  device (see [§22.2](07-storage-node.md#222-sinks-and-devices) and the [placement decision report](placement-decisions.md))
- Sign access tokens (presigned URLs) for every data-plane request
  ([§33.2](10-security.md#332-access-tokens))
- Maintain the metadata index
- Keep retention policy and object dates, approve deletions, and order the
  immediate ones
- Reallocate after write failures
- Track node, sink, and device health, and quarantine failing devices
- Call nodes' control API for everything above that needs a node's hands
  ([§34.9](11-deployment.md#349-events-and-directives))
- Version the placement policy

### Storage Node

The data plane. It knows nothing about producers or readers and serves any
request that carries a valid CP-signed access token
([§33.2](10-security.md#332-access-tokens)).

- Accept resumable uploads, staging each part in a RAM part buffer, subject
  to admission control ([§12.2](04-write-path.md#122-resumable-part-uploads))
- Run one Device Queue per device
- Write objects durably and self-describingly ([§23](07-storage-node.md#23-object-model-and-storage-format))
- Serve full and range reads
- Delete objects the Control Plane approves or orders, and sweep past
  deletion dates daily ([§20.1](06-retention-gc.md#201-date_deleted-is-an-expiry-not-an-event))
- Detect sink pressure and propose GC candidates
- Push commit, deletion, and missing-file events to the CP
  ([§34.9](11-deployment.md#349-events-and-directives))
- Serve the control API the CP calls ([§35.7](12-api.md#357-storage-node-control-api))
- Send node, sink, and device heartbeats

### Producer

A Producer is the host behind one set: one machine, one certificate, running
`shale serve producer`. It receives the encoded streams of the set's cameras,
cuts them into segments, and uploads them. It holds no durable or cluster
state, but **it owns each segment until the Storage Node acknowledges the
commit**.

1. Join the cluster on first start and wait to be adopted
   ([§33.4](10-security.md#334-joining-and-adoption)).
2. Negotiate the set's upload profile with the Control Plane from its
   cameras' settings and its link ([§12.6](04-write-path.md#126-upload-profile-negotiation)).
3. Cut segments with boundaries staggered across the set's cameras ([§12.2](04-write-path.md#122-resumable-part-uploads)).
4. Keep allocations for the segments due within `allocation_horizon`
   (negotiated, default 10 minutes) fetched ahead from the Control Plane ([§12.1](04-write-path.md#121-flow)), in
   memory only.
5. Upload each segment to the assigned Storage Node, either **live** (streamed
   while it is recorded; the default for CCTV) or **buffered** (sent once it is
   complete) ([§12.2](04-write-path.md#122-resumable-part-uploads)). Either way the upload is resumable: after a disconnect,
   the producer continues from the offset the node reports.
6. Treat the object as stored only after the final response, which the node
   sends only after the data is durable on HDD ([§12](04-write-path.md#12-write-path)).
7. Keep resuming on the same target while the upload makes progress.
8. Report a failed attempt and move to the next candidate on persistent failure.
9. Give up (object → LOST) when retries are exhausted.

Because a set's cameras are spread over many nodes ([§11](03-placement.md#11-placement)), a producer keeps a pool
of keep-alive connections, one per node it is currently writing to.

What a producer takes in is an **encoded stream** in a streamable container
(MPEG-TS, fragmented MP4). It never decodes or encodes; the camera or a
capture process in front of it does. The producer can run and supervise
those capture processes itself, find cameras on the host and the network,
and choose its bitrate ceilings, which is the producer's own design
([§38](15-producer.md#38-producer)) and changes nothing on the storage side.

### Reader

A Reader is a media server host. It is stateless. `shale serve reader` runs
beside the media server and holds its certificate
([§33.4](10-security.md#334-joining-and-adoption)).

1. Query the tenant API for objects (by ID, or `ObjectService.Timeline` for a
   set or source over a time range, one page at a time).
2. Receive read tokens (GET URLs) with the answer.
3. Read whole objects or byte ranges directly from Storage Nodes.

## 6. Identity of the Project

Shale solves this problem:

> For large immutable workloads that tolerate partial loss, run many nodes and
> HDDs as one logical store without spending raw capacity on redundancy, while
> treating the HDD's physical behavior as a first-class design input.

Typical object stores optimize for durability. Shale chooses the opposite:

```text
give up some durability
        ↓
capacity efficiency
        ↓
no rebuild / recovery
        ↓
simple failure isolation
        ↓
HDD-friendly sequential I/O
```

Shale's core is not just "no RAID". It is **isolating failures instead of
repairing them, and using source and time to deliberately shape what a failure
takes away.**
