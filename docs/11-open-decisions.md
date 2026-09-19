# Shale — Open Decisions

## 34. Open Decisions

| Topic | Decision needed | Current proposal |
|---|---|---|
| Object size | default target and `max_object_size` | 64 MB target, 128 MB max |
| Uploads | `part_size`, `max_uploads`, `part_buffer_pool` | 16 MB, 64 per sink, 12 GiB |
| Upload timeouts | `idle_timeout`, `resume_timeout`, `abandon_timeout` | 30 s, 2 min, 5 min |
| Upload mode | default mode and `retain` for CCTV producers | `live`, `until_commit` |
| Scheduler weights | WRITE : READ : MAINT | 1 : 1 : 0.1 |
| Read chunk | size | 16 MB |
| Placement | default `epoch`, `set_spread` | 1h, `spread` |
| Pre-allocation | `allocation_horizon`, `allocation_ttl` | 10 min, 20 min |
| Zone spread | use `zone` labels in placement | not in v1; soft anti-affinity later if needed |
| Reading live objects | serve an `open` upload's written prefix to readers (near-real-time playback from storage) | not in v1; the data is already in the sink, so this is an API question |
| QUIC | HTTP/3 for lossy wireless links | revisit after measuring TCP + BBR |
| Watermarks | critical / low / target | 3% / 5% / 8% |
| Quarantine | score decay, thresholds, cool-down | TBD from failure data |
| Retry limits | same-target / placement | 2 / 3 |
| Event replay window | on node restart | 10 min |
| Hold vs. `must_delete_by` | which wins | policy / legal input needed |
| Clock tolerance | max `start_time` skew accepted | e.g. 5 min |
| Duplicate handling | delete immediately or leave to GC | leave to GC |
| Checksum | on / off, algorithm | off; CRC32C if enabled |
| URL signing | on / off | on (cheap, catches misrouting) |

| Sink layout | file-per-object vs. append-only volume files | file-per-object (see note) |

**Note on volume files.** Packing many objects into large pre-allocated volume
files (Haystack style) would remove per-object filesystem metadata entirely.
Deletion would then happen per volume, which fits epoch-grouped, similarly
expiring data. Holds would pin whole volumes, and the unlink/fd semantics of
[§18](05-read-path.md#18-delete-during-read) would change. It is not needed at 64–128 MB objects; revisit if objects
shrink.

**Note on multipart.** S3-style multipart (independent parts, a part list,
and a completion call) was considered and dropped. The offset-based resumable
upload of [§12.2](04-write-path.md#122-resumable-part-uploads) gives the same RAM savings and restart safety, and its only
state is a file size.
