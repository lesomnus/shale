# Managed Producer lifecycle

An adopted Producer using a CP SourceProvider can remain idle with zero cameras.
Its host heartbeat, certificate renewal and relay control run while the first
assignment or catalog is unavailable. No synthetic Source bootstraps enrollment.
`Producer.Ready` means these authenticated control loops have started; it does
not mean that a camera has delivered usable video or committed a recording.

The provider factory opens once after machine authentication and Set adoption.
The runtime retries bounded full reads and periodically resynchronizes even if
a notification was lost. Transport errors retain the accepted desired snapshot
and existing captures. Explicit authorization rejection fences them; certificate
renewal's connection replacement is not interpreted as a new catalog. A valid
empty snapshot intentionally stops collection. Foreign scopes, assignments,
older revisions, and changed content at an unchanged revision are rejected.

## Reconciliation and admission

One lifecycle owner compares full desired snapshots. It constructs unpublished
source handles, resolves their exact reserved Source IDs, and checks scope,
aliases and binding labels. It never creates a replacement managed Source or
silently adopts another camera's alias. Each capture incarnation keeps immutable
configuration and its own counters, input reader and FIFO uploader.

An aggregate catalog change does not restart an unchanged Source. A changed
per-source specification or assignment generation requires a new incarnation.
Endpoint changes preserve Source/Binding identity and recording history. Static
file/manual capture commands, USB controls and ONVIF inference remain private
compatibility settings; they are not remotely supplied capture specifications.

Managed negotiation names the exact assignment generation, catalog revision and
collecting membership. It does not charge stopped historical Sources, scale
copied feeds to fit a new camera, or automatically raise a managed ceiling from
starvation telemetry. Single-source policy limits and the Set's aggregate cap
fail with ResourceExhausted rather than changing unrelated video. The optional
Producer uplink limit reserves recording plus peak simultaneous live traffic
and recording headroom (2.2 times the declared total), including on-demand live.
New incarnations reserve a negotiated segment's maximum RAM in addition to bytes
already retained. Existing static negotiation keeps its compatibility behavior.

The sequence is preview, atomic strict commit, changed-source cancellation/join
and bounded FIFO drain, then new capture. A rejected preview or strict CP commit leaves the
existing captures and peers running. Every ticket freezes its storage profile,
so a newly committed lower Source profile cannot lower old queued segments'
max_length or change their incarnation. All changed captures stop promptly;
their independent drains proceed concurrently. Unchanged captures keep running.
If retained queues grow during that commit/drain interval, a final RAM check can
leave changed captures stopped while the replacement waits for enough room;
that later check does not promise to restore their old sessions.

Desired publication is separate from current execution. Publishing a replacement
does not retire the currently running ticket: it may keep serving and recording
while the replacement waits for capacity. Replacement BeginApply, explicit stop,
assignment fencing and ownership revocation retire the old execution. A newer
desired specification does not make the old execution qualify for the new job.

Strict negotiation atomically records each Binding's admitted specification and
assignment generation. BeginApply requires those exact fences; stale nonzero
Source metadata cannot authorize a replacement after capacity rejection. Stop
releases that admission, and re-enabling collection must reserve capacity again.
An unchanged admitted camera can reconnect with a fresh ticket at the latest
catalog while another desired camera waits for capacity. If this same Source's
desired specification changed, only its already established old input session is
retained. Once that session ends, restarting requires approval of the current
desired specification; the runtime does not revive an older input or credential.
CP credential resolution retains that exact desired-reference contract.

## Starting, applied and active

The Producer obtains a monotonic BeginApply ticket before every managed capture
process, including a reconnect with an unchanged specification. Begin uses a
sequence CAS and an idempotent intent key. Retrying a lost response does not
allocate another ticket. Superseded or unknown tickets cannot acknowledge a
newer application. Starting is not applied state.

The new read loop must observe codec configuration, actual sample dimensions,
genuine IDR/IRAP video and advancing video samples. It checks the exact desired
codec, dimensions and cadence, a bounded observed bitrate and any declared
keyframe constraint. Fractional cadence is preserved. Startup measurements use
the existing qualification tolerances; an unknown GOP is not manufactured as
evidence. Neither a running ffmpeg process nor desired metadata acknowledges
capture. This container/bitstream progress proof is distinct from independent
live decoding and from physical USB or vendor qualification.

Applied CAS records only the current Producer ticket and the exact credential
reference actually leased. Duplicate acknowledgments preserve the original
application timestamp. The optional neutral ApplyObserver receives material-free
local applied/stopped events under the current-run guard, independently of CP
ACK transport retries. It can support an explicitly enabled credential cache;
it does not implement offline catalog authorization or cold-start activation.

An epoch is immutable from read loop to Segment, allocation, signed storage
record, Lamina, heartbeat and live stream. Old buffered data is never relabeled
with the current epoch. ACTIVE requires the exact desired specification's real
Producer ACK, current-epoch ingest, a new complete committed current-epoch
recording and advancing independently decoded live evidence carrying that same
epoch. Publication, a fetched URL, worker-reported applied_revision, or a late
old commit cannot substitute for this chain. The storage test uses incarnation
lineage and its starting watermark, not only wall-clock commit time after ACK.

## Upload drain and loss

The lifecycle cancels capture and joins its read owner before draining. Only
that reader calls Cutter.Stop; the manager does not race Feed to close a segment.
There is one FIFO uploader per Source incarnation. Active and draining queues
both remain visible to the process-wide RAM budget and statistics.

`producer.drain_timeout` defaults to two minutes and is bounded by five minutes.
When the process stops, its own context is already canceled, so the drain gets
a bound of its own: `drain_timeout` or 20 s, whichever is shorter, under the
30 s the systemd unit allows (`TimeoutStopSec`). The uploads run on until the
drain ends, not until the process context does; before #255 the shutdown drain
ended at once and every open segment was dropped. At expiry, the uploader is
joined, remaining segments are discarded with a fixed reason, and allocated
rows are reported lost under a bounded report window. Lost segments and bytes remain in lifetime counters even
after the old handle is reclaimed. Removing a source never erases its metadata
or recordings. `retain: written` keeps its existing incomplete-tail semantics.

Ordinary RTSP reconnects may leave closed, unallocated segments behind. The CP
retains at most sixteen retired application tickets for five minutes, with a
server cutoff and frozen negotiated limits. Exact old-epoch Allocate/Renew can
finish captured-before-cutoff data while the current owner remains adopted for
the same assignment. Expired or evicted epochs fail explicitly and enter normal
loss accounting. The history cannot grant ACK, current live or ACTIVE status.
Handover/revocation denies the former owner's future calls regardless of this
window. Previously issued data capabilities and already admitted transfers keep
their independent expiry/session semantics.

## Live isolation

Publish tokens contain per-source signed capture grants. Presence marks a
managed Source; an empty grant is a deny, never legacy compatibility. Epochs
carry the monotonic starting sequence, preventing a delayed valid old token or
reset from rolling live backwards. Only unmanaged Sources use empty epochs.

The Producer reauthorizes membership over its existing relay stream. Reliable,
coalesced control takes priority over the lossy media queue. Reset and data carry
immutable epochs; stale queued data and late helper callbacks are dropped. A
changed source clears its init/recent state and closes only its own peers.
Unrelated source buffers and existing peers survive.

View capabilities and WHEP/recent/renew admission require the expected epoch.
The response exposes Shale-Capture-Epoch for a consumer to verify before using
frame evidence. Ordinary viewers may observe the old currently applied epoch
while a replacement is pending, but the new job's Observe/ACTIVE path requires
its exact new acknowledged revision and epoch.

## Remaining boundaries

This implements common Producer machinery. USB udev/by-path identity and physical
C270 acceptance remain #160; additional actual GOP qualification remains #190.
Central credential/cache and worker-policy composition are separate #196/#197/
#200 work. Complete vendor enrollment and independent live qualification remain
separate acceptance paths; a synthetic operator publication in an authority
test is not a claim that those procedures ran.
