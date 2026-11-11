# Streaming MP4 downloads

Recording exports are ordinary attachment downloads from Shale. An external
viewer handles the camera/time selection and authorization metadata; recording
bytes do not pass through that viewer's application server. This implements
[issue #161](https://github.com/lesomnus/shale/issues/161).

The export is the console's ([issue #270](https://github.com/lesomnus/shale/issues/270)):
one fragmented MP4 of the first recording's init segment, then every selected
recording's fragments, without re-encoding or remuxing. Each fragment's `tfdt`
is moved to where its recording sits on the wall clock and its `mfhd`
renumbered, in place, so no size changes: the response's length is known, and
its first byte sent, once each recording's index and init segment are read,
however long the range. The fragments stream through as they are read. Playback
after the download finishes is the contract. HTTP Range resumption is not part
of this endpoint.

## Authorization and download

`LaminaService.Export` takes one source, `from`, and `to`. An authenticated
recording reader receives a short-lived URL and its expiry. External apps use
`DelegationService.Export` with their server-only delegation handle and the same
recording request. The returned URL contains a narrowly scoped bearer capability,
not the delegation handle or the app's roster exchange credential.

The capability binds its actor, tenant, source and time range. A delegated export
also names the app and cannot widen its recording permissions. Its admission
expiry is no later than the delegation's end time.
Treat the URL as a credential: avoid analytics/referrers and redact its `token`
query parameter from access logs. Opening the URL as a normal browser download
avoids fetching the entire response into a JavaScript Blob.

The download handler validates the capability and current storage state before
starting the response. It reads only canonical Storage endpoints selected by
Shale, verifies HTTPS against public and cluster roots, and does not follow
redirects carrying read credentials. Plain HTTP is development-only.

## Admission failures

A URL can be issued successfully and then fail when opened: capability
resolution also reads current source visibility and recording availability.
Resolver failures keep their existing HTTP status and return fixed text that
identifies the next step, rather than calling every failure an authorization
failure ([issue #171](https://github.com/lesomnus/shale/issues/171)).

| HTTP status | Download response |
| --- | --- |
| 400 | `invalid export request` |
| 401 | `this export link is invalid or expired; request a new export` |
| 403 | `you are not permitted to download this export` |
| 404 | `no recording is available for this export range` |
| 408 | `export preparation was canceled or timed out; try again` |
| 429 | `export limits were exceeded; request a shorter range` |
| 503 | `exports are temporarily unavailable; try again later` |
| 500 | `cannot prepare this export; try again later` |

For resolver failures, 408 covers gRPC `Canceled` and `DeadlineExceeded`; 503
covers `Unavailable` and `FailedPrecondition`. Other unclassified failures
remain 500. These messages do not expose the resolver's error text, Storage
URLs or bearer credentials. The diagnostic event `MP4 export admission failed`
records only `grpc_code` and `http_status`, in addition to the logger's standard
event fields, so operators can distinguish these cases without recording the
download URL. Capability scope, admission expiry and execution limits are
unchanged. This classification does not establish the cause of any particular
failed recording export.

## Serving and resource costs

Exports are opt-in on the tenant HTTP listener. Unlike ordinary Timeline reads,
the exporting Control Plane process carries the complete media stream from
Storage to the browser. Give the selected Shale replicas/ingress enough bandwidth
and set concurrent-job and execution-time limits before enabling it. The external
viewer remains metadata-only; this does not make export bandwidth disappear.

Enable it in the control-plane configuration, then expose only the selected
tenant HTTP listener through the download ingress:

```yaml
export:
  enabled: true
  public_url: https://shale.example/exports/recording.mp4
  max_concurrent: 2
  per_actor: 1
  max_duration: 30m
```

`public_url` defaults to `/exports/recording.mp4` relative to the tenant HTTP
origin; an external viewer should configure an absolute public URL or resolve
this path against that HTTP origin, not its own frontend or the native gRPC
port. An ingress can route this path to a selected set of CP replicas. Set
proxy buffering/timeouts consistently with `max_duration`. Never expose the
cluster listener as the public download listener. Zero limits select the defaults
above; negative limits are refused. Plain HTTP public/storage URLs are allowed
only with development mode.

For example, request a download URL as an existing authenticated reader:

```sh
shale lamina export '{"source":{"slug":{"alias":"front","tenant":{"alias":"acme"}}},"from":"2026-10-30T10:00:00Z","to":"2026-10-30T10:30:00Z"}'
```

The command answers `url` and `dateExpires`; navigate to that URL as a file
download. Existing tenant/site visibility applies. Where ordinary reads require
explicit roster grants, grant `/shale.LaminaService/Export` as well. A delegated
app uses `/shale.DelegationService/Export` with a `DelegationExportRequest` and
its exchange credential; its existing `/shale.DelegationService/Timeline`
recording grant and handle bounds are the permission ceiling.

Memory is bounded independently of output size: a job holds each recording's
index and init segment, and two 4 MiB media buffers. The HTTP response does not
require an output-sized RAM buffer, OPFS copy, or complete staging file.

Cuts are the console's: from the last indexed keyframe at or before the range's
start to the first after its end, each recording's keyframe times placed on the
wall clock from its `date_started`. A recording that goes on from the one before
it in the same encoder session (within 2 s of where that session would put it)
keeps the session's own decode times; any other is placed by the wall clock,
never before what came before it, so a gap stays a gap. Track settings that
differ between recordings (the encoder changed) cannot share one init segment
and are refused before any output body is sent.

Planning reads, per recording, one 16 KiB range at its end (the `mfro` and an
index of a few hundred keyframes) and its init segment: two small exact Storage
requests, for up to eight recordings at once (`ScanParallel`). It also reads
512 KiB where the first recording's part begins, for its first samples' times,
which place the output's start. Nothing else is read before the first byte.

Media then streams in reads of up to 4 MiB (`MediaChunk`) of each recording's
selected range, one HTTP request each, the next read while the previous one is
written to the client. Top-level boxes are followed across reads: each `moof`
is held only while it is rewritten; `mdat` passes through.

The init segment is written first, so the durations it says (`mvhd`, `tkhd`,
`mdhd`, and an `mehd` added to `mvex`) come from the plan: to the keyframe the
range is cut at, or to the last recording's `date_ended`. Players read them as
the length; the fragments' own times are exact. `mvhd`'s creation time is the
first sample's wall clock time.

Planning limits a job to a seven-day requested window and 1,024 input
recordings. The capability resolver also examines at most 20,000 returned
laminae before media planning; skipped/unavailable Timeline rows and gaps are
paged without becoming source inputs. These are resource bounds, not an
output-size limit; choose a shorter range when they are exceeded. Delegated
recording boundaries have the same whole-lamina read semantics as Timeline
tokens.

The URL's expiry controls admission. Like issued Storage read capabilities, a
previously issued export URL remains redeemable until its expiry after a grant
change, standing change or handle revocation; it cannot mint a new export URL.
A started export has a separate bounded job deadline and can run beyond the
ticket or delegation's end, renewing read credentials only for its initially
admitted laminae. Set `max_duration` according to this authorization lifetime as
well as bandwidth. Disconnecting cancels upstream reads. A failure after response headers
are sent aborts the response, leaving an incomplete download rather than appending
an error message to the MP4. A failed or expired URL must be requested again.

A fragmented MP4 has no sample tables to finish, so a download cut short still
plays what arrived up to its last whole fragment; the browser reports it failed,
short of its `Content-Length`. The file has no `sidx` or `mfra`: players seek
by its fragments' `tfdt`s. Chromium seeks a 45-minute export to its middle and
end in a few range reads (see Validation).

## Validation

`go test ./internal/mp4export ./internal/exporthttp` checks the layout, payload
preservation, keyframe cuts, encoder restarts and session continuity, recording
gaps, recordings without an index, what planning reads, bounded streaming over
4 GiB, cancellation and incomplete HTTP responses. With `ffmpeg` and `ffprobe`
in `PATH`, the export tests additionally compare every packet's hash, PTS and
DTS with an independent demuxer, decode every output track, and cover B-frame
presentation offsets. The normal CI test job installs these tools.

`go test ./internal/e2e -run 'Test(NativeExport|DelegatedNativeExport)'` exercises
the real API stack, Storage upload and native HTTP downloads, including tenant,
site, Reader and delegated recording permissions, and the placement of an
encoder restart across two recordings.

For browser seeking and ordinary attachment downloads, generate synthetic
artifacts and run the portable browser check (Playwright Chromium installed):

```sh
MP4EXPORT_ARTIFACTS=/tmp/shale-export MP4EXPORT_LARGE=1 \
  go test ./internal/mp4export -run 'TestValidationArtifacts$' -count=1
cd ts
node test/export-download.mjs /tmp/shale-export
```

`CHROMIUM_EXECUTABLE` optionally selects a locally installed browser. The large
artifact is 303,723,820 bytes of 450 recordings, 45 min 45.033 s, and checks
front/middle/tail seeks and a complete ordinary download without Range. It
contains synthetic video, AAC and Opus, not camera footage. These browser checks
serve completed export artifacts; the end-to-end Go tests separately cover
ticket issuance and the live export HTTP endpoint.
