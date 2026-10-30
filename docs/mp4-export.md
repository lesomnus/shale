# Streaming MP4 downloads

Recording exports are ordinary attachment downloads from Shale. An external
viewer handles the camera/time selection and authorization metadata; recording
bytes do not pass through that viewer's application server. This implements
[issue #161](https://github.com/lesomnus/shale/issues/161).

The export reads compatible stored fragmented MP4 recordings, rebases the sample
timeline without re-encoding, writes media data incrementally, then appends the
normal MP4's final `moov` sample tables and duration. Playback after the download
finishes is the contract. Progressive playback during generation and HTTP Range
resumption are not part of this endpoint.

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

Media-copy memory is bounded independently of output size. Sample-table metadata
still grows with sample count and needs its own limit. The HTTP response does not
require an output-sized RAM buffer, OPFS copy, or complete staging file. Output
is clipped at decodable video boundaries. Compatible encoder restarts and gaps
must preserve the wall-clock placement of audio and video; incompatible track
settings are refused before any output body is sent.

Planning currently limits a job to a seven-day requested window, 1,024 input
recordings, 2,000,000 samples and 64 MiB of parsed container metadata. The
capability resolver also retains at most 20,000 returned laminae before
media planning; skipped/unavailable Timeline rows and gaps are paged without
becoming source inputs. These are resource bounds, not a 256 MiB output-size limit;
choose a shorter range when they are exceeded. Cuts can include the preceding
keyframe and the GOP covering the requested end, just as the existing viewer
does; delegated recording boundaries have the same whole-lamina read semantics
as Timeline tokens.

The URL's expiry controls admission. Like issued Storage read capabilities, a
previously issued export URL remains redeemable until its expiry after a grant
change, standing change or handle revocation; it cannot mint a new export URL.
A started export has a separate bounded job deadline and can run beyond the
ticket or delegation's end, renewing read credentials only for its initially
admitted laminae. Set `max_duration` according to this authorization lifetime as
well as bandwidth. Disconnecting cancels upstream reads. A failure after response headers
are sent aborts the response, leaving an incomplete download rather than appending
an error message to the MP4. A failed or expired URL must be requested again.

The `moov` at the tail is intentional. Completed local files can seek to it;
moving it to the front is an optional faststart optimization, not a prerequisite
for playing the downloaded file. The exporter creates a normal MP4 sample table,
not a second `moov` appended to an existing fragmented initialization segment.
