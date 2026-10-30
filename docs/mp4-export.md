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
also names the app and cannot widen its recording permissions or handle lifetime.
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

Media-copy memory is bounded independently of output size. Sample-table metadata
still grows with sample count and needs its own limit. The HTTP response does not
require an output-sized RAM buffer, OPFS copy, or complete staging file. Output
is clipped at decodable video boundaries. Compatible encoder restarts and gaps
must preserve the wall-clock placement of audio and video; incompatible track
settings are refused before any output body is sent.

The URL's expiry controls admission. A started export has a separate bounded job
deadline; disconnecting cancels upstream reads. A failure after response headers
are sent aborts the response, leaving an incomplete download rather than appending
an error message to the MP4. A failed or expired URL must be requested again.

The `moov` at the tail is intentional. Completed local files can seek to it;
moving it to the front is an optional faststart optimization, not a prerequisite
for playing the downloaded file. The exporter creates a normal MP4 sample table,
not a second `moov` appended to an existing fragmented initialization segment.
