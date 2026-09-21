# Shale — Producer

## 38. Producer

The producer is the host program that turns cameras into laminae:
`shale serve producer` ([§5](01-overview.md#5-components),
[§33.4](10-security.md#334-joining-and-adoption)). The storage side never
knows any of this: nodes store bytes, the Control Plane places and indexes
them ([§22](07-storage-node.md#22-storage-node-internals)). This document is
the producer's own design: what it takes in, how it cuts, how it runs capture
processes, how it finds and registers cameras, and how it chooses the values
it negotiates.

### 38.1 Inputs

Every source reaches the producer as **one MPEG-TS byte stream**. Where the
stream comes from is the only difference:

| Kind | Who runs it | Typical use |
|---|---|---|
| **Managed capture** | the producer spawns and supervises a capture process per source ([§38.3](#383-managed-capture)) | USB cameras, IP cameras, CSI cameras on a Pi |
| **Push** (`input: push`) | a process on the host writes the stream to the producer's listener, `producer.push`, in the node's upload contract ([§38.9](#389-pushed-sources-and-raw-frames)): TS by default, or raw frames (`kind: raw`) for what is not video | recording software the operator already runs; vendor SDKs; a robot's sensor streams |
| **File** | `file:` plays a recording through ffmpeg, `raw:` replays one without it, both looped | tests |

The contract is the same for all three:

- MPEG-TS, with the `random_access_indicator` set on keyframes, as every
  standard muxer does. Fragmented MP4 is accepted too; it is cut at fragment
  boundaries with the init segment prepended to each lamina. TS is the
  default because any prefix of a TS segment plays up to its last complete
  packet, which is what an incomplete lamina needs
  ([§15](04-write-path.md#15-partial-laminae)).
- A keyframe at least every 2 seconds ([§12.6](04-write-path.md#126-upload-profile-negotiation)).
- All streams together at or below the source's `max_bitrate`. Audio goes in
  the same TS when a source has it: a camera's own audio as the camera
  sends it, a microphone only when configured, since recording sound in
  public places is restricted in many jurisdictions
  ([producer bench](producer-bench.md)); `audio: {codec: none}` drops it
  ([§38.3](#383-managed-capture)).

The producer never decodes or encodes. It reads packet headers, nothing
inside them.

A segment is the producer's word for what it sends. The cluster keeps it as
a lamina, with the state [§8](02-data-model.md#8-state-model) gives it, and
the producer never sees that state: it holds a segment until a node has it,
and then forgets it.

### 38.2 Cutting segments

The producer keeps, per source, the last PAT and PMT packets it has seen and
the video PID from the PMT. A segment boundary is the first video packet with
`payload_unit_start_indicator` and `random_access_indicator` set, i.e. the
start of a keyframe, at or after the moment the boundary is due. The new
segment starts with the cached PAT and PMT followed by that keyframe, so each
lamina plays on its own.

A boundary is due:

- at the source's staggered phase ([§12.2](04-write-path.md#122-resumable-part-uploads)),
  every agreed segment duration;
- **early**, at the first keyframe after the segment has produced
  `max_bitrate × duration` bytes while its phase boundary is still ahead. A
  stream running at its ceiling reaches that amount exactly at the phase,
  so it is never cut early; one running above it is. The bytes that arrive
  between the decision and the keyframe, at most one keyframe interval plus
  one encoder burst, are what `max_length`'s headroom was sized for
  ([§12.6](04-write-path.md#126-upload-profile-negotiation)), so the node
  refuses nothing. The segment closes cleanly a little short, the next one
  starts at that keyframe, and the following boundary is the next phase
  point as usual. This is the safety net for a ceiling guessed too low
  ([§38.5](#385-choosing-the-ceiling)).

`date_started` is the wall-clock time at which the segment's first keyframe
arrived. `date_ended` is the arrival time of the next segment's first
keyframe, so consecutive segments tile the timeline exactly, or the time of
the last packet when the source stops. Both go into the upload's headers
([§12.2](04-write-path.md#122-resumable-part-uploads)). Wall-clock stamping
is accurate to the pipe's latency, tens of milliseconds, which is enough.

In **live** mode ([§12.2](04-write-path.md#122-resumable-part-uploads)) the
bytes stream to the node as they arrive, and the producer keeps what its
`retain` policy says: the whole segment until it is committed, or, under
`written`, only what the node has not yet reported durable, at the price
that such a segment ends where its node has it if that node dies. In
buffered mode the segment is complete at the next cut and uploaded then.

### 38.3 Managed capture

The producer runs one **capture process** per managed source and reads TS
from its standard output. The default tool is **ffmpeg**. Any program that
writes TS to its standard output satisfies the contract, so a GStreamer
pipeline ending in `mpegtsmux ! fdsink`, or a vendor tool, can take its
place per source.

**Why ffmpeg.** One binary covers V4L2 and RTSP inputs, ALSA audio, and
every hardware encoder the producer is likely to meet (`h264_v4l2m2m` on a
Raspberry Pi, VAAPI, QSV, NVENC), and the distribution's own build is
enough: the [producer bench](producer-bench.md) ran on Raspberry Pi OS's
ffmpeg unchanged. Its flags are what most operators already know, and the
command the producer runs can be copied into a shell and reproduced. A
process boundary keeps Shale's binary static and its license its own, and a
crashing encoder takes down one camera, not the producer. GStreamer would
make the cut simpler, since its application sink hands over each buffer with
its keyframe flag, but only when embedded in the process, which costs the
static binary; as a subprocess it has no advantage over ffmpeg.

**Configuration, in three tiers.** The producer translates the first tier
into ffmpeg arguments, passes the second through, and leaves the third alone.

```yaml
producer:
  cp: https://cp.example.com:7400
  set: lobby                # the set this host is adopted for
  uplink: 40Mbps            # optional; bounds the sum of the ceilings (§38.5)
  ffmpeg: /usr/bin/ffmpeg   # default: from PATH

sources:
  - alias: door             # tier 1: structured, portable
    input: v4l2:/dev/video0
    format: mjpeg           # what the camera delivers: mjpeg | yuyv | h264
    size: 1920x1080
    fps: 30
    encoder: auto           # auto | h264_v4l2m2m | h264_vaapi | h264_nvenc | libx264 | ...
    max_bitrate: auto       # or 4Mbps; the ceiling of §12.6
    keyframe_interval: 2s
    audio: {device: alsa:hw:1, bitrate: 64kbps}   # a microphone; absent: a USB camera has no audio
    controls: {exposure_dynamic_framerate: 0}     # V4L2 controls, set before every start
    idle: {dark_after: 10m}   # skip the segments of a dark scene (§38.10); absent: store everything

  - alias: yard
    input: rtsp://10.1.2.40/stream1
    format: h264            # already encoded: remuxed with -c copy, no encoding
    max_bitrate: onvif      # read from the camera (§38.4)
    # audio: {codec: copy}  # the camera's own audio as it sends it (the default); aac, opus, none

  - alias: gate             # tier 2: structured plus overrides
    input: v4l2:/dev/video2
    format: mjpeg
    size: 1280x720
    fps: 30
    encoder: libx264
    max_bitrate: 2Mbps
    encoder_options: {preset: veryfast, tune: zerolatency}   # -preset veryfast -tune zerolatency
    extra_input_args: [-thread_queue_size, "512"]
    extra_output_args: [-x264-params, "nal-hrd=cbr"]

  - alias: roof             # tier 3: your own command; stdout must be TS
    command: >
      rpicam-vid -t 0 --codec h264 --inline --intra 60 --bitrate 4000000 -o -
      | ffmpeg -f h264 -i - -c copy -f mpegts -
    max_bitrate: 4.5Mbps
```

**Translation** of tier 1:

| Field | ffmpeg |
|---|---|
| `input: v4l2:…`, `format`, `size`, `fps` | `-f v4l2 -input_format <format> -video_size <size> -framerate <fps> -i <device>` |
| `input: rtsp://…` | `-rtsp_transport tcp -i <url>`, with a socket timeout, and `-c:v copy` when `format: h264` |
| `encoder: auto` | the first that works on this host: `h264_v4l2m2m`, `h264_vaapi`, `h264_qsv`, `h264_nvenc`, else `libx264 -preset veryfast` with a warning about CPU |
| `max_bitrate` | encoders with rate control (`libx264`, VAAPI, NVENC, QSV): capped VBR, `-maxrate <video ceiling> -bufsize <2 × ceiling>` around a quality target; encoders that only take a target (`h264_v4l2m2m`): CBR at `-b:v <video ceiling>`. The video ceiling is `max_bitrate` minus the audio bitrate, divided by 1.05 for TS overhead, so the muxed stream stays under the ceiling |
| `keyframe_interval` | `-g <fps × interval> -force_key_frames expr:gte(t,n_forced*<interval>)`, with the agreed interval ([§12.6](04-write-path.md#126-upload-profile-negotiation)), 2 s by default |
| `audio` | a camera's own audio: `-c:a copy`, or `-c:a aac` / `-c:a libopus -b:a <bitrate>` when `codec` says so, `-an` for `none`; a microphone (`device`): `-f alsa -i <device>` as a second input, `-map 0:v:0 -map 1:a:0 -c:a aac -b:a <bitrate>` (`libopus` for `codec: opus`); a USB camera without one: `-an`. Audio that TS has no type for, G.711 from an IP camera above all, comes out of ffmpeg as a private stream nothing names, which no player finds; the first PMT shows it, and the capture is restarted once encoding it as AAC, with a log line saying so |
| output (fixed) | `-f mpegts -` |
| `controls` | not ffmpeg: V4L2 controls set on the device through the ioctls `v4l2-ctl -c` uses, by v4l2-ctl's names, before every start of the capture, since a re-plugged camera forgets them. A control the device does not have, or a value outside its range, is a warning in the log and the rest are set |

The producer records which encoder `auto` chose and shows it in its
heartbeat ([§38.6](#386-health-and-heartbeats)).

**Controls worth setting.** A Logitech camera ships with
`exposure_dynamic_framerate: 1` and halves its frame rate in low light to
lengthen the exposure: a source configured at 30 fps records 15 in the
evening, and nothing but the heartbeat's `frame_rate` says so. `0` keeps
the frame rate and lets the picture darken instead, which is what a
recording wants. `power_line_frequency` (1 for 50 Hz, 2 for 60 Hz) stops
the flicker under mains lighting; `focus_automatic_continuous: 0` stops a
camera hunting for focus. `shale producer scan` prints every control a
camera has with its current value ([§38.4](#384-discovery-and-registration)).

**Supervision.** The producer starts each capture process, reads its output,
restarts it with backoff when it exits, and logs its standard error. While a
process is down the source's segment is closed as "the camera stopped"
([§15](04-write-path.md#15-partial-laminae)) and the next one starts when
frames return. Ten seconds after a start the producer checks what it is
getting: a keyframe interval above 2 s or a measured rate at the ceiling is
logged as a warning with the source's alias, since either will show up as
early cuts or poor motion quality.

### 38.4 Discovery and registration

`shale producer scan` prints what this host can see, as a configuration
skeleton to edit:

- **USB and CSI cameras**: every `/dev/video*` device with its formats,
  frame sizes, and frame rates (V4L2 enumeration), whether it delivers
  H.264 itself, and its controls with their current values. The `size`
  proposed is the largest of 1080p, 720p, and 480p the camera lists, so
  the skeleton runs as printed, and a camera with
  `exposure_dynamic_framerate` gets it turned off in the skeleton
  ([§38.3](#383-managed-capture)).
- **IP cameras**: ONVIF WS-Discovery on the local network. With credentials,
  each camera's media profiles: stream URL, resolution, frame rate, the
  configured bitrate limit, and the GOP length, which are exactly the values
  the producer would otherwise have to guess.
- **Audio devices**: ALSA capture devices.
- **Encoders**: which encoders ffmpeg on this host can open, in the order
  `auto` tries them ([§38.3](#383-encoding)). A build lists encoders for
  hardware it does not have; only those that open are printed.

`shale producer probe [--sources …]` runs each configured source for 30
seconds and reports sustained frame rate, CPU per stream, SoC temperature,
and which encoder engaged, the way the bench did. It answers "how many
cameras can this host carry", not "what bitrate should they have".

**Registration.** After the host is adopted, the producer makes sure every
source in its configuration exists in its set (`SourceService.Add`,
idempotent by alias). The Control Plane assigns ordinals. Removing a source
from the configuration only stops recording it; erasing the `Source` is an
admin's action, because it ends the source's history.

### 38.5 Choosing the ceiling

`max_bitrate` is a contract ([§12.6](04-write-path.md#126-upload-profile-negotiation)):
the node enforces it, the forecast and the link check assume it. So it has
to be a declared number. What Shale can do is choose that number well, and
the way to do it is **not a one-time probe**. A minute of encoding at start-up
describes one scene at one time of day: the bench measured a constant-quality
encoder spending 8.6 Mbps on sensor noise at night, which says nothing about
the same camera at noon or with a car in view. Instead, `max_bitrate: auto`
is a loop with three parts.

1. **A starting value that needs no measurement.**

   | Source | Starting ceiling |
   |---|---|
   | managed capture | from the mode: 640×480@30 → 1 Mbps, 1280×720@30 → 2 Mbps, 1920×1080@15 → 2.5 Mbps, 1920×1080@30 → 4 Mbps, 2560×1440@30 → 8 Mbps, 3840×2160@30 → 12 Mbps; H.265 × 0.6. The table lives in the producer's configuration and comes from the bench, where hardware H.264 at these rates met its target within 5% |
   | `onvif` | the bitrate limit the camera is configured with ([§38.4](#384-discovery-and-registration)), × 1.05 for TS overhead, plus the audio bitrate: the camera's limit is the encoder's, and the ceiling covers the container |
   | any other pass-through stream | the peak one-second rate over the first 60 s × 1.5, logged as a guess |

2. **A guard that costs no bytes.** A segment that has produced
   `max_bitrate × duration` bytes before its phase boundary is running
   above its ceiling, and is cut at the next keyframe
   ([§38.2](#382-cutting-segments)). After an early cut the producer raises
   that source's ceiling by 25% at its next negotiation, whatever its rate
   control: the stream exceeded what was declared, so the declaration was
   wrong. A ceiling guessed too low therefore costs a few short segments and
   nothing else.

3. **Starvation, measured by the producer.** A capped-VBR encoder that
   needs more bits than its cap allows sits at the cap and raises its
   quantizer, which is where motion loses detail. Bitrate alone cannot tell
   that apart from a night of sensor noise, which also sits at the cap, so
   the producer looks at the shape of the time spent at the cap, second by
   second:

   | Term | Definition |
   |---|---|
   | second at the cap | a second whose trailing keyframe interval holds at least 95% of what the ceiling allows for that interval. Measured per bare second, every other second would carry an I-frame and look like a spike ([producer bench](producer-bench.md)) |
   | episode | 3 to 60 consecutive seconds at the cap. Shorter is one keyframe's spike; longer is a scene condition (noise, rain, snow, a crowd) that more bits would only spend on noise |
   | starved | at least 50 episodes in a day, while fewer than 10% of the day's seconds were at the cap |

   Both counts travel in the producer's heartbeat ([§38.6](#386-health-and-heartbeats)).
   For a starved source the console **suggests** raising the ceiling by 25%,
   with the hours of the day the episodes fell in, so a person can see
   whether it is traffic or weather. A set with `auto_raise: on` applies the
   suggestion itself, at most once a day, and the producer re-launches the
   encoder with the new cap at the next segment boundary. The default is
   off, because the episode filter makes the guess good, not certain. A CBR
   source is never adjusted this way: it sits at its ceiling by definition,
   and its quality is a choice, not an observation. Ceilings are never
   lowered automatically. A loose ceiling costs only headroom in the link
   check, and the console suggests lowering it when a source never exceeds
   half of it.

Every step stays inside the `UploadPolicy` cap, the set's
`max_bitrate_total`, and the producer's `uplink` when one is declared: the
sum of the set's ceilings × 1.2 must fit it
([§12.2](04-write-path.md#122-resumable-part-uploads)). When a raise would
break one of these, the loop stops and warns instead.

The **segment duration** follows the ceiling, 64 MB ÷ ceiling by default,
and is re-derived when the ceiling changes. Link parameters (`idle_timeout`,
`abandon_timeout`, `allocation_horizon`) are not probed: the defaults suit
LAN and WAN links, and a wireless producer sets a longer idle timeout in its
configuration.

### 38.6 Health and heartbeats

The producer sends `ProducerService.Heartbeat` every
`producer_heartbeat_interval` (30 s): per source its input state (`up`,
`down`, `starting`), frame rate, measured rate over the last minute,
keyframe interval, encoder, restarts, early cuts, the seconds at the
cap and episodes since the last heartbeat ([§38.5](#385-choosing-the-ceiling)),
and whether its scene is dark and its segments skipped ([§38.10](#3810-dark-scenes));
for the host, CPU, temperature, and uplink usage. Consoles show a camera
that went dark, and the Control Plane's `date_seen` on the `Producer` row
comes from it ([§35.3](12-api.md#353-entities), [§31](09-operations.md#31-observability)).
A producer whose heartbeats have been missing for `producer_down_after`
(90 s, three heartbeats) is shown as down, and so are all of its sources.

**A link that stalls.** Every connection between a host and the Control
Plane, and between a producer and its relay, carries an HTTP/2 keepalive:
a ping every 10 s, on an idle connection too, answered within 5 s or the
connection is closed and the next call redials. A producer on WiFi sees
its link stall now and then, for tens of seconds, most often when a live
session starts and the tee's bytes join the uploads; without the
keepalive that was a heartbeat timed out at its 10 s deadline and a
connection the server ended a minute later. With it the stall costs one
call and a redial. What the stall does to the bytes themselves is the
uplink's problem ([§38.5](#385-choosing-the-ceiling)): recording is
retained and retried, and live viewers get what the link carries.

### 38.7 Live output

The producer never serves viewers. It keeps one gRPC stream to the relay
the CP assigned it ([§39.2](16-relay.md#392-assignment)), and:

- attaches with the publish token from its `Negotiate` or heartbeat answer,
  and asks `ProducerService.Relay` for a fresh assignment the moment the
  stream breaks;
- on `Start {source}`, tees that source's TS stream
  ([§38.1](#381-inputs)) into the relay stream, beginning at the next
  keyframe, and stops on `Stop {source}`. The video is the one it stores,
  never encoded twice. Audio that is not Opus goes through the **live
  helper** first: one `ffmpeg -i pipe:0 -c:v copy -c:a libopus` per watched
  source, fed from the tee, whose output is what the relay gets, since
  browsers play no audio but Opus over WebRTC
  ([§39.4](16-relay.md#394-viewers)). It runs only while the source is
  watched, costs one audio decode and one Opus encode (a percent or two of
  a core), and adds about half a second before the first frame. It is
  supervised like a capture process: started again at the next keyframe
  when it exits, and after three exits the bytes go as they are, silent
  for the viewer but alive; a stream with no audio, or with Opus already,
  needs no helper, and a host without ffmpeg sends the bytes as they are;
- counts the cameras usually watched into its uplink budget
  ([§38.5](#385-choosing-the-ceiling)), since each watched camera costs its
  bitrate once more.

A camera meant to be watched live records H.264 Main or High profile, which
browsers play without transcoding. Its audio is recorded as the camera
sends it, AAC or whatever else, and plays live through the helper;
recording Opus (`audio: {codec: opus}`) makes the helper unnecessary, at
the price of a recording that HLS players do not take
([§39.3](16-relay.md#393-from-the-producer)).

### 38.8 What the producer does not do

- Decode, encode, or look inside a frame. The capture process does that,
  and so does the live helper, another ffmpeg ([§38.7](#387-live-output)).
- Serve viewers. Live viewing goes through the relay
  ([§38.7](#387-live-output)), never through the producer's own uplink to
  each viewer.
- Control cameras: no PTZ, no exposure, no motion detection. A system that
  needs them sits beside the producer and reads recordings through the
  `Timeline` ([§17](05-read-path.md#17-read-path)) or watches live through
  the relay.

### 38.9 Pushed sources and raw frames

A pushed source (`input: push`) is written to the producer by a process on
the host, at the listener `producer.push` names: `unix:/run/shale/push.sock`,
or `tcp://127.0.0.1:7450` for a writer in a container. The contract is the
node's upload ([§12.2](04-write-path.md#122-resumable-part-uploads)), one
per source, that never completes while the source runs:

```text
PUT  /sources/<alias>/frames   Upload-Offset: o   Upload-Complete: ?0 | ?1
                               body = the stream from o (Content-Length, or chunked)
       → 204  Upload-Offset: cur   taken up to cur
       → 409  Upload-Offset: cur   o is not cur, or a request is already open
HEAD /sources/<alias>/frames   → Upload-Offset: cur, Upload-Complete: ?0 | ?1
```

- The offset counts the bytes of the stream the producer has taken. A
  writer keeps what is above the last `204` and, after a disconnect, asks
  `HEAD` and sends from there; a request at any other offset is refused
  with the right one. One request is open per source at a time.
- `?0` is the normal state: the stream goes on in the next request. `?1`
  ends the stream: the open segment is cut at once (`Stopped`), the
  offset returns to zero, and the next request begins a new stream.
- A request with no bytes for `push_idle` (30 s) is answered `204` and
  ended, and a stream with no bytes for that long closes its open segment
  as stopped; both stay open for the next bytes. A sensor that reports
  nothing for a while does not hold a lamina open, and its next record
  starts the next one.
- What the bytes are is the source's `kind` in the producer's
  configuration; the request does not say. `ts`, the default, is the
  contract of [§38.1](#381-inputs), cut at keyframes like a capture's
  stdout. `raw` is frames:

```text
frame = kind (1) | timestamp (8, unix ns, big-endian; 0 = none) | length (4, big-endian) | bytes
kind   0  data     a lamina may be cut before this frame
       1  prefix   kept, and prepended to every lamina that starts after it;
                   replaced by the next prefix frame
```

This is TS with the video taken out: a prefix frame is the PAT and PMT,
every data frame boundary is a keyframe, the timestamp is the PCR. The
schedule stays the producer's, as for a camera: the staggered phase, the
epoch rule and the early cut past `max_bitrate × duration`
([§38.2](#382-cutting-segments), [§25](07-storage-node.md#25-lamina-size)),
with the cut moved to the next frame boundary. A raw source declares
`max_bitrate`, since there is no camera mode to guess it from, and a
stream under about 0.28 Mbps makes a small lamina every quarter epoch, so
low-rate records belong multiplexed into one source.

- Data times are the frames' timestamps when the writer gives them,
  arrival otherwise ([§10](02-data-model.md#10-time-semantics)). A writer
  flushing what it kept while offline lands its records with their own
  times, within `max_backlog_age`. `date_ended` is the next lamina's
  first frame, or the last frame when the stream ended.
- Only the bytes between the frame headers are stored: a lamina is the
  prefix followed by whole records, and a reader that knows the format
  reads it as a file. An incomplete lamina
  ([§15](04-write-path.md#15-partial-laminae)) ends in whole records plus
  at most one torn one.
- What the bytes are is the source's `content_type`
  ([§7](02-data-model.md#7-source-set-zone-epoch)): a raw source's
  configuration names it (`content_type: application/x-mcap`), the
  producer proposes it when it negotiates, and the CP keeps it unless a
  person set another. A TS source is `video/mp2t`; a raw source with no
  name is `application/octet-stream`.
- No live output ([§38.7](#387-live-output)): a raw source has nothing a
  relay could show, and `Live` on one is refused with its content type.
- A self-delimiting format needs one frame per record and one prefix
  frame per header. For MCAP: the magic, the Header and every Schema and
  Channel so far go in the prefix frame, resent whole when a channel is
  added; one Chunk record makes a data frame; a lamina lacks a Footer,
  which MCAP's stream readers accept.

`shale producer push [--to ADDR] ALIAS [FILE]` is the reference writer: it
sends a file, or stdin, in requests of a part each, resumes from `HEAD`,
and completes the stream at the end (`--open` leaves it open). A pushed
source is not probed ([§38.4](#384-discovery-and-registration)); whatever
pushes it is not the producer's process.

### 38.10 Dark scenes

A camera without infrared sees nothing once the lights are off, and in an
office or a lab ten dark minutes mean everybody left and forgot the
camera. Storing that video is cost without a reader: the night is most of
the day. A source with `idle:` skips it.

```yaml
sources:
  - alias: bench
    input: v4l2:/dev/video0
    format: mjpeg
    size: 1280x720
    fps: 30
    idle:
      dark_after: 10m      # this long dark, and the segments are skipped
      threshold: 0.10      # a pixel is dark at or below this luma (0..1)
```

- **Lit: everything is stored**, as without `idle:`. There is no motion
  gating: a lit, static scene is stored.
- **Dark for `dark_after`: storing stops.** Capture goes on, so live
  viewing works and the measurement runs; the segments that open while
  the scene is dark are held in RAM and, at their close, skipped instead
  of uploaded. The CP is told (`LaminaService.Skip`): the lamina becomes
  `SKIPPED` with the span and the reason, and the timeline shows the span
  as a `DARK` gap ([§19](05-read-path.md#19-reader-semantics)), not
  `NOT_RECEIVED`, so a camera nobody is watching is told from a camera
  that died. Skipping is not failing: nothing is `LOST`, no attempt is
  reported, and the row leaves with row retention
  ([§20.4](06-retention-gc.md#204-row-retention)).
- **Lit again: storing resumes at once**, with the segment that is open,
  so up to one segment of what came before the light is stored with it:
  the pre-roll costs nothing, the producer held the segment anyway. The
  next dark spell starts the clock again. Brightness alone decides: the
  lights coming on, a torch, a door opening are all lit frames, and a
  webcam cannot see motion in the dark anyway.
- **What measures it** is the capture's own ffmpeg: a one-frame-per-second
  branch of the video runs through `blackframe` (98 % of the pixels at or
  below `threshold`) into nothing, `-vf split[v][m];[m]fps=1,blackframe,nullsink;[v]null`,
  and it prints a line per dark second on stderr, which the producer reads
  like every other line the process writes. The process runs at info
  level for that, and the lines ffmpeg prints about its inputs and
  outputs are dropped from the log. A second without a line is lit. Only
  a source the producer encodes can be measured: a copied stream
  (`format: h264`, `encoder: copy`) is never decoded, and `idle:` on one
  is refused. An `extra_output_args` with its own `-vf` replaces the
  measuring branch, and the scene is never dark.
- **What it does not do.** A covered lens is dark too; live view is the
  tell. A camera whose exposure stretches in low light halves its frame
  rate before the scene is dark, which is `controls:`
  ([§38.3](#383-managed-capture)), and the encoder keeps running at its
  rate while dark: lowering it needs a restart both ways and is not done.
- **What shows.** The heartbeat's `dark` per source
  ([§38.6](#386-health-and-heartbeats)), `shale.producer.dark{source}`
  and `shale.producer.segments{outcome=skipped}` on the producer,
  `shale.cp.laminae_skipped{reason}` on the CP
  ([§31](09-operations.md#31-observability)), and the log at the two
  transitions: `dark: storing suspended` and `lit: storing resumed`.
