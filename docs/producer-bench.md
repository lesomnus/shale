# Shale Producer Bench — Raspberry Pi 400

Measurements of a small producer recording one camera. They inform the upload
design ([§12.6](04-write-path.md#126-upload-profile-negotiation)) and anyone
choosing producer hardware. This is not part of the storage design itself.

Interactive frame comparison and charts:
<https://claude.ai/artifact/DrFH9JXumuPbPGMRuQm2cR> (private to its owner
until shared).

## Setup

| Item | Value |
|---|---|
| Host | Raspberry Pi 400 (Pi 4 SoC, 4 × Cortex-A72 at 1.8 GHz, 3.7 GiB), Debian 13 |
| Camera | USB 2.0 Camera (UVC): MJPEG up to 1920×1080 at 30 fps; YUYV only at 5–10 fps above 640×480; mono microphone |
| Software | ffmpeg 7.1.5 (Raspberry Pi build) |
| Encoders | `h264_v4l2m2m` (hardware), `libx264 -preset veryfast` (software) |
| Scene | night street through a window: low light, heavy sensor noise, almost no motion; repeated at daybreak (06:54 KST, overcast light, a parked truck, almost no traffic) |
| Date | 2026-09-19 |

## Encoding

15 s per recording, camera MJPEG decoded in software, then encoded.

| Recording | Measured | Frames | ffmpeg CPU (% of one core) | Per camera per day |
|---|---:|---|---:|---:|
| Camera MJPEG 1080p30 (no re-encode) | 118.7 Mbps | 451 / 450 | — | 1,282 GB |
| HW H.264 1080p30, 8 / 4 / 2 Mbps | 8.26 / 4.17 / 2.12 Mbps | 450 / 450 | ~145% | 89 / 45 / 23 GB |
| HW H.264 1080p15, 4 / 2 Mbps | 4.15 / 2.10 Mbps | 225 / 225 | ~74% | 45 / 23 GB |
| HW H.264 720p30, 4 / 2 / 1 Mbps | 4.16 / 2.12 / 1.10 Mbps | 450 / 450 | ~70% | 45 / 23 / 12 GB |
| HW H.264 640×480 30 fps, 1 / 0.5 Mbps | 1.10 / 0.58 Mbps | 450 / 450 | 42% | 12 / 6 GB |
| x264 CRF 23, 720p30 | 8.59 Mbps | 449 / 450 | 358% | 93 GB |
| x264 CRF 23, 1080p15 | 19.31 Mbps | **189 / 225** | 353% | 209 GB |

Findings:

- **Hardware encoding is required.** The Pi's H.264 encoder keeps up at
  1080p30. Software x264 loses 16% of frames at 1080p15 while using all four
  cores.
- **The hardware encoder meets its target within 5%**, so a capped stream's
  storage is predictable.
- **Constant-quality encoding follows the scene, not a budget.** In low light,
  sensor noise is most of the picture, and x264 at CRF 23 spent 8.6 Mbps at
  720p. This is why producers declare a ceiling (`max_bitrate`), and the CP
  learns the actual rate from commits
  ([§12.6](04-write-path.md#126-upload-profile-negotiation)).
- **Most of the CPU goes to decoding the camera's MJPEG**, not to encoding. A
  USB MJPEG camera is the expensive case: about 1.5 cores per 1080p30 stream on
  this hardware, so two such cameras are about the limit (not tested). IP
  cameras that already deliver H.264 or H.265 need no re-encoding. The producer
  only remuxes them, at negligible CPU cost.

## Daybreak

The same recordings again at daybreak, 15 s each, in the same order.

| Recording | Night | Daybreak | Frames (daybreak) | ffmpeg CPU |
|---|---:|---:|---|---:|
| Camera MJPEG 1080p30 (no re-encode) | 118.7 Mbps | 101.1 Mbps | 451 / 450 | — |
| HW H.264 1080p30, 4 / 2 Mbps | 4.17 / 2.12 Mbps | 4.16 / 2.12 Mbps | 450 / 450 | ~136% |
| HW H.264 720p30, 2 / 1 Mbps | 2.12 / 1.10 Mbps | 2.12 / 1.11 Mbps | 450 / 450 | ~68% |
| HW H.264 640×480 30 fps, 1 Mbps | 1.10 Mbps | 1.10 Mbps | 450 / 450 | 38% |
| HW H.264 1080p30, 20 Mbps | — | 20.50 Mbps | 450 / 450 | — |
| x264 CRF 23, 720p30 | 8.59 Mbps | **2.14 Mbps** | 450 / 450 | 190% |
| x264 CRF 23, 1080p15 | 19.31 Mbps | **9.52 Mbps** | 225 / 225 | 347% |

Findings:

- **The hardware encoder is constant-rate through ffmpeg.** Asked for
  20 Mbps on a static daylight scene, it delivered 20.5. Its driver
  advertises a variable-bitrate mode (`video_bitrate_mode`, default VBR),
  but `h264_v4l2m2m` gives no way to select it, and the output pads to the
  target whatever the scene. For the producer, `-b:v` on this encoder is the
  rate the camera will cost, day and night
  ([§38.3](15-producer.md#383-managed-capture)).
- **Constant quality is four times cheaper by day.** x264 CRF 23 at 720p fell
  from 8.6 to 2.1 Mbps, and at 1080p15 from 19.3 to 9.5, for the same quality
  target. What changed is the sensor noise, not the content. A source with a
  quality target and no ceiling would spend most of its budget on nights;
  a ceiling is what stops that ([§12.6](04-write-path.md#126-upload-profile-negotiation)).
- **1080p15 no longer drops frames in software**: 225 of 225 by day against
  189 of 225 at night. Less noise is fewer bits to encode.
- **Daylight frames are where the bitrates can be judged.** At whole-frame
  size 1080p30 at 4 Mbps is close to the camera's own picture; at 2 Mbps
  foliage and small lettering soften; 720p30 at 1 Mbps is soft but every
  object in the scene is still readable. The night frames had shown mostly
  noise at every rate.

## Ten minutes, second by second

Two longer daybreak recordings, read back one second at a time: the
hardware encoder at 1080p30 with `-b:v 4M`, the setting a producer would
run, and x264 at 720p30 as a capped variable-rate reference
(`-crf 23 -maxrate 2M -bufsize 4M`). "At the cap" follows the producer
design ([§38.5](15-producer.md#385-choosing-the-ceiling)): a second counts
when its trailing keyframe interval holds at least 95% of what the ceiling
allows.

| | HW H.264 1080p30, 4 Mbps | x264 720p30, capped 2 Mbps |
|---|---:|---:|
| Duration | 594 s | 300 s |
| Mean | 4.01 Mbps (100.3% of the ceiling) | 1.70 Mbps (85%) |
| Median second | 4.01 Mbps | 1.46 Mbps |
| Lowest / highest second | 2.36 / 4.65 Mbps | 1.08 / 2.35 Mbps |
| Highest trailing interval | 4.18 Mbps | 1.89 Mbps (95%) |
| Seconds at the cap | 592 of 594 (99.7%) | 0 |
| Episodes of 3–60 s | 0 (one run of 592 s) | 0 |
| Keyframe interval | 2.0 s, every time | 2.0 s, every time |
| Frames | — | 9,000 of 9,000 |

Findings:

- **A constant-rate source shows nothing.** Every interval of the hardware
  run sat at the cap, as CBR must. This is why the design never applies the
  starvation rule to CBR sources: for them, "at the cap" is the resting
  state, not a signal.
- **A capped source in a quiet scene idles at 70–95% of its cap** and never
  holds it for a full interval, so the episode count is zero. The cap here
  was set at the scene's own constant-quality rate (2.1 Mbps in the short
  run), which is the least favourable case for savings; even so the stream
  cost 15% less than a constant 2 Mbps, and 20% less than the hardware
  encoder's 2.12. A cap set above the quiet-scene rate, as the design's
  starting table does, saves the whole difference in quiet hours.
- **Seconds alone mislead; intervals do not.** With a keyframe every 2 s,
  every other second carries an I-frame: counted per second, half of the
  x264 run's seconds were "at 95% of the cap"; counted per trailing
  interval, none were. The design measures per interval for this reason.
- **The rate drifts with the light.** Over the five x264 minutes the rate
  fell from about 1.9 to 1.5 Mbps as the sky brightened and sensor gain
  dropped. Noise, again, is the variable.

Nothing crossed the view during either run that the bitrate shows. The
episode rule still has no motion under it.

## Audio in the same segment

| | |
|---|---|
| Streams | H.264 1280×720 30 fps at 2 Mbps + AAC mono 48 kHz at 64 kbps, one MPEG-TS |
| Total | 2.19 Mbps (+3%) |
| Start offset between streams | 21 ms |
| Levels | peak 0.0 dBFS, mean −33.9 dB |

Audio and video share one lamina with no change to storage. The declared
`max_bitrate` covers both. The peak at 0 dBFS means the microphone gain is too
high and loud sounds clip.

Recording audio in public places is restricted by law in some jurisdictions
(e.g. Korea's Personal Information Protection Act for CCTV), so producers
should be able to turn audio off per camera.

## Load and temperature

16 minutes: 1 min idle; 12 min encoding 1080p30 at 4 Mbps with AAC from the
camera, output discarded; 3 min cooldown. Sampled every 5 s.

| Phase | SoC temperature | ffmpeg CPU | ARM clock | Throttled |
|---|---|---:|---|---|
| Idle | 37.0–38.9 °C | — | 600 MHz | no |
| Encoding | 41.8 → 49.6 °C; **48.4 °C average over the last 5 min** | 161% | 1.8 GHz held | no (`0x0`) |
| Cooldown | 44.3 → 39.9 °C | — | 600 MHz | no |

- Temperature rises for about five minutes, then holds. It stays ~30 °C below
  the 80 °C soft throttling point.
- A Pi 400 needs no extra cooling for this load. Its SoC sits under a large
  aluminium plate inside the keyboard. A bare Pi 4 board in an enclosure runs
  hotter and should be measured the same way before deployment.

## Caveats

- Neither scene moved much, and nothing crossed the view during the long
  runs. Low bitrates degrade first on motion, so bitrate choices and the
  episode rule need a run at a busy hour. For judging quality under motion,
  any clip of similar content at the same resolution fed through the same
  encoder works. For load and temperature, only the real camera path counts,
  because MJPEG decoding dominates the CPU.
- The daybreak recordings were made one after another over three minutes
  while the light was still changing, so small differences between them are
  partly the sky.
- One frame per recording was compared, 8 s in. Frames between keyframes (every
  2 s here) can look worse.
- 640×480 is a 4:3 mode with a different field of view.
- The hardware encoder was only driven through ffmpeg, where it is
  constant-rate. Whether its variable-bitrate mode can be reached another
  way (a V4L2 control on the encoder's own file descriptor) was not tried.
