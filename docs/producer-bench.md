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
| Scene | night street through a window: low light, heavy sensor noise, almost no motion |
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

## Audio in the same segment

| | |
|---|---|
| Streams | H.264 1280×720 30 fps at 2 Mbps + AAC mono 48 kHz at 64 kbps, one MPEG-TS |
| Total | 2.19 Mbps (+3%) |
| Start offset between streams | 21 ms |
| Levels | peak 0.0 dBFS, mean −33.9 dB |

Audio and video share one object with no change to storage. The declared
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

- The scene barely moved. Low bitrates degrade first on motion, so bitrate
  choices need a re-run with people or vehicles in view. For judging quality
  under motion, any clip of similar content at the same resolution fed through
  the same encoder works. For load and temperature, only the real camera path
  counts, because MJPEG decoding dominates the CPU.
- One frame per recording was compared, 8 s in. Frames between keyframes (every
  2 s here) can look worse.
- 640×480 is a 4:3 mode with a different field of view.
- Variable-bitrate modes of the hardware encoder were not measured.
