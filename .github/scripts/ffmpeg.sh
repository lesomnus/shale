#!/bin/sh
# Install the pinned static FFmpeg build's ffmpeg and ffprobe into $1 and put
# it on PATH. A prebuilt from BtbN/FFmpeg-Builds' month-end release, which is
# kept, fetched through the release API and checked against its SHA-256; no
# distribution mirror is involved. A cached directory from an earlier run of
# the same pin is used as it is.
set -eu
if [ "$#" -ne 1 ]; then
  echo 'usage: ffmpeg.sh directory' >&2
  exit 2
fi
dir=$1
# autobuild-2026-10-31-12-59, ffmpeg-n8.1.3-27-g8dca1586f4-linux64-gpl-8.1.tar.xz
name=ffmpeg-n8.1.3-27-g8dca1586f4-linux64-gpl-8.1
asset=https://api.github.com/repos/BtbN/FFmpeg-Builds/releases/assets/695398022
sha256=41e9c565594c390622cf968d82fa4135d9e9a11d06c1515644288cbc844e3380
if [ ! -x "$dir/ffmpeg" ] || [ ! -x "$dir/ffprobe" ]; then
  sh "$(dirname "$0")/fetch-tool.sh" "$asset" "$sha256" "$dir/$name.tar.xz"
  tar -xJf "$dir/$name.tar.xz" -C "$dir" --strip-components=2 "$name/bin/ffmpeg" "$name/bin/ffprobe"
  rm -f "$dir/$name.tar.xz"
fi
echo "$dir" >> "${GITHUB_PATH:-/dev/null}"
"$dir/ffmpeg" -hide_banner -version | head -n 1
