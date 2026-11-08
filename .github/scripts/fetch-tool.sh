#!/bin/sh
# Download one pinned CI executable. Retry transport failures, never validation.
set -eu
if [ "$#" -ne 3 ]; then
  echo 'usage: fetch-tool.sh https-url sha256 destination' >&2
  exit 2
fi
url=$1
expected=$2
destination=$3
case "$url" in https://*) ;; *) echo 'tool URL must use HTTPS' >&2; exit 2 ;; esac
case "$expected" in *[!0-9a-f]*|'') echo 'invalid SHA256' >&2; exit 2 ;; esac
[ "${#expected}" -eq 64 ] || { echo 'invalid SHA256 length' >&2; exit 2; }
mkdir -p "$(dirname "$destination")"
if [ -f "$destination" ] && [ "$(sha256sum "$destination" | cut -d ' ' -f 1)" = "$expected" ]; then
  chmod 0755 "$destination"
  exit 0
fi
partial=$(mktemp "${destination}.download.XXXXXX")
trap 'rm -f "$partial"' EXIT HUP INT TERM
# Authenticated release API requests avoid shared anonymous download throttling.
# Never attach the workflow token to arbitrary download hosts or redirect targets.
set --
case "$url" in
  https://api.github.com/repos/*)
    if [ -n "${GH_TOKEN:-}" ]; then
      set -- --header "Authorization: Bearer $GH_TOKEN"
    fi
    ;;
esac
curl --fail --location --silent --show-error --proto '=https' --proto-redir '=https' \
  --header 'Accept: application/octet-stream' \
  --connect-timeout 10 --max-time 60 --retry 4 --retry-delay 2 --retry-max-time 180 \
  "$@" \
  --output "$partial" "$url"
printf '%s  %s\n' "$expected" "$partial" | sha256sum --check --strict
chmod 0755 "$partial"
mv -f "$partial" "$destination"
