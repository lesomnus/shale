#!/bin/sh
# Builds a Debian package of Shale (§34.6, §52): the static binary, the
# systemd template unit, an example configuration, and a postinst that
# makes the service user and the directories. No build dependencies beyond
# Go and dpkg-deb.
#
#   deploy/deb/build.sh [version] [arch]      # e.g. 0.1.0 amd64
#
# Every role is an instance of the one unit: shale@control, shale@cluster,
# shale@storage, shale@producer, shale@reader, shale@relay, or shale@all.
set -eu
cd "$(dirname "$0")/../.."
VERSION=${1:-0.0.0+$(git rev-parse --short HEAD)}
ARCH=${2:-amd64}
case "$ARCH" in
  amd64) GOARCH=amd64 ;;
  arm64) GOARCH=arm64 ;;
  *) echo "arch: amd64 or arm64" >&2; exit 2 ;;
esac
OUT=${OUT:-dist}
PKG="$OUT/shale_${VERSION}_${ARCH}"
rm -rf "$PKG"
mkdir -p "$PKG/DEBIAN" "$PKG/usr/bin" "$PKG/lib/systemd/system" "$PKG/etc/shale" "$PKG/usr/share/doc/shale"

CGO_ENABLED=0 GOOS=linux GOARCH=$GOARCH go build -tags grpcnotrace -trimpath \
  -ldflags="-s -w -X github.com/lesomnus/shale/internal/hostagent.Version=$VERSION" \
  -o "$PKG/usr/bin/shale" ./cmd/shale
install -m 0644 deploy/systemd/shale@.service "$PKG/lib/systemd/system/shale@.service"
install -m 0644 deploy/deb/shale.yaml.example "$PKG/etc/shale/shale.yaml.example"
install -m 0644 deploy/systemd/README.md "$PKG/usr/share/doc/shale/README.systemd.md"
install -m 0644 README.md "$PKG/usr/share/doc/shale/README.md"

SIZE=$(du -sk "$PKG" | cut -f1)
cat > "$PKG/DEBIAN/control" <<CONTROL
Package: shale
Version: $VERSION
Section: net
Priority: optional
Architecture: $ARCH
Maintainer: lesomnus <lesomnus@gmail.com>
Installed-Size: $SIZE
Depends: adduser
Recommends: ffmpeg
Description: loss-tolerant object store for CCTV
 Shale records camera streams as laminae on plain HDDs and plays them
 live. One binary serves every role: the control plane, a storage node,
 a producer beside the cameras, a relay for live viewing, a reader.
 Each role is an instance of the shale@ systemd unit.
CONTROL
install -m 0755 deploy/deb/postinst "$PKG/DEBIAN/postinst"
install -m 0755 deploy/deb/prerm "$PKG/DEBIAN/prerm"
echo "/etc/shale/shale.yaml.example" > "$PKG/DEBIAN/conffiles"

dpkg-deb --build --root-owner-group "$PKG" "$PKG.deb" >/dev/null
rm -rf "$PKG"
echo "$PKG.deb"
