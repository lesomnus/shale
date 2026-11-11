# Synthetic media/process qualification in a disposable, externally disconnected
# container. Application roles subsequently drop every Linux capability.
FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 iproute2 dnsmasq-base busybox util-linux ffmpeg ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY *.py /test/
COPY e2e.test /test/e2e.test
WORKDIR /test
ENV SHALE_CAMERA_LAN_NETNS_TEST=1
