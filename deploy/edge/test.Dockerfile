# An explicitly invoked, isolated protocol fixture; never runs on a host LAN.
FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 iproute2 dnsmasq-base busybox util-linux && rm -rf /var/lib/apt/lists/*
COPY . /test
WORKDIR /test
ENV SHALE_CAMERA_LAN_NETNS_TEST=1
CMD ["python3", "namespace_test.py"]
