# Two steps, as in lesomnus/flob:
#
#   build    the Dockerfile on one platform, cross-compiling both binaries,
#            out to ./output/{amd64,arm64}
#   package  ./output wrapped into the image for each platform: a COPY onto
#            distroless, so the arm64 image costs no compiler and no QEMU
#
#   docker buildx bake build
#   docker buildx bake package                          # both platforms, local cache only
#   docker buildx bake package --load \
#     --set package.platform=linux/amd64                # into the local daemon
#   TAG=edge docker buildx bake package --push          # what CI does on main
group "default" {
  targets = ["build"]
}

variable "REPO" {
  default = "ghcr.io/lesomnus/shale"
}
variable "TAG" {
  default = "local"
}
# The commit, for the image's revision label and its immutable tag.
variable "BUILD_HASH" {
  default = "0000000000000000000000000000000000000000"
}
# What `shale version` and every heartbeat report (internal/hostagent).
variable "VERSION" {
  default = "dev"
}

target "build" {
  context    = "."
  dockerfile = "./Dockerfile"
  target     = "binaries"
  platforms  = ["linux/amd64"]
  args = {
    VERSION = VERSION
  }
  output = [{ type = "local", dest = "./output" }]
}

target "package" {
  context   = "./output"
  platforms = ["linux/amd64", "linux/arm64"]

  # Root rather than nonroot: the state directory and the sinks are volumes
  # and bind mounts whose ownership the image cannot know, and a node that
  # reads SMART needs the devices (§34.8). Drop to a user with the mounts
  # prepared for it where that matters.
  dockerfile-inline = <<EOT
FROM gcr.io/distroless/static-debian12
ARG TARGETARCH
COPY "./$${TARGETARCH}" /usr/bin/shale
# State and sinks are volumes (§34.8); the working directory holds nothing.
VOLUME ["/var/lib/shale"]
ENTRYPOINT ["/usr/bin/shale"]
CMD ["--config", "/etc/shale/shale.yaml", "serve", "all"]
EOT

  labels = {
    "org.opencontainers.image.source"   = "https://github.com/lesomnus/shale"
    "org.opencontainers.image.revision" = BUILD_HASH
    "org.opencontainers.image.version"  = VERSION
  }
  tags = [
    "${REPO}:${TAG}",
    "${REPO}:${BUILD_HASH}",
  ]
}
