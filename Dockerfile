# The binaries, not the image (§34.8). Every role runs from the one static
# binary:
#   shale serve control|cluster|storage|producer|reader|relay|all
# The console (§40) is built into it: the control plane serves it at the
# root of its tenant HTTP listener.
#
# Everything here runs on the builder's own platform and cross-compiles: the
# binary is CGO_ENABLED=0 and the console is JavaScript, so neither needs to
# run on the target. The last stage is the two binaries and nothing else,
# `/amd64` and `/arm64`; docker-bake.hcl takes them out (`build`) and wraps
# them into the multi-platform image (`package`) with no compiler and no
# emulation:
#
#   docker buildx bake build      # ./output/{amd64,arm64}
#   docker buildx bake package    # ghcr.io/lesomnus/shale, both platforms
FROM --platform=$BUILDPLATFORM node:24-bookworm-slim AS console
WORKDIR /src/ts
COPY ts/package.json ts/package-lock.json ./
COPY ts/vendor ./vendor
RUN npm ci --no-audit --no-fund
COPY ts .
RUN npm run build

FROM --platform=$BUILDPLATFORM golang:1.27 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
COPY --from=console /src/web/console/dist ./web/console/dist
# What heartbeats report (internal/hostagent) and `shale version` prints
# (payday/version). The context has no .git, so the toolchain stamps no
# revision; the image's revision label carries it instead (docker-bake.hcl).
ARG VERSION=dev
ENV CGO_ENABLED=0 GOOS=linux
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    mkdir /dist \
    && for arch in amd64 arm64; do \
         GOARCH=$arch go build -tags grpcnotrace -trimpath \
           -ldflags="-s -w \
             -X github.com/lesomnus/shale/internal/hostagent.Version=${VERSION} \
             -X github.com/lesomnus/payday/version.version=${VERSION}" \
           -o /dist/$arch ./cmd/shale || exit 1; \
       done
# The one this builder can run says the build is a working binary with the
# version in it; the other is the same source for another GOARCH.
RUN /dist/$(go env GOHOSTARCH) version

FROM scratch AS binaries
COPY --from=builder /dist /
