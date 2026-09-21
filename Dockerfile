# The image holds the static binary only (§34.8). Every role runs from it:
#   shale serve control|cluster|storage|producer|reader|relay|all
# The console (§40) is built into the binary: the control plane serves it
# at the root of its tenant HTTP listener.
FROM node:24-bookworm-slim AS console
WORKDIR /src/ts
COPY ts/package.json ts/package-lock.json ./
COPY ts/vendor ./vendor
RUN npm ci --no-audit --no-fund
COPY ts .
RUN npm run build

FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=console /src/web/console/dist ./web/console/dist
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -tags grpcnotrace -trimpath \
    -ldflags="-s -w -X github.com/lesomnus/shale/internal/hostagent.Version=${VERSION}" \
    -o /out/shale ./cmd/shale

# Root rather than nonroot: the state directory and the sinks are volumes
# and bind mounts whose ownership the image cannot know, and a node that
# reads SMART needs the devices (§34.8). Drop to a user with the mounts
# prepared for it where that matters.
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/shale /usr/bin/shale
# State and sinks are volumes (§34.8); the working directory holds nothing.
VOLUME ["/var/lib/shale"]
ENTRYPOINT ["/usr/bin/shale"]
CMD ["--config", "/etc/shale/shale.yaml", "serve", "all"]
