# The image holds the static binary only (§34.8). Every role runs from it:
#   shale serve control|cluster|storage|producer|reader|relay|all
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
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
