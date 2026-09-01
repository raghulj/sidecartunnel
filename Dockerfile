# syntax=docker/dockerfile:1

# sidecartunnel release image — docs/10-operations.md §1.
#
# Single static binary on distroless static, non-root, no shell. NFR-6 targets under
# 20 MB, and the binary is nearly all of it.

# --platform=$BUILDPLATFORM keeps the compiler running natively and cross-compiles with
# GOOS/GOARCH, rather than emulating the target under QEMU — the difference between a
# multi-arch release taking one minute and taking twenty.
FROM --platform=${BUILDPLATFORM} golang:1.26-alpine AS build

WORKDIR /src

# Dependencies in their own layer, so a source-only change does not re-download the
# module cache.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Version metadata. GoReleaser, `make image` and a bare `docker build .` all pass these
# the same way; the defaults are what an unadorned `docker build .` gets, and they are
# honest rather than absent.
ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown
ARG TARGETOS=linux
ARG TARGETARCH

# CGO_ENABLED=0 is what makes the binary runnable on a base image with no libc at all.
# -trimpath keeps the build reproducible by stripping the builder's directory names, and
# the same flags are in the Makefile so that what is tested locally is what ships.
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build \
      -trimpath \
      -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${DATE}" \
      -o /sidecartunnel \
      ./cmd/sidecartunnel

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /sidecartunnel /sidecartunnel

# Re-declared: an ARG is scoped to the stage that declares it, and these three are needed
# again here for the labels below.
ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown

# The OCI labels live here rather than in .goreleaser.yaml, so that every path that builds
# this image produces the same metadata. They used to be nine `--label` flags duplicated
# across the amd64 and arm64 blocks of .goreleaser.yaml, which meant the release image
# carried all nine and a local `docker build .` carried none at all — `Config.Labels` came
# back `null`. `image.source` is the one with teeth: it is what links a GHCR package back
# to its repository and what most scanners key on.
#
# After VERSION, so a version bump does not invalidate the layer holding the binary.
LABEL org.opencontainers.image.title="sidecartunnel" \
      org.opencontainers.image.description="Websocket gateway for applications that cannot hold long-lived connections" \
      org.opencontainers.image.url="https://github.com/raghulj/sidecartunnel" \
      org.opencontainers.image.source="https://github.com/raghulj/sidecartunnel" \
      org.opencontainers.image.documentation="https://github.com/raghulj/sidecartunnel/tree/main/docs" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.created="${DATE}"

USER nonroot:nonroot

# One listener, carrying the websocket endpoint, GET /health and GET /ready. The second
# listener this image used to keep off the network is gone along with the operator API it
# was carrying (docs/12-roadmap.md §2).
EXPOSE 8000

# The binary checks itself, and that is the whole reason `healthcheck` is a subcommand:
# this is distroless static, with no shell and no curl in it, so the only executable
# available to probe the process is the process. Exec form, not string form — there is no
# /bin/sh here to parse a string.
#
# It performs a loopback GET /health against server.listen. Liveness only: /health never
# consults the bus, because a bus-dependent healthcheck restarts every container during a
# Redis restart and turns an eight-second blip into a full outage
# (docs/08-config.md §1, docs/10-operations.md §5).
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD ["/sidecartunnel", "healthcheck"]

ENTRYPOINT ["/sidecartunnel"]
