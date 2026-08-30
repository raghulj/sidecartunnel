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

# Version metadata. GoReleaser and the release workflow pass these; a bare `docker build .`
# gets the "dev" defaults rather than failing.
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

USER nonroot:nonroot

# The websocket listener, and only the websocket listener. `EXPOSE 9001` is deliberately
# absent: `docker run -P` publishes every exposed port, and admin.listen defaults to
# loopback precisely so it cannot be reached from outside (docs/10-operations.md §1).
EXPOSE 8000

# The binary checks itself. There is no shell and no curl in this image, and a
# bus-dependent healthcheck would kill the whole fleet during a Redis restart, so the
# subcommand tests liveness only (docs/08-config.md §1, docs/04-integration.md §4).
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD ["/sidecartunnel", "healthcheck"]

ENTRYPOINT ["/sidecartunnel"]
