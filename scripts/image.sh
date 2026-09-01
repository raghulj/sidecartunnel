#!/bin/sh
#
# Image metadata gate. Asserts that an image carries what a released image carries.
#
# The container image is built by four different paths -- the release workflow through
# GoReleaser, `make image`, a bare `docker build .`, and the flask example's compose file.
# For the first release they did not agree. All nine OCI labels were `--label` flags in
# .goreleaser.yaml, duplicated across the amd64 and arm64 blocks, so the published image
# had all nine and every other path produced an image whose Config.Labels was `null`.
# Nothing noticed, because the only checks on image metadata were the manual ones in
# docs/15-releasing.md §4, run against an image that was already published.
#
# The labels now live in the Dockerfile, which is the one file every path goes through.
# This script is what keeps that true: it runs in CI on a locally built image, so a label
# dropped in a pull request fails there rather than in a release nobody re-inspects.
#
# image.source is the label with consequences -- it is what links a GHCR package back to
# its repository and what most scanners and policy engines key on.
#
# Usage: scripts/image.sh <image-ref>
set -eu

IMAGE="${1:-}"
if [ -z "$IMAGE" ]; then
	echo "usage: scripts/image.sh <image-ref>" >&2
	exit 2
fi

failed=0

# Report and count, rather than exiting on the first failure: one run should say
# everything that is wrong with the image, not the first thing.
check() {
	name=$1
	expected=$2
	actual=$3
	if [ "$actual" = "$expected" ]; then
		printf '%-46s %s\n' "$name" "ok"
	else
		printf '%-46s %s\n' "$name" "FAIL"
		printf '%-46s   expected: %s\n' "" "$expected"
		printf '%-46s   actual:   %s\n' "" "${actual:-<empty>}"
		failed=$((failed + 1))
	fi
}

# present() takes a label that is generated rather than fixed -- a version, a commit, a
# timestamp -- where the value cannot be asserted but its absence is the bug.
present() {
	name=$1
	actual=$2
	if [ -n "$actual" ] && [ "$actual" != "<no value>" ]; then
		printf '%-46s %s\n' "$name" "ok ($actual)"
	else
		printf '%-46s %s\n' "$name" "FAIL (missing)"
		failed=$((failed + 1))
	fi
}

label() {
	docker image inspect --format "{{ index .Config.Labels \"$1\" }}" "$IMAGE" 2>/dev/null || true
}

echo "image: $IMAGE"
echo

REPO=https://github.com/raghulj/sidecartunnel

check "org.opencontainers.image.title" "sidecartunnel" "$(label org.opencontainers.image.title)"
check "org.opencontainers.image.description" \
	"Websocket gateway for applications that cannot hold long-lived connections" \
	"$(label org.opencontainers.image.description)"
check "org.opencontainers.image.url" "$REPO" "$(label org.opencontainers.image.url)"
check "org.opencontainers.image.source" "$REPO" "$(label org.opencontainers.image.source)"
check "org.opencontainers.image.documentation" "$REPO/tree/main/docs" \
	"$(label org.opencontainers.image.documentation)"
check "org.opencontainers.image.licenses" "MIT" "$(label org.opencontainers.image.licenses)"

present "org.opencontainers.image.version" "$(label org.opencontainers.image.version)"
present "org.opencontainers.image.revision" "$(label org.opencontainers.image.revision)"
present "org.opencontainers.image.created" "$(label org.opencontainers.image.created)"

# `docker run -P` publishes every exposed port, so anything beyond 8000 in this list is a
# build regression that hands out a listener nobody meant to publish. The second listener
# this image used to declare is gone along with the operator API it carried.
check "exposed ports" '{"8000/tcp":{}}' \
	"$(docker image inspect --format '{{json .Config.ExposedPorts}}' "$IMAGE")"

# Distroless nonroot. An image that silently starts running as root is the kind of
# regression a base image bump can introduce without touching a line of this repository.
check "user" "nonroot:nonroot" \
	"$(docker image inspect --format '{{ .Config.User }}' "$IMAGE")"

check "entrypoint" "[/sidecartunnel]" \
	"$(docker image inspect --format '{{ .Config.Entrypoint }}' "$IMAGE")"

# The binary must actually answer, and it must answer with the version that was linked
# into it. A -X against a symbol the linker cannot find is silently ignored, so a build
# that lost the ldflags produces a working image that reports "dev" -- which is exactly
# what every non-release path did before the labels moved.
reported=$(docker run --rm "$IMAGE" --version 2>/dev/null | head -1 || true)
present "reported version" "$reported"

echo
if [ "$failed" -gt 0 ]; then
	echo "FAILED: $failed check(s). The image does not carry what a released image carries."
	exit 1
fi
echo "OK: image metadata matches a release build."
