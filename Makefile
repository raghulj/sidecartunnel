# sidecartunnel — build and check targets.
#
# `check` is the default goal because the thing you want by reflex is the thing that
# should happen when you type `make` with no argument.

BINARY      := sidecartunnel
PKG         := ./cmd/sidecartunnel
COVERAGE    := coverage.out
REDIS_NAME  := sidecartunnel-redis
REDIS_PORT  ?= 6379
REDIS_IMAGE ?= redis:7-alpine

.DEFAULT_GOAL := check
.PHONY: check test cover lint build redis redis-stop clean tidy

## check: lint, test and the coverage gate. What CI runs, and what to run before pushing.
check: lint test cover

## test: unit and protocol tests with the race detector.
#
# -race is not optional on any layer (docs/11-testing.md §7). The concurrency rules in
# docs/09-internals.md §4 are exactly the kind of thing that passes review and fails under
# the detector.
test:
	go test -race -cover ./...

## cover: enforce 100% statement coverage per package and print the table.
#
# The gate lives in scripts/cover.sh so that CI and a laptop run the identical check.
# Uncovered lines are justified in place with a "// coverage: <reason>" comment, never in
# a list here — see docs/14-coding-standards.md §3.
cover:
	./scripts/cover.sh $(COVERAGE)

## lint: golangci-lint with the repository configuration.
lint:
	golangci-lint run ./...

## build: the gateway binary, statically linked.
#
# Identical flags to the release Dockerfile in docs/10-operations.md §1, so that what you
# test locally is what ships. NFR-6 wants the image under 20 MB.
build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/$(BINARY) $(PKG)

## redis: a throwaway Redis for the integration tests.
#
# --rm and a fixed name, so a second run replaces the first rather than failing on a port
# clash. Nothing here is persisted and nothing should be: the bus holds no state
# (docs/02-architecture.md).
redis:
	@docker rm -f $(REDIS_NAME) >/dev/null 2>&1 || true
	docker run --rm -d --name $(REDIS_NAME) -p $(REDIS_PORT):6379 $(REDIS_IMAGE) \
		redis-server --client-output-buffer-limit "pubsub 256mb 64mb 60"
	@echo "redis on :$(REDIS_PORT) — ST_BUS__URL=redis://localhost:$(REDIS_PORT)/0"

## redis-stop: remove the throwaway Redis.
redis-stop:
	-docker rm -f $(REDIS_NAME)

## tidy: refresh go.mod and go.sum.
tidy:
	go mod tidy

## clean: remove build and coverage artifacts.
clean:
	rm -rf bin $(COVERAGE)
