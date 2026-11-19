GO ?= go

.PHONY: build proto lint test test-integration check up down bench

## build: compile every binary under cmd/ into bin/
build:
	@if [ -d cmd ]; then $(GO) build -o bin/ ./cmd/...; else echo "build: no binaries under cmd/ yet (first binary arrives in phase 4)"; fi

## proto: lint proto/ and regenerate gen/ from it with buf
proto:
	$(GO) tool buf lint proto
	$(GO) tool buf generate --template proto/buf.gen.yaml proto

## lint: go vet and staticcheck (shellcheck joins in phase 13)
lint:
	$(GO) vet ./...
	$(GO) tool staticcheck ./...

## test: unit tests with the race detector
test:
	$(GO) test -race ./...

## test-integration: unit tests plus the etcd-backed integration suite
test-integration:
	$(GO) test -race -tags integration ./...

## check: lint, then test; run before every commit
check: lint test

up:
	@echo "up: not implemented yet (phase 13)"

down:
	@echo "down: not implemented yet (phase 13)"

bench:
	@echo "bench: not implemented yet (phase 14)"
