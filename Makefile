GO ?= go

.PHONY: build test race lint fuzz clean

build:
	$(GO) build -o bin/ ./cmd/...

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

lint:
	golangci-lint run ./...

fuzz:
	$(GO) test ./internal/wal -run '^$$' -fuzz FuzzDecode -fuzztime 30s

clean:
	rm -rf bin
