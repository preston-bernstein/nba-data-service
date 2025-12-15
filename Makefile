GO ?= go
CGO_ENABLED ?= 0
GOCACHE ?= $(CURDIR)/.cache/go-build
BIN_DIR ?= bin

.PHONY: build test fmt tidy run

build:
	@mkdir -p $(BIN_DIR) $(GOCACHE)
	CGO_ENABLED=$(CGO_ENABLED) GOCACHE=$(GOCACHE) $(GO) build -o $(BIN_DIR)/server ./cmd/server

test:
	@mkdir -p $(GOCACHE)
	CGO_ENABLED=$(CGO_ENABLED) GOCACHE=$(GOCACHE) $(GO) test ./...

fmt:
	$(GO) fmt ./...

tidy:
	$(GO) mod tidy

run:
	@mkdir -p $(GOCACHE)
	CGO_ENABLED=$(CGO_ENABLED) GOCACHE=$(GOCACHE) $(GO) run ./cmd/server
