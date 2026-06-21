ADDR  ?= :8080
NODES ?= 3
SEED  ?= 1

.PHONY: all build build-ui run dev dev-ui test test-race fuzz fmt vet clean

all: build

## build-ui: install deps and produce webui/dist (embedded by the Go binary)
build-ui:
	cd webui && npm install && npm run build

## build: build the self-contained demo binary (UI embedded)
build: build-ui
	go build -o bin/raftdemo ./cmd/raftdemo

## run: build then serve the visualizer at $(ADDR)
run: build
	./bin/raftdemo -addr $(ADDR) -nodes $(NODES) -seed $(SEED)

## dev: run the Go backend (serves /ws); pair with `make dev-ui`
dev:
	go run ./cmd/raftdemo -addr $(ADDR) -nodes $(NODES) -seed $(SEED)

## dev-ui: run the Vite dev server (http://localhost:5173) with hot reload
dev-ui:
	cd webui && npm install && npm run dev

## test: run the full Go test suite
test:
	go test ./...

## test-race: run tests with the race detector
test-race:
	go test -race ./...

## fuzz: run the randomized safety-invariant scenarios
fuzz:
	go test ./sim/ -run TestFuzz -v

fmt:
	gofmt -w raft sim metrics server cmd kvstore

vet:
	go vet ./...

clean:
	rm -rf bin webui/dist/assets
