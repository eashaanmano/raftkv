.PHONY: deps build test test-short race jepsen jepsen-negative bench bench-failover docker-up docker-down

deps:            ## fetch gRPC and write go.sum
	go mod tidy

build: deps
	go build -o bin/ ./cmd/...

test-short:      ## fast: unit tests + 8s fault runs
	go test -short ./...

test:            ## unit tests + 30s fault-injection runs
	go test ./...

race:
	go test -race ./...

jepsen:          ## 10 x 60s fault-injection runs, 5-node group, fsynced WAL
	go run ./cmd/raftkv-jepsen -runs 10 -duration 60s -nodes 5

jepsen-negative: ## must FAIL: reads skip ReadIndex, checker should catch stale reads
	go run ./cmd/raftkv-jepsen -runs 1 -duration 15s -unsafe-local-reads -faults partition,isolate-leader,lossy-net || echo "violation detected, as intended"

bench:           ## 5 real gRPC servers on loopback, fsynced WAL
	go run ./cmd/raftkv-bench -mode grpc-local -nodes 5 -clients 128 -duration 20s

bench-failover:
	go run ./cmd/raftkv-bench -mode grpc-local -nodes 5 -clients 64 -duration 20s -kill-leader-at 10s

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down -v
