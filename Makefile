.PHONY: proto build up down demo test load-test docker-up docker-down load

proto:
	PATH="$$(go env GOPATH)/bin:$$PATH" go run github.com/bufbuild/buf/cmd/buf@v1.50.0 generate

build:
	go build -o bin/kv-node ./cmd/node
	go build -o bin/kvctl ./cmd/client

test:
	go test ./...

up:
	docker compose up --build

down:
	docker compose down

demo:
	bash ./scripts/demo.sh

load-test:
	go run ./cmd/client -- load-test --writes 1000 --reads 1000

docker-up: up

docker-down: down

load:
	go run ./cmd/client -- status
	go run ./cmd/client -- load-test --writes 1000 --reads 1000
