.PHONY: proto build test docker-up docker-down load

proto:
	PATH="$$(go env GOPATH)/bin:$$PATH" go run github.com/bufbuild/buf/cmd/buf@v1.50.0 generate

build:
	go build ./cmd/node ./cmd/client

test:
	go test ./...

docker-up:
	docker compose up --build

docker-down:
	docker compose down

load:
	go run ./cmd/client --addr localhost:5001 status
	go run ./scripts/loadtest.go --addr localhost:5001 --n 1000
