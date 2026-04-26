.PHONY: run build fmt test check docker docker-run lint tidy migrate worker enqueue stats compose-up compose-down

run:
	go run ./cmd/tinybus

build:
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/tinybus ./cmd/tinybus

fmt:
	go fmt ./...

test:
	go test -race ./...

# One-shot CI gate: format, vet, and race-tested unit/integration tests.
check: fmt lint test

lint:
	go vet ./...

tidy:
	go mod tidy

docker:
	docker build -t tinybus:dev .

docker-run:
	docker run --rm -e DATABASE_URL=$$DATABASE_URL tinybus:dev worker --queue=default

compose-up:
	docker compose up --build

compose-down:
	docker compose down -v

# Convenience CLI shortcuts (require DATABASE_URL set in env)
migrate:
	go run ./cmd/tinybus migrate up

worker:
	go run ./cmd/tinybus worker --queue=default

enqueue:
	go run ./cmd/tinybus enqueue --queue=default --payload='{"hello":"world"}'

stats:
	go run ./cmd/tinybus stats
