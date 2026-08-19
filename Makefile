.PHONY: build run test fmt tidy compose-up compose-down verify clean

build:
	go build -o bin/engine ./cmd/engine

run: build
	./bin/engine -config configs/config.yaml

test:
	go test ./...

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './vendor/*')

tidy:
	go mod tidy

compose-up:
	docker compose up -d postgres redis

compose-down:
	docker compose down

verify: build
	curl -fsS http://localhost:8080/healthz

clean:
	rm -rf bin
