.PHONY: build test test-unit lint dev-up dev-down run stop bench-audio bench-queries load chaos k8s-validate clean

build:
	go build -o bin/ ./cmd/...

test-unit:
	go test -short -race ./...

# Integration tests need Redis and Postgres: make dev-up
test: dev-up
	go test -race -count=1 ./...

lint:
	gofmt -l . | (! grep .)
	go vet ./...

dev-up:
	@scripts/dev-up.sh >/dev/null

dev-down:
	@scripts/dev-down.sh

run: dev-up
	scripts/run-local.sh start 4

stop:
	scripts/run-local.sh stop

bench-audio:
	scripts/bench-audio.sh 8

bench-queries: dev-up
	scripts/bench-queries.sh

load: dev-up
	scripts/load-test.sh

chaos: dev-up
	scripts/chaos.sh

k8s-validate:
	scripts/k8s-validate.sh

clean:
	rm -rf bin bench/out
