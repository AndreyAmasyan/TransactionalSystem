.PHONY: run 

run: 

	go run cmd/server/main.go

.PHONY: build

build: 

	go build -v cmd/server/main.go

.PHONY: test 

test: 

	go test -v -race -timeout 30s ./...

.DEFAULT_GOAL := build