.PHONY: tidy build

tidy:
	go mod tidy

build:
	go build ./...
