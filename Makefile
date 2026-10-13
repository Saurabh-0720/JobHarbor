.PHONY: run test build
run:   ; go run ./cmd/jobharbor
test:  ; go test -race ./...
build: ; go build -o bin/jobharbor ./cmd/jobharbor
