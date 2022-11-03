IMG ?= adapter:latest
CONTAINER_RUNTIME ?= docker
BIN_DIR ?= bin

.PHONY: manifests
manifests:
	protoc -I api/ --go-grpc_out=internal/source/adapter/api/ internal/source/adapter/api/deppy_source_adapter.proto       
	protoc -I api/ --go_out=internal/source/adapter/api/ internal/source/adapter/api/deppy_source_adapter.proto

.PHONY: build
build:
	CGO_ENABLED=0 go build -o bin/catalog_source_controller internal/source/adapter/catalogsource/cmd/cmd.go

.PHONY: build-container
build-container:
	$(CONTAINER_RUNTIME) build -f deppysrc.Dockerfile -t $(IMG) .
