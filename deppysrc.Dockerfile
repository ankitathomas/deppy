FROM golang:1.17.6-alpine as Build

COPY . . 

RUN GOPATH= CGO_ENABLED=0 go build -o catalog_source_controller internal/source/adapter/catalogsource/cmd/cmd.go

EXPOSE 50052

CMD ["./catalog_source_controller"]
