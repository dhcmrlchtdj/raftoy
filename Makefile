SHELL := bash

build: gen
	go run -race ./main.go

gen: rpc/rpc.pb.go rpc/rpc_grpc.pb.go

rpc/rpc.pb.go: rpc/rpc.proto
	protoc --go_out=. --go_opt=paths=source_relative $<

rpc/rpc_grpc.pb.go: rpc/rpc.proto
	protoc --go-grpc_out=. --go-grpc_opt=paths=source_relative $<
