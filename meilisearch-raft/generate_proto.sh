#!/usr/bin/env bash

push () {
    pushd $1 >/dev/null 2>&1
}

pop () {
    popd $1 >/dev/null 2>&1
}

mkdir -p src/proto

push proto

protoc --rust_out=../src/proto indexrpcpb.proto
protoc -I. --rust_out=../src/proto --grpc_out=../src/proto --plugin=protoc-gen-grpc=`which grpc_rust_plugin` indexpb.proto

pop
