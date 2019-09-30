#!/bin/bash

GIT_TAG="v1.2.0"

go get -d -u github.com/golang/protobuf/protoc-gen-go
git -C "$(go env GOPATH)"/src/github.com/golang/protobuf checkout $GIT_TAG
go install github.com/golang/protobuf/protoc-gen-go
go install github.com/golang/protobuf/proto