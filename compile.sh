#!/bin/sh
export GOPATH=$(pwd)
go build src/kvpaxos/client.go
mv client bin/client
go build src/kvpaxos/kvpaxos.go
mv kvpaxos bin/kvpaxos

