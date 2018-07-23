#!/bin/sh
set -e
apk add --update go git mercurial build-base ca-certificates
mkdir -p /go/src/github.com/nonspecialist
cp -r /src /go/src/github.com/nonspecialist/logspout-logstash-k8s
cd /go/src/github.com/nonspecialist/logspout-logstash-k8s
export GOPATH=/go
echo Building version $1
go get
go build -ldflags "-X main.Version=$1" -o /bin/logspout
echo Cleanup
apk del go git mercurial build-base
rm -rf /go
rm -rf /var/cache/apk/*

# backwards compatibility
ln -fs /tmp/docker.sock /var/run/docker.sock
