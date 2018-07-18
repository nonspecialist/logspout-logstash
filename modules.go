package main

import (
	_ "github.com/gliderlabs/logspout/transports/tcp"
	_ "github.com/gliderlabs/logspout/transports/udp"
	_ "github.com/nonspecialst/logspout-logstash-k8s"
)
