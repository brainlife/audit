#!/bin/bash
set -e
set -x

tag=1.0.3

docker build -t soichih/audit ..
docker tag soichih/audit soichih/audit:$tag
docker push soichih/audit:$tag
