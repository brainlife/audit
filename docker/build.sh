#!/bin/bash
set -e
set -x

tag=1.0.2

docker build -t soichih/audit ..
docker tag soichih/audit soichih/audit:$tag
docker push soichih/audit:$tag
