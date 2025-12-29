#!/usr/bin/env bash
set -euo pipefail

docker build --progress=plain --no-cache -f docker/Dockerfile.test -t lsmengine-test .
