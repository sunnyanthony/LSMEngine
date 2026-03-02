#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${LSM_DOCKER_TEST_IMAGE:-golang:1.21}"
MODE="${LSM_DOCKER_TEST_MODE:-run}" # run|build
TAGS="${LSM_DOCKER_TEST_TAGS:-test}"
PKGS="${LSM_DOCKER_TEST_PKGS:-./...}"
GO_BIN="${LSM_DOCKER_TEST_GO_BIN:-/usr/local/go/bin/go}"
CACHE_ROOT="${LSM_DOCKER_TEST_CACHE_DIR:-$ROOT_DIR/.cache/docker-go}"
MOD_CACHE_DIR="$CACHE_ROOT/mod"
BUILD_CACHE_DIR="$CACHE_ROOT/build"

mkdir -p "$MOD_CACHE_DIR" "$BUILD_CACHE_DIR"

if [[ "$MODE" == "build" ]]; then
  docker build --progress=plain -f docker/Dockerfile.test -t lsmengine-test "$ROOT_DIR"
  exit 0
fi

docker run --rm \
  -v "$ROOT_DIR":/workspace \
  -v "$MOD_CACHE_DIR":/go/pkg/mod \
  -v "$BUILD_CACHE_DIR":/tmp/go-build \
  -w /workspace \
  -e CGO_ENABLED=1 \
  -e GOCACHE=/tmp/go-build \
  "$IMAGE" \
  /bin/bash -lc "${GO_BIN} test -v -tags ${TAGS} ${PKGS}"
