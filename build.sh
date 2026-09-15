#!/bin/sh
# Builds both binaries, stamping the current UTC time into them
# (pkg/buildinfo.BuildTime) so the running build is identifiable.
set -ex
export PATH="${PATH}:/usr/local/go/bin"
BUILD_TIME="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
LDFLAGS="-X omp-vk-gateway/pkg/buildinfo.BuildTime=${BUILD_TIME}"
go build -ldflags "${LDFLAGS}" -o omp-agent ./cmd/vk-gateway
go build -ldflags "${LDFLAGS}" -o omp-agent-restarter ./cmd/vk-gateway-restarter
echo "built: omp-agent, omp-agent-restarter (build time ${BUILD_TIME})"
