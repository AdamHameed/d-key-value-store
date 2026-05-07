#!/usr/bin/env bash
set -euo pipefail

WRITES="${WRITES:-1000}"
READS="${READS:-1000}"

go run ./cmd/client -- load-test --writes "$WRITES" --reads "$READS"
