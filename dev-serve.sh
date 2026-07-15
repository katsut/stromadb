#!/usr/bin/env bash
# Local dev launcher for stroma-serve: source .env (STROMA_* config) then run the release binary.
set -euo pipefail
cd "$(dirname "$0")"
if [ -f .env ]; then
  set -a
  . ./.env
  set +a
fi
exec ./target/release/stroma-serve
