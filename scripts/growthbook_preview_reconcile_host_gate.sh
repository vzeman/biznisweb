#!/usr/bin/env bash
set -euo pipefail

if [[ "${GROWTHBOOK_ENVIRONMENT:-}" != "preview" ]]; then
  echo "Legacy Preview host-gate entrypoint requires preview" >&2
  exit 1
fi

exec /bin/bash "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/growthbook_reconcile_host_gate.sh"
