#!/bin/sh
set -eu

server_pid=""
cleanup() {
  if [ -n "${server_pid}" ] && kill -0 "${server_pid}" 2>/dev/null; then
    kill "${server_pid}"
    wait "${server_pid}" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

python -m growthbook_collector.server &
server_pid="$!"

attempt=1
while [ "${attempt}" -le 20 ]; do
  if health_json="$(curl -fsS http://127.0.0.1:8080/health 2>/dev/null)"; then
    break
  fi
  if ! kill -0 "${server_pid}" 2>/dev/null; then
    echo "COLLECTOR_HOST_GATE_SERVER_EXITED" >&2
    exit 1
  fi
  attempt=$((attempt + 1))
  sleep 1
done

if [ "${attempt}" -gt 20 ]; then
  echo "COLLECTOR_HOST_GATE_HEALTH_TIMEOUT" >&2
  exit 1
fi

marker_json="$(curl -fsS http://127.0.0.1:8080/marker.json)"
export health_json marker_json
python - <<'PY'
import json
import os

health = json.loads(os.environ["health_json"])
marker = json.loads(os.environ["marker_json"])
expected_version = os.environ["GROWTHBOOK_COLLECTOR_VERSION"]
expected_environment = os.environ["GROWTHBOOK_ENVIRONMENT"]

assert health == {
    "environment": expected_environment,
    "marker": "VEVO_GROWTHBOOK_COLLECTOR_HEALTH",
    "ok": True,
    "version": expected_version,
}
assert marker == {
    "environment": expected_environment,
    "marker": "VEVO_GROWTHBOOK_COLLECTOR_HOST_OK",
    "runtime_path": "/app",
    "version": expected_version,
}
print(f"COLLECTOR_LOCALHOST_HEALTH_OK:{expected_environment}:{expected_version}")
print(f"COLLECTOR_LOCALHOST_MARKER_OK:/app:{expected_version}")
PY
