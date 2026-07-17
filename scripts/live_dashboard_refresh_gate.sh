#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

PROJECT="${REPORT_PROJECT:?REPORT_PROJECT missing}"
LIVE_SERVER_PID=""
MARKER_PID=""

cleanup() {
  if [[ -n "${LIVE_SERVER_PID}" ]]; then
    kill "${LIVE_SERVER_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${MARKER_PID}" ]]; then
    kill "${MARKER_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

run_local_live_dashboard_smoke() {
  local local_auth_user="workflow-local-${PROJECT}"
  local local_auth_password
  local server_ready=false
  local production_api_path

  local_auth_password="$(python - <<'PY'
import secrets
print(secrets.token_urlsafe(32))
PY
  )"
  export LIVE_DASHBOARD_AUTH_USER="${local_auth_user}"
  export LIVE_DASHBOARD_AUTH_PASSWORD="${local_auth_password}"
  export REPORT_PROJECT="${PROJECT}"
  export REPORT_SKIP_PROJECT_ENV="true"

  python live_dashboard_server.py --host 127.0.0.1 --port 8080 >/tmp/live-dashboard-server.log 2>&1 &
  LIVE_SERVER_PID="$!"

  for _ in $(seq 1 60); do
    if curl -fsS http://127.0.0.1:8080/health -o /tmp/local-live-health.json; then
      server_ready=true
      break
    fi
    if ! kill -0 "${LIVE_SERVER_PID}" >/dev/null 2>&1; then
      break
    fi
    sleep 2
  done
  if [[ "${server_ready}" != "true" ]]; then
    cat /tmp/live-dashboard-server.log >&2 || true
    echo "Local live dashboard server failed its /health hard gate." >&2
    exit 1
  fi

  curl -fsS --max-time 240 \
    -u "${local_auth_user}:${local_auth_password}" \
    "http://127.0.0.1:8080/production/${PROJECT}" \
    -o /tmp/local-production-board.html
  if [[ "${PROJECT}" == "roy" ]]; then
    production_api_path="/api/operations/roy/live?refresh=1"
  else
    production_api_path="/api/production/${PROJECT}/live?refresh=1"
  fi
  curl -fsS --max-time 240 \
    -u "${local_auth_user}:${local_auth_password}" \
    "http://127.0.0.1:8080${production_api_path}" \
    -o /tmp/local-production-board-api.json
  for period in 7d 30d 90d full; do
    curl -fsS --max-time 240 \
      -u "${local_auth_user}:${local_auth_password}" \
      "http://127.0.0.1:8080/api/${PROJECT}/latest?period=${period}" \
      -o "/tmp/local-accounting-${period}.json"
    curl -fsS --max-time 240 \
      -u "${local_auth_user}:${local_auth_password}" \
      "http://127.0.0.1:8080/report/${PROJECT}?period=${period}" \
      -o "/tmp/local-accounting-${period}.html"
  done

  python - "${PROJECT}" <<'PY'
import json
import sys

project = sys.argv[1]
health = json.load(open("/tmp/local-live-health.json", encoding="utf-8"))
assert health.get("ok") is True, "Local /health response is not healthy"
assert project in (health.get("projects") or []), f"Local /health does not expose {project}"

production_html = open("/tmp/local-production-board.html", encoding="utf-8-sig").read()
production_api = json.load(open("/tmp/local-production-board-api.json", encoding="utf-8"))
expected_board_marker = "roy-operations-dashboard" if project == "roy" else f"{project}-production-board"
assert expected_board_marker in production_html, f"Local production board marker missing for {project}"
assert production_api.get("project") == project, f"Local production API served the wrong project for {project}"

for period in ("7d", "30d", "90d", "full"):
    payload = json.load(open(f"/tmp/local-accounting-{period}.json", encoding="utf-8"))
    report_html = open(f"/tmp/local-accounting-{period}.html", encoding="utf-8-sig").read()
    assert payload.get("project") == project, f"Local {period} payload served the wrong project"
    switcher = payload.get("period_switcher") or {}
    assert switcher.get("current_key") == period, f"Local {period} payload served the wrong period"
    assert len(report_html) > 1000 and "<html" in report_html.lower(), f"Local {period} HTML report is invalid"
    assert "window.__PERIOD_HREF_BASE_MAP__" in report_html, f"Local {period} live period map missing"
    for mapped_period in ("7d", "30d", "90d", "full"):
        expected_href = f"/report/{project}?period={mapped_period}"
        assert expected_href in report_html, f"Local {period} HTML is missing {expected_href}"
PY

  kill "${LIVE_SERVER_PID}" >/dev/null 2>&1 || true
  wait "${LIVE_SERVER_PID}" 2>/dev/null || true
  LIVE_SERVER_PID=""
  echo "LOCALHOST_LIVE_DASHBOARD_OK:${PROJECT}:periods=7d,30d,90d,full"
}

verify_marker_on_host() {
  python -m http.server 8000 --bind 127.0.0.1 --directory /tmp/live-dashboard-marker >/tmp/live-dashboard-marker/http.log 2>&1 &
  MARKER_PID="$!"
  echo "LIVE_ARTIFACT_MARKER_BEGIN"
  for marker_attempt in $(seq 1 10); do
    if curl -fsS http://127.0.0.1:8000/marker.json; then
      break
    fi
    if [[ "${marker_attempt}" == "10" ]]; then
      exit 1
    fi
    sleep 1
  done
  echo
  echo "LIVE_ARTIFACT_MARKER_END"
  kill "${MARKER_PID}" >/dev/null 2>&1 || true
  wait "${MARKER_PID}" 2>/dev/null || true
  MARKER_PID=""
}

if [[ "${SKIP_ARTIFACT_REFRESH:-false}" == "true" ]]; then
  echo "LIVE_ARTIFACT_REFRESH_SKIPPED project=${PROJECT}"
  run_local_live_dashboard_smoke
  python - <<'PY'
import json
import os
from pathlib import Path

project = os.environ["REPORT_PROJECT"]
marker_dir = Path("/tmp/live-dashboard-marker")
marker_dir.mkdir(parents=True, exist_ok=True)
marker = {
    "marker": "LIVE_ARTIFACT_MARKER_OK",
    "project": project,
    "mode": "skip_artifact_refresh",
}
(marker_dir / "marker.json").write_text(json.dumps(marker, ensure_ascii=False), encoding="utf-8")
PY
  verify_marker_on_host
  exit 0
fi

echo "LIVE_ARTIFACT_REFRESH_START project=${PROJECT}"
python daily_report_runner.py --project "${PROJECT}" --skip-email --skip-invoices --creditnote-storno-dry-run
run_local_live_dashboard_smoke
python - <<'PY'
import json
import os
from pathlib import Path

project = os.environ["REPORT_PROJECT"]
data_dir = Path("data") / project
payload_path = data_dir / "dashboard_payload_latest.json"
payload = json.loads(payload_path.read_text(encoding="utf-8"))
dashboard = payload.get("dashboard") or {}
kpis = dashboard.get("kpis") or {}
series = dashboard.get("series") or {}
commercial = dashboard.get("roy_product_demand") or {}
inventory = dashboard.get("roy_operations_inventory") or commercial
assert kpis.get("windows"), "KPI windows missing in generated payload"
assert series.get("dates"), "KPI source series dates missing in generated payload"
if project == "roy":
    assert inventory.get("summary"), "Operations inventory summary missing in generated payload"
    assert inventory.get("inventory_rows"), "Operations inventory rows missing in generated payload"
    inventory_summary = inventory.get("summary") or {}
    expected_model_version = "order-aware-tsb-v1"
    assert inventory_summary.get("demand_model_version") == expected_model_version, (
        "Unexpected ROY demand model version: "
        f"{inventory_summary.get('demand_model_version')!r}"
    )
    required_smart_fields = {
        "raw_recent_30d_units",
        "raw_alert_30d_units",
        "alert_30d_units",
        "demand_model",
        "demand_model_version",
        "demand_confidence",
        "alert_reason_code",
        "alert_reason_label_sk",
    }
    invalid_inventory_rows = []
    for row in inventory.get("inventory_rows") or []:
        if not isinstance(row, dict):
            invalid_inventory_rows.append({"sku": None, "missing": ["row_not_object"]})
            continue
        missing = sorted(required_smart_fields - set(row))
        if missing:
            invalid_inventory_rows.append({"sku": row.get("sku"), "missing": missing})
    assert not invalid_inventory_rows, (
        "Smart inventory fields missing: "
        f"{invalid_inventory_rows[:5]}"
    )
invalid_loss_rows = []
for row in commercial.get("loss_product_rows") or []:
    if not isinstance(row, dict):
        invalid_loss_rows.append(row)
        continue
    raw_gross = row.get("gross_profit")
    if raw_gross is None:
        raw_gross = row.get("cm1_profit")
    try:
        gross_profit = float(raw_gross)
    except (TypeError, ValueError):
        invalid_loss_rows.append(row)
        continue
    if gross_profit >= 0:
        invalid_loss_rows.append(row)
assert not invalid_loss_rows, "Loss products must be based on negative gross profit only"
marker_dir = Path("/tmp/live-dashboard-marker")
marker_dir.mkdir(parents=True, exist_ok=True)
marker = {
    "marker": "LIVE_ARTIFACT_MARKER_OK",
    "project": project,
    "payload_path": str(payload_path),
    "kpi_series_days": len(series.get("dates") or []),
    "inventory_alerts": (inventory.get("summary") or {}).get("alert_delivery_count"),
    "inventory_rows": len(inventory.get("inventory_rows") or []),
    "demand_model_version": (inventory.get("summary") or {}).get("demand_model_version"),
    "demand_anomalies": len(inventory.get("demand_anomaly_rows") or []),
}
(marker_dir / "marker.json").write_text(json.dumps(marker, ensure_ascii=False), encoding="utf-8")
PY
verify_marker_on_host
