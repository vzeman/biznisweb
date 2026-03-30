#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

chmod +x scripts/install-hooks.sh scripts/check_env.sh || true
./scripts/install-hooks.sh

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created .env from .env.example"
fi

python_cmd=""
if command -v python3 >/dev/null 2>&1; then
  python_cmd="python3"
elif command -v python >/dev/null 2>&1; then
  python_cmd="python"
else
  echo "ERROR: Python not found" >&2
  exit 1
fi

if [[ ! -d .venv ]]; then
  "$python_cmd" -m venv .venv
fi

source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
./scripts/check_env.sh .env .env.example .env.required

echo "Bootstrap complete. Activate with: source .venv/bin/activate"
