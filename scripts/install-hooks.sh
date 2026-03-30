#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

git config core.hooksPath .githooks
chmod +x .githooks/pre-commit scripts/check_env.sh

echo "Hooks installed."
echo "core.hooksPath=$(git config core.hooksPath)"
