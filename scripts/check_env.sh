#!/usr/bin/env bash
set -euo pipefail

env_path="${1:-.env}"
example_path="${2:-.env.example}"
required_path="${3:-.env.required}"

if [[ ! -f "$example_path" ]]; then
  echo "ERROR: File not found: $example_path" >&2
  exit 2
fi

if [[ ! -f "$env_path" ]]; then
  echo "ERROR: File not found: $env_path" >&2
  exit 2
fi

extract_env_keys() {
  local path="$1"
  awk '
    {
      line=$0
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", line)
      if (line=="" || line ~ /^#/) next
      sub(/^export[[:space:]]+/, "", line)
      eq=index(line, "=")
      if (eq<=1) next
      key=substr(line, 1, eq-1)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", key)
      if (key ~ /^[A-Za-z_][A-Za-z0-9_]*$/) print key
    }
  ' "$path" | sort -u
}

extract_key_list() {
  local path="$1"
  awk '
    {
      line=$0
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", line)
      if (line=="" || line ~ /^#/) next
      sub(/^export[[:space:]]+/, "", line)
      eq=index(line, "=")
      key=(eq>1) ? substr(line, 1, eq-1) : line
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", key)
      if (key ~ /^[A-Za-z_][A-Za-z0-9_]*$/) print key
    }
  ' "$path" | sort -u
}

example_keys="$(extract_env_keys "$example_path")"
actual_keys="$(extract_env_keys "$env_path")"

required_source="$example_path"
if [[ -f "$required_path" ]]; then
  required_keys="$(extract_key_list "$required_path")"
  required_source="$required_path"
else
  required_keys="$example_keys"
fi

missing_keys="$(comm -23 <(printf "%s\n" "$required_keys") <(printf "%s\n" "$actual_keys") || true)"
extra_keys="$(comm -13 <(printf "%s\n" "$example_keys") <(printf "%s\n" "$actual_keys") || true)"

required_count=$(printf "%s\n" "$required_keys" | sed '/^$/d' | wc -l | tr -d ' ')
actual_count=$(printf "%s\n" "$actual_keys" | sed '/^$/d' | wc -l | tr -d ' ')

echo "ENV check"
echo "- required source: $required_source"
echo "- required keys:   $required_count"
echo "- actual keys:     $actual_count"

if [[ -n "${missing_keys//[$'\t\r\n ']}" ]]; then
  echo
  echo "Missing required keys in $env_path:"
  while IFS= read -r key; do
    [[ -z "$key" ]] && continue
    echo "  - $key"
  done <<< "$missing_keys"
fi

if [[ -n "${extra_keys//[$'\t\r\n ']}" ]]; then
  echo
  echo "Extra keys in $env_path (not in $example_path):"
  while IFS= read -r key; do
    [[ -z "$key" ]] && continue
    echo "  - $key"
  done <<< "$extra_keys"
fi

if [[ -z "${missing_keys//[$'\t\r\n ']}" ]]; then
  echo
  echo "OK: no missing required keys."
  exit 0
fi

exit 1
