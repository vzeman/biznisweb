from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any


MARKER_PREFIX = "COLLECTOR_REGISTRY_OK"


class RegistryMarkerError(ValueError):
    """Raised when the packaged registry cannot be identified safely."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RegistryMarkerError(message)


def build_registry_marker(registry_path: Path, environment: str) -> str:
    raw = registry_path.read_bytes()
    try:
        payload: Any = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RegistryMarkerError("collector registry is not valid JSON") from exc

    _require(isinstance(payload, dict), "collector registry must be an object")
    _require(payload.get("schema_version") == 1, "collector registry schema drift")
    environments = payload.get("environments")
    _require(isinstance(environments, dict), "collector registry environments are missing")
    contracts = environments.get(environment)
    _require(isinstance(contracts, dict), "collector registry environment is missing")
    keys = sorted(contracts)
    _require(
        all(isinstance(key, str) and key for key in keys),
        "collector registry contains an invalid tracking key",
    )
    digest = hashlib.sha256(raw).hexdigest()
    return f"{MARKER_PREFIX}:{environment}:{digest}:{','.join(keys)}"


def main() -> int:
    environment = os.environ.get("GROWTHBOOK_ENVIRONMENT", "").strip()
    registry_value = os.environ.get("GROWTHBOOK_EXPERIMENT_REGISTRY_PATH", "").strip()
    _require(environment in {"preview", "production"}, "collector environment is invalid")
    _require(bool(registry_value), "collector registry path is missing")
    print(build_registry_marker(Path(registry_value), environment))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
