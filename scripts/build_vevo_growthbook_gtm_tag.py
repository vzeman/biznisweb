#!/usr/bin/env python3
"""Build the exact GTM Custom HTML loader from versioned source and config."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from pathlib import Path
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parents[1]
CLIENT_PATH = ROOT / "storefront" / "vevo-growthbook" / "vevo-growthbook.js"
ACTIVATION_PATH = ROOT / "projects" / "vevo" / "growthbook_production_aa_activation.json"
PRODUCTION_DISABLED_MARKER = "var PRODUCTION_ACTIVATION = false;"
PRODUCTION_ENABLED_MARKER = "var PRODUCTION_ACTIVATION = true;"
CONFIG_FIELDS = {
    "schemaVersion",
    "environment",
    "clientKey",
    "apiHost",
    "collectorUrl",
    "allowedHost",
    "gtmContainerId",
    "enableDevMode",
}


def _production_collector_host_sha256() -> str:
    activation = json.loads(ACTIVATION_PATH.read_text(encoding="utf-8"))
    digest = activation.get("collector", {}).get("endpoint_host_sha256")
    if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError("reviewed Production collector host hash is unavailable")
    return digest


def validate_config(
    payload: object,
    *,
    expected_production_collector_host_sha256: str | None = None,
) -> dict:
    if not isinstance(payload, dict) or set(payload) != CONFIG_FIELDS:
        raise ValueError("config must use the exact storefront field set")
    if payload["schemaVersion"] != 1 or payload["environment"] not in {
        "preview",
        "production",
    }:
        raise ValueError("only reviewed Preview or Production config can be built")
    environment = payload["environment"]
    if (
        not isinstance(payload["clientKey"], str)
        or not re.fullmatch(r"sdk-[A-Za-z0-9_-]{8,120}", payload["clientKey"])
        or "REPLACE" in payload["clientKey"]
    ):
        raise ValueError(f"invalid GrowthBook {environment} client key")
    if payload["apiHost"] != "https://cdn.growthbook.io":
        raise ValueError("unexpected GrowthBook API host")
    if payload["allowedHost"] != "www.vevo.sk" or payload["gtmContainerId"] != "GTM-5ZB5LFGB":
        raise ValueError("unexpected VEVO storefront or GTM container")
    expected_dev_mode = environment == "preview"
    if payload["enableDevMode"] is not expected_dev_mode:
        raise ValueError(
            f"{environment.capitalize()} artifact requires "
            f"enableDevMode={str(expected_dev_mode).lower()}"
        )
    collector = urlparse(str(payload["collectorUrl"]))
    execute_api_collector = bool(
        re.fullmatch(
            r"[a-z0-9]+\.execute-api\.eu-central-1\.amazonaws\.com",
            collector.hostname or "",
        )
    )
    allowed_collector = execute_api_collector or (
        environment == "preview" and collector.hostname == "events-preview.vevo.sk"
    )
    if (
        collector.scheme != "https"
        or not allowed_collector
        or collector.path != "/v1/events"
        or collector.params
        or collector.query
        or collector.fragment
        or collector.port
        or collector.username
        or collector.password
    ):
        raise ValueError(f"invalid {environment} collector URL")
    if environment == "production":
        expected_digest = (
            expected_production_collector_host_sha256
            or _production_collector_host_sha256()
        )
        actual_digest = hashlib.sha256(
            (collector.hostname or "").encode("utf-8")
        ).hexdigest()
        if actual_digest != expected_digest:
            raise ValueError("Production collector host does not match reviewed evidence")
    return payload


def build_tag(
    config_path: Path,
    output_path: Path,
    *,
    client_key_override: str | None = None,
    collector_url_override: str | None = None,
    expected_production_collector_host_sha256: str | None = None,
) -> str:
    config_payload = json.loads(config_path.read_text(encoding="utf-8"))
    if client_key_override is not None or collector_url_override is not None:
        if not isinstance(config_payload, dict):
            raise ValueError("config must be a JSON object")
        config_payload = dict(config_payload)
    if client_key_override is not None:
        config_payload["clientKey"] = client_key_override
    if collector_url_override is not None:
        config_payload["collectorUrl"] = collector_url_override
    config = validate_config(
        config_payload,
        expected_production_collector_host_sha256=(
            expected_production_collector_host_sha256
        ),
    )
    client = CLIENT_PATH.read_text(encoding="utf-8").strip()
    if (
        client.count(PRODUCTION_DISABLED_MARKER) != 1
        or PRODUCTION_ENABLED_MARKER in client
    ):
        raise ValueError("storefront Production compile-time gate drift")
    if config["environment"] == "production":
        client = client.replace(
            PRODUCTION_DISABLED_MARKER,
            PRODUCTION_ENABLED_MARKER,
            1,
        )
    if "</script" in client.lower():
        raise ValueError("storefront client cannot be safely embedded in Custom HTML")
    config_json = json.dumps(config, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    artifact = (
        "<!-- Generated by scripts/build_vevo_growthbook_gtm_tag.py; do not edit in GTM. -->\n"
        "<script>\n"
        f"window.VEVO_GROWTHBOOK_CONFIG={config_json};\n"
        "</script>\n"
        "<script>\n"
        f"{client}\n"
        "</script>\n"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(artifact, encoding="utf-8", newline="\n")
    return hashlib.sha256(artifact.encode("utf-8")).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    config_payload = json.loads(args.config.read_text(encoding="utf-8"))
    environment = (
        config_payload.get("environment") if isinstance(config_payload, dict) else None
    )
    if environment == "production":
        client_key_override = os.getenv("VEVO_GROWTHBOOK_PRODUCTION_CLIENT_KEY")
        collector_url_override = os.getenv("VEVO_GROWTHBOOK_PRODUCTION_COLLECTOR_URL")
    else:
        client_key_override = os.getenv("VEVO_GROWTHBOOK_PREVIEW_CLIENT_KEY")
        collector_url_override = os.getenv("VEVO_GROWTHBOOK_PREVIEW_COLLECTOR_URL")
    digest = build_tag(
        args.config.resolve(),
        args.output.resolve(),
        client_key_override=client_key_override,
        collector_url_override=collector_url_override,
    )
    print(f"VEVO_GROWTHBOOK_GTM_SHA256={digest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
