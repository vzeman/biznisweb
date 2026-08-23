#!/usr/bin/env python3
"""Build and record sanitized VEVO Production GrowthBook clone evidence offline.

The actual GrowthBook objects are created and read back only in the authenticated
GrowthBook UI after the Production reader gate passes. This module has no
GrowthBook, AWS, GTM, Meta Ads, BiznisWeb, browser, or network client. It turns
the reviewed object IDs and read-back assertions into canonical evidence and can
update only the separately gated clone fields in the workspace manifest.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

try:
    from record_growthbook_natural_evidence import (
        _changed_leaf_paths,
        _write_json_atomic,
        canonical_evidence_bytes,
    )
    from record_growthbook_production_reader_evidence import (
        ReaderEvidenceRecordingError,
        validate_reader_evidence,
    )
except ModuleNotFoundError:  # Imported as scripts.record_growthbook_production_clone_evidence.
    from scripts.record_growthbook_natural_evidence import (
        _changed_leaf_paths,
        _write_json_atomic,
        canonical_evidence_bytes,
    )
    from scripts.record_growthbook_production_reader_evidence import (
        ReaderEvidenceRecordingError,
        validate_reader_evidence,
    )


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WORKSPACE_PATH = ROOT / "projects" / "vevo" / "growthbook_workspace.json"

EXPECTED_SCHEMA_VERSION = 2
EXPECTED_OBSERVATION_TYPE = "vevo_growthbook_production_clone_observation"
EXPECTED_ORGANIZATION_ID = "org_19g6mmt1q79o1"
EXPECTED_PROJECT_ID = "prj_2CeEJc6J9FwQFix9UhsnKr"
EXPECTED_DATA_SOURCE_NAME = "VEVO Production Experiment Facts"
EXPECTED_DATABASE = "vevo_growthbook_production"
EXPECTED_WORKGROUP = "vevo-growthbook-readonly-production"
EXPECTED_ASSIGNMENT_QUERY_NAME = "VEVO consented devices"
EXPECTED_IDENTIFIER_TYPE = "device_id"

FACT_TABLE_KEYS = (
    "vevo_device_outcomes_v1",
    "vevo_performance_vitals_v1",
)
METRIC_KEYS = (
    "vevo_add_to_cart_24h",
    "vevo_purchase_conversion_7d",
    "vevo_revenue_per_exposed_device_7d",
    "vevo_cm1_per_exposed_device_7d",
    "vevo_average_order_value_7d",
    "vevo_cancelled_order_rate_14d",
    "vevo_refunded_order_rate_14d",
    "vevo_client_error_device_rate_24h",
)
PAID_PRO_METRIC_KEYS = (
    "vevo_lcp_p75_24h",
    "vevo_inp_p75_24h",
    "vevo_cls_p75_milli_24h",
)

RUN_ID_RE = re.compile(r"^[1-9][0-9]{5,19}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
DATA_SOURCE_ID_RE = re.compile(r"^ds_[A-Za-z0-9]+$")
FACT_TABLE_ID_RE = re.compile(r"^ftb_[A-Za-z0-9]+$")
METRIC_ID_RE = re.compile(r"^fact__[A-Za-z0-9]+$")
OBSERVED_AT_RE = re.compile(
    r"^20[2-9][0-9]-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"
)

ROOT_KEYS = {
    "schema_version",
    "observation_type",
    "status",
    "observed_at_utc",
    "organization_id",
    "project_id",
    "reader_evidence_provenance",
    "source_preview_readback",
    "production_data_source",
    "production_fact_tables",
    "production_metrics",
    "paid_pro_quantile_metrics",
    "safety",
}
SOURCE_KEYS = {
    "data_source_id",
    "fact_table_ids",
    "metric_ids",
    "query_sha256",
    "objects_unchanged",
    "connection_repointed",
}
PROVENANCE_KEYS = {"workflow_run_id", "main_commit", "artifact_sha256"}
DATA_SOURCE_KEYS = {
    "id",
    "name",
    "type",
    "database",
    "workgroup",
    "identifier_type",
    "assignment_query_name",
    "assignment_query_sha256",
    "connection_test_passed",
    "assignment_query_test_passed",
    "assignment_query_result_row_count",
}
FACT_TABLE_KEYS_SET = {
    "id",
    "name",
    "identifier_type",
    "query_sha256",
    "query_test_passed",
    "growthbook_ui_query_result_row_count",
    "curated_result_row_count",
    "schema_probe_result_row_count",
    "schema_probe_excluded_from_named_experiments",
    "configuration_readback_match",
}
METRIC_KEYS_SET = {
    "id",
    "name",
    "contract_sha256",
    "configuration_readback_match",
}
PAID_PRO_KEYS = {
    "upgrade_authorized",
    "creation_skipped",
    "target_metric_ids",
}
SAFETY_KEYS = {
    "contains_credentials",
    "contains_query_results",
    "contains_event_or_device_ids",
    "contains_customer_or_order_data",
    "production_allocation_percent",
    "production_experiment_started",
    "gtm_published",
    "meta_ads_mutated",
    "biznisweb_mutated",
    "preview_repointed",
    "paid_upgrade_accepted",
}
EXPECTED_SAFETY = {
    "contains_credentials": False,
    "contains_query_results": False,
    "contains_event_or_device_ids": False,
    "contains_customer_or_order_data": False,
    "production_allocation_percent": 0,
    "production_experiment_started": False,
    "gtm_published": False,
    "meta_ads_mutated": False,
    "biznisweb_mutated": False,
    "preview_repointed": False,
    "paid_upgrade_accepted": False,
}
METRIC_DYNAMIC_KEYS = {
    "growthbook_id",
    "status",
    "created_verified_date",
    "analysis_query_verified_date",
    "analysis_query_synthetic_device_count",
}
ALLOWED_CHANGED_PATHS = {
    "athena.production.growthbook_clone.status",
    "athena.production.growthbook_clone.clone_allowed",
    "athena.production.growthbook_clone.mutation_status",
    "athena.production.growthbook_clone.observation_status",
    "athena.production.growthbook_clone.observation_sha256",
    "athena.production.growthbook_clone.successful_clone_verification",
    "athena.production.growthbook_clone.target_data_source_id",
    *{
        f"athena.production.growthbook_clone.target_fact_table_ids.{key}"
        for key in FACT_TABLE_KEYS
    },
    *{
        f"athena.production.growthbook_clone.target_metric_ids.{key}"
        for key in METRIC_KEYS
    },
    "athena.production.next_gate",
}


class CloneEvidenceRecordingError(ValueError):
    """Raised when Production clone evidence or state fails closed."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CloneEvidenceRecordingError(message)


def _exact(value: Any, keys: set[str], field: str) -> Mapping[str, Any]:
    _require(isinstance(value, dict), f"{field} must be an object")
    _require(set(value) == keys, f"{field} field set drift")
    return value


def _parse_utc(value: Any, field: str) -> datetime:
    text = str(value or "")
    _require(OBSERVED_AT_RE.fullmatch(text) is not None, f"{field} schema drift")
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
    except ValueError as exc:
        raise CloneEvidenceRecordingError(f"{field} is invalid") from exc


def _repo_file_sha256(rel_path: str) -> str:
    path = (ROOT / rel_path).resolve()
    try:
        path.relative_to(ROOT.resolve())
    except ValueError as exc:
        raise CloneEvidenceRecordingError("clone query path escapes repository") from exc
    _require(path.is_file(), f"clone query file is missing: {rel_path}")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _metric_contracts(workspace: Mapping[str, Any]) -> dict[str, dict[str, str]]:
    rows = {
        row.get("key"): row
        for row in workspace.get("metrics", [])
        if isinstance(row, dict) and row.get("key") in METRIC_KEYS
    }
    _require(set(rows) == set(METRIC_KEYS), "Starter-compatible metric contract drift")
    result: dict[str, dict[str, str]] = {}
    for key in METRIC_KEYS:
        row = rows[key]
        contract = {
            field: copy.deepcopy(value)
            for field, value in row.items()
            if field not in METRIC_DYNAMIC_KEYS
        }
        result[key] = {
            "name": str(row.get("name") or ""),
            "contract_sha256": hashlib.sha256(
                canonical_evidence_bytes(contract)
            ).hexdigest(),
        }
    return result


def _fact_contracts(workspace: Mapping[str, Any]) -> dict[str, dict[str, str]]:
    rows = {
        row.get("key"): row
        for row in workspace.get("fact_tables", [])
        if isinstance(row, dict) and row.get("key") in FACT_TABLE_KEYS
    }
    _require(set(rows) == set(FACT_TABLE_KEYS), "fact-table contract drift")
    clone = _clone_contract(workspace)
    query_paths = clone.get("target_fact_table_query_paths")
    _require(
        isinstance(query_paths, dict) and set(query_paths) == set(FACT_TABLE_KEYS),
        "Production fact-table query-path drift",
    )
    result: dict[str, dict[str, str]] = {}
    for key in FACT_TABLE_KEYS:
        row = rows[key]
        query_path = str(query_paths.get(key) or "")
        _require(row.get("identifier") == EXPECTED_IDENTIFIER_TYPE, "fact identifier drift")
        result[key] = {
            "name": str(row.get("name") or ""),
            "query_sha256": _repo_file_sha256(query_path),
        }
    return result


def _clone_contract(workspace: Mapping[str, Any]) -> Mapping[str, Any]:
    production = (workspace.get("athena") or {}).get("production")
    _require(isinstance(production, dict), "Production state is missing")
    clone = production.get("growthbook_clone")
    _require(isinstance(clone, dict), "Production clone state is missing")
    return clone


def _reader_provenance(workspace: Mapping[str, Any]) -> dict[str, str]:
    production = (workspace.get("athena") or {}).get("production")
    _require(isinstance(production, dict), "Production state is missing")
    provenance = {
        "workflow_run_id": str(production.get("reader_provisioning_run_id") or ""),
        "main_commit": str(production.get("reader_provisioning_main_commit") or ""),
        "artifact_sha256": str(production.get("reader_evidence_artifact_sha256") or ""),
    }
    _require(
        RUN_ID_RE.fullmatch(provenance["workflow_run_id"]) is not None,
        "reader provenance run ID drift",
    )
    _require(
        COMMIT_RE.fullmatch(provenance["main_commit"]) is not None,
        "reader provenance main commit drift",
    )
    _require(
        SHA256_RE.fullmatch(provenance["artifact_sha256"]) is not None,
        "reader provenance SHA-256 drift",
    )
    return provenance


def _query_hashes(workspace: Mapping[str, Any]) -> dict[str, str]:
    production = (workspace.get("athena") or {}).get("production") or {}
    clone = production.get("growthbook_clone") or {}
    fact_rows = {
        row.get("key"): row
        for row in workspace.get("fact_tables", [])
        if isinstance(row, dict) and row.get("key") in FACT_TABLE_KEYS
    }
    _require(set(fact_rows) == set(FACT_TABLE_KEYS), "Preview fact-table contract drift")
    return {
        "assignment": _repo_file_sha256(str(clone.get("assignment_query_path") or "")),
        **{
            key: _repo_file_sha256(str(fact_rows[key].get("query") or ""))
            for key in FACT_TABLE_KEYS
        },
    }


def _validate_target_ids(
    clone: Mapping[str, Any],
    *,
    data_source_id: str,
    fact_table_ids: Mapping[str, str],
    metric_ids: Mapping[str, str],
) -> None:
    _require(DATA_SOURCE_ID_RE.fullmatch(data_source_id) is not None, "target data-source ID drift")
    _require(data_source_id != clone.get("source_data_source_id"), "Production data source reuses Preview")
    _require(set(fact_table_ids) == set(FACT_TABLE_KEYS), "target fact-table key drift")
    _require(set(metric_ids) == set(METRIC_KEYS), "target metric key drift")
    for key, value in fact_table_ids.items():
        _require(FACT_TABLE_ID_RE.fullmatch(value) is not None, f"target fact-table ID drift: {key}")
        _require(
            value != (clone.get("source_fact_table_ids") or {}).get(key),
            f"Production fact table reuses Preview: {key}",
        )
    for key, value in metric_ids.items():
        _require(METRIC_ID_RE.fullmatch(value) is not None, f"target metric ID drift: {key}")
        _require(
            value != (clone.get("source_metric_ids") or {}).get(key),
            f"Production metric reuses Preview: {key}",
        )
    all_ids = [data_source_id, *fact_table_ids.values(), *metric_ids.values()]
    _require(len(all_ids) == len(set(all_ids)), "Production GrowthBook target IDs are not unique")


def validate_clone_observation(
    observation: Mapping[str, Any], workspace: Mapping[str, Any]
) -> None:
    root = _exact(observation, ROOT_KEYS, "clone observation")
    _require(root["schema_version"] == EXPECTED_SCHEMA_VERSION, "clone schema drift")
    _require(root["observation_type"] == EXPECTED_OBSERVATION_TYPE, "clone type drift")
    _require(root["status"] == "passed", "clone observation did not pass")
    _parse_utc(root["observed_at_utc"], "clone observed_at_utc")
    _require(root["organization_id"] == EXPECTED_ORGANIZATION_ID, "GrowthBook organization drift")
    _require(root["project_id"] == EXPECTED_PROJECT_ID, "GrowthBook project drift")

    provenance = _exact(
        root["reader_evidence_provenance"],
        PROVENANCE_KEYS,
        "reader evidence provenance",
    )
    _require(
        provenance == _reader_provenance(workspace),
        "clone reader evidence provenance drift",
    )

    clone = _clone_contract(workspace)
    expected_query_hashes = _query_hashes(workspace)
    expected_fact_contracts = _fact_contracts(workspace)
    expected_metric_contracts = _metric_contracts(workspace)

    source = _exact(root["source_preview_readback"], SOURCE_KEYS, "Preview readback")
    _require(source["data_source_id"] == clone.get("source_data_source_id"), "Preview data-source drift")
    _require(source["fact_table_ids"] == clone.get("source_fact_table_ids"), "Preview fact-table drift")
    _require(source["metric_ids"] == clone.get("source_metric_ids"), "Preview metric drift")
    _require(source["query_sha256"] == expected_query_hashes, "Preview query hash drift")
    _require(source["objects_unchanged"] is True, "Preview objects were not read back unchanged")
    _require(source["connection_repointed"] is False, "Preview connection was repointed")

    data_source = _exact(root["production_data_source"], DATA_SOURCE_KEYS, "Production data source")
    _require(data_source["name"] == EXPECTED_DATA_SOURCE_NAME, "Production data-source name drift")
    _require(data_source["type"] == "athena", "Production data-source type drift")
    _require(data_source["database"] == EXPECTED_DATABASE, "Production database drift")
    _require(data_source["workgroup"] == EXPECTED_WORKGROUP, "Production workgroup drift")
    _require(data_source["identifier_type"] == EXPECTED_IDENTIFIER_TYPE, "Production identifier drift")
    _require(
        data_source["assignment_query_name"] == EXPECTED_ASSIGNMENT_QUERY_NAME,
        "Production assignment-query name drift",
    )
    _require(
        data_source["assignment_query_sha256"] == expected_query_hashes["assignment"],
        "Production assignment-query hash drift",
    )
    _require(data_source["connection_test_passed"] is True, "Production connection test missing")
    _require(
        data_source["assignment_query_test_passed"] is True,
        "Production assignment-query test missing",
    )
    _require(
        data_source["assignment_query_result_row_count"] == 0
        and type(data_source["assignment_query_result_row_count"]) is int,
        "Production assignment query must remain empty before traffic",
    )

    fact_tables = _exact(
        root["production_fact_tables"], set(FACT_TABLE_KEYS), "Production fact tables"
    )
    fact_ids: dict[str, str] = {}
    for key in FACT_TABLE_KEYS:
        row = _exact(fact_tables[key], FACT_TABLE_KEYS_SET, f"Production fact table {key}")
        fact_ids[key] = str(row["id"])
        _require(row["name"] == expected_fact_contracts[key]["name"], f"fact name drift: {key}")
        _require(row["identifier_type"] == EXPECTED_IDENTIFIER_TYPE, f"fact identifier drift: {key}")
        _require(
            row["query_sha256"] == expected_fact_contracts[key]["query_sha256"],
            f"fact query hash drift: {key}",
        )
        _require(row["query_test_passed"] is True, f"fact query test missing: {key}")
        _require(
            row["growthbook_ui_query_result_row_count"] == 1
            and type(row["growthbook_ui_query_result_row_count"]) is int,
            f"GrowthBook fact query must return exactly one schema probe: {key}",
        )
        _require(
            row["schema_probe_result_row_count"] == 1
            and type(row["schema_probe_result_row_count"]) is int,
            f"GrowthBook schema-probe count drift: {key}",
        )
        _require(
            row["curated_result_row_count"] == 0
            and type(row["curated_result_row_count"]) is int,
            f"Production curated fact table must remain empty before traffic: {key}",
        )
        _require(
            row["schema_probe_excluded_from_named_experiments"] is True,
            f"GrowthBook schema probe is not excluded from named experiments: {key}",
        )
        _require(
            row["configuration_readback_match"] is True,
            f"fact configuration readback mismatch: {key}",
        )

    metrics = _exact(root["production_metrics"], set(METRIC_KEYS), "Production metrics")
    metric_ids: dict[str, str] = {}
    for key in METRIC_KEYS:
        row = _exact(metrics[key], METRIC_KEYS_SET, f"Production metric {key}")
        metric_ids[key] = str(row["id"])
        _require(row["name"] == expected_metric_contracts[key]["name"], f"metric name drift: {key}")
        _require(
            row["contract_sha256"] == expected_metric_contracts[key]["contract_sha256"],
            f"metric contract hash drift: {key}",
        )
        _require(
            row["configuration_readback_match"] is True,
            f"metric configuration readback mismatch: {key}",
        )

    paid = _exact(root["paid_pro_quantile_metrics"], PAID_PRO_KEYS, "paid Pro metrics")
    _require(paid["upgrade_authorized"] is False, "paid Pro upgrade was not authorized")
    _require(paid["creation_skipped"] is True, "paid Pro metric creation must be skipped")
    _require(
        paid["target_metric_ids"] == {key: None for key in PAID_PRO_METRIC_KEYS},
        "paid Pro metric target state drift",
    )
    _require(_exact(root["safety"], SAFETY_KEYS, "clone safety") == EXPECTED_SAFETY, "clone safety drift")
    _validate_target_ids(
        clone,
        data_source_id=str(data_source["id"]),
        fact_table_ids=fact_ids,
        metric_ids=metric_ids,
    )


def build_clone_observation(
    workspace: Mapping[str, Any],
    *,
    observed_at: datetime,
    data_source_id: str,
    fact_table_ids: Mapping[str, str],
    metric_ids: Mapping[str, str],
) -> dict[str, Any]:
    _require(
        observed_at.tzinfo is not None and observed_at.utcoffset() is not None,
        "clone observation clock must be timezone-aware",
    )
    clone = _clone_contract(workspace)
    _validate_target_ids(
        clone,
        data_source_id=data_source_id,
        fact_table_ids=fact_table_ids,
        metric_ids=metric_ids,
    )
    query_hashes = _query_hashes(workspace)
    fact_contracts = _fact_contracts(workspace)
    metric_contracts = _metric_contracts(workspace)
    observation = {
        "schema_version": EXPECTED_SCHEMA_VERSION,
        "observation_type": EXPECTED_OBSERVATION_TYPE,
        "status": "passed",
        "observed_at_utc": observed_at.astimezone(timezone.utc).replace(
            microsecond=0
        ).isoformat().replace("+00:00", "Z"),
        "organization_id": EXPECTED_ORGANIZATION_ID,
        "project_id": EXPECTED_PROJECT_ID,
        "reader_evidence_provenance": _reader_provenance(workspace),
        "source_preview_readback": {
            "data_source_id": clone["source_data_source_id"],
            "fact_table_ids": copy.deepcopy(clone["source_fact_table_ids"]),
            "metric_ids": copy.deepcopy(clone["source_metric_ids"]),
            "query_sha256": query_hashes,
            "objects_unchanged": True,
            "connection_repointed": False,
        },
        "production_data_source": {
            "id": data_source_id,
            "name": EXPECTED_DATA_SOURCE_NAME,
            "type": "athena",
            "database": EXPECTED_DATABASE,
            "workgroup": EXPECTED_WORKGROUP,
            "identifier_type": EXPECTED_IDENTIFIER_TYPE,
            "assignment_query_name": EXPECTED_ASSIGNMENT_QUERY_NAME,
            "assignment_query_sha256": query_hashes["assignment"],
            "connection_test_passed": True,
            "assignment_query_test_passed": True,
            "assignment_query_result_row_count": 0,
        },
        "production_fact_tables": {
            key: {
                "id": fact_table_ids[key],
                "name": fact_contracts[key]["name"],
                "identifier_type": EXPECTED_IDENTIFIER_TYPE,
                "query_sha256": fact_contracts[key]["query_sha256"],
                "query_test_passed": True,
                "growthbook_ui_query_result_row_count": 1,
                "curated_result_row_count": 0,
                "schema_probe_result_row_count": 1,
                "schema_probe_excluded_from_named_experiments": True,
                "configuration_readback_match": True,
            }
            for key in FACT_TABLE_KEYS
        },
        "production_metrics": {
            key: {
                "id": metric_ids[key],
                "name": metric_contracts[key]["name"],
                "contract_sha256": metric_contracts[key]["contract_sha256"],
                "configuration_readback_match": True,
            }
            for key in METRIC_KEYS
        },
        "paid_pro_quantile_metrics": {
            "upgrade_authorized": False,
            "creation_skipped": True,
            "target_metric_ids": {key: None for key in PAID_PRO_METRIC_KEYS},
        },
        "safety": copy.deepcopy(EXPECTED_SAFETY),
    }
    validate_clone_observation(observation, workspace)
    return observation


def _validate_reader_state(production: Mapping[str, Any]) -> None:
    evidence = production.get("successful_reader_provisioning")
    _require(isinstance(evidence, dict), "verified Production reader evidence is missing")
    run_id = str(production.get("reader_provisioning_run_id") or "")
    main_commit = str(production.get("reader_provisioning_main_commit") or "")
    evidence_sha256 = str(production.get("reader_evidence_artifact_sha256") or "")
    _require(RUN_ID_RE.fullmatch(run_id) is not None, "reader run ID state drift")
    _require(COMMIT_RE.fullmatch(main_commit) is not None, "reader main commit state drift")
    _require(SHA256_RE.fullmatch(evidence_sha256) is not None, "reader evidence SHA state drift")
    try:
        validate_reader_evidence(
            evidence,
            expected_workflow_run_id=run_id,
            expected_main_commit=main_commit,
            expected_foundation_run_id=str(production.get("foundation_deployment_run_id") or ""),
            expected_foundation_main_commit=str(
                production.get("foundation_deployment_main_commit") or ""
            ),
            expected_foundation_sha256=str(
                production.get("foundation_evidence_artifact_sha256") or ""
            ),
        )
    except (ReaderEvidenceRecordingError, TypeError) as exc:
        raise CloneEvidenceRecordingError("verified Production reader evidence drift") from exc
    _require(
        hashlib.sha256(canonical_evidence_bytes(evidence)).hexdigest() == evidence_sha256,
        "verified Production reader evidence SHA-256 drift",
    )


def record_clone_evidence(
    workspace: Mapping[str, Any],
    observation: Mapping[str, Any],
    *,
    observation_sha256: str,
) -> dict[str, Any]:
    _require(SHA256_RE.fullmatch(observation_sha256) is not None, "clone observation SHA-256 is invalid")
    validate_clone_observation(observation, workspace)
    _require(
        hashlib.sha256(canonical_evidence_bytes(observation)).hexdigest()
        == observation_sha256,
        "clone observation SHA-256 mismatch",
    )
    result = copy.deepcopy(dict(workspace))
    production = (result.get("athena") or {}).get("production")
    _require(isinstance(production, dict), "Production state is missing")
    _validate_reader_state(production)
    clone = production.get("growthbook_clone")
    _require(isinstance(clone, dict), "Production clone state is missing")

    if clone.get("successful_clone_verification") is not None:
        _require(
            clone.get("status") == "verified_complete"
            and clone.get("clone_allowed") is False
            and clone.get("mutation_status") == "created_and_query_verified"
            and clone.get("observation_status") == "verified_canonical_sha256_recorded"
            and clone.get("observation_sha256") == observation_sha256
            and clone.get("successful_clone_verification") == observation,
            "a different or incomplete Production clone observation is already recorded",
        )
        return result

    _require(
        production.get("status") == "route_disabled_foundation_deployed_verified"
        and production.get("credentials_created") is True
        and production.get("reader_provisioning_status")
        == "verified_active_encrypted_handoff_ready_for_growthbook"
        and production.get("reader_provisioning_allowed") is False,
        "Production reader is not in the verified clone-ready state",
    )
    _require(
        clone.get("status") == "reader_verified_ready_for_reviewed_growthbook_clone"
        and clone.get("clone_allowed") is True
        and clone.get("mutation_status") == "not_started",
        "Production clone gate is not open",
    )
    _require(
        clone.get("paid_pro_upgrade_authorized") is False
        and (result.get("workspace") or {}).get("plan_type") == "starter"
        and (result.get("workspace") or {}).get("subscription_or_trial_status")
        == "starter_active_no_paid_upgrade_accepted",
        "paid GrowthBook upgrade is not authorized",
    )
    _require(
        clone.get("observation_status") == "not_recorded"
        and clone.get("observation_sha256") is None,
        "Production clone observation state is not pending",
    )
    _require(
        clone.get("observation_schema_version") == EXPECTED_SCHEMA_VERSION
        and clone.get("observation_file")
        == "vevo-growthbook-production-clone-observation.json",
        "Production clone observation contract drift",
    )
    _require(
        clone.get("target_data_source_id") is None
        and all(value is None for value in (clone.get("target_fact_table_ids") or {}).values())
        and all(value is None for value in (clone.get("target_metric_ids") or {}).values()),
        "Production clone target IDs are already populated",
    )
    _require(production.get("experiment_registry_empty") is True, "Production registry is not empty")
    _require(
        (result.get("workspace") or {}).get("production_allocation_percent") == 0,
        "Production allocation must remain zero",
    )
    _require(
        (result.get("gtm_preview_workspace") or {}).get("publish_status") == "not_published",
        "GTM must remain unpublished",
    )
    _require(
        (result.get("decision_gates") or {}).get("production_activation_allowed") is False,
        "Production activation must remain disabled",
    )

    data_source = observation["production_data_source"]
    fact_tables = observation["production_fact_tables"]
    metrics = observation["production_metrics"]
    clone["status"] = "verified_complete"
    clone["clone_allowed"] = False
    clone["mutation_status"] = "created_and_query_verified"
    clone["observation_status"] = "verified_canonical_sha256_recorded"
    clone["observation_sha256"] = observation_sha256
    clone["successful_clone_verification"] = copy.deepcopy(dict(observation))
    clone["target_data_source_id"] = data_source["id"]
    clone["target_fact_table_ids"] = {
        key: fact_tables[key]["id"] for key in FACT_TABLE_KEYS
    }
    clone["target_metric_ids"] = {
        key: metrics[key]["id"] for key in METRIC_KEYS
    }
    production["next_gate"] = "prepare_production_aa_activation_after_review"
    _require(
        _changed_leaf_paths(workspace, result) == ALLOWED_CHANGED_PATHS,
        "Production clone manifest change-set boundary drift",
    )
    return result


def load_validate_and_record(
    *,
    observation_path: Path,
    workspace_path: Path,
    expected_observation_sha256: str,
) -> dict[str, Any]:
    try:
        raw = observation_path.read_bytes()
        observation = json.loads(raw.decode("utf-8"))
        workspace = json.loads(workspace_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CloneEvidenceRecordingError("clone observation or workspace is unreadable") from exc
    _require(isinstance(observation, dict), "clone observation must contain an object")
    _require(isinstance(workspace, dict), "workspace must contain an object")
    _require(
        raw == canonical_evidence_bytes(observation),
        "clone observation bytes are not canonical",
    )
    _require(
        hashlib.sha256(raw).hexdigest() == expected_observation_sha256,
        "clone observation SHA-256 mismatch",
    )
    return record_clone_evidence(
        workspace,
        observation,
        observation_sha256=expected_observation_sha256,
    )


def _write_canonical_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(canonical_evidence_bytes(payload))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    except Exception:
        try:
            os.unlink(temporary_name)
        except OSError:
            pass
        raise


def _parse_key_values(values: Sequence[str], expected_keys: Sequence[str], field: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        key, separator, object_id = value.partition("=")
        _require(separator == "=" and key and object_id, f"{field} must use key=id")
        _require(key not in result, f"duplicate {field} key: {key}")
        result[key] = object_id
    _require(set(result) == set(expected_keys), f"{field} key set drift")
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build", help="Build canonical reviewed clone observation")
    build.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE_PATH)
    build.add_argument("--observed-at-utc", required=True)
    build.add_argument("--data-source-id", required=True)
    build.add_argument("--fact-table-id", action="append", default=[])
    build.add_argument("--metric-id", action="append", default=[])
    build.add_argument("--output", required=True, type=Path)

    record = subparsers.add_parser("record", help="Record canonical reviewed clone evidence")
    record.add_argument("--observation", required=True, type=Path)
    record.add_argument("--observation-sha256", required=True)
    record.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE_PATH)
    record.add_argument("--output", required=True, type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "build":
            workspace = json.loads(args.workspace.read_text(encoding="utf-8"))
            _require(isinstance(workspace, dict), "workspace must contain an object")
            observed_at = _parse_utc(args.observed_at_utc, "clone observed_at_utc")
            observation = build_clone_observation(
                workspace,
                observed_at=observed_at,
                data_source_id=args.data_source_id,
                fact_table_ids=_parse_key_values(
                    args.fact_table_id, FACT_TABLE_KEYS, "fact-table ID"
                ),
                metric_ids=_parse_key_values(args.metric_id, METRIC_KEYS, "metric ID"),
            )
            _write_canonical_atomic(args.output, observation)
            print(
                "VEVO_GROWTHBOOK_PRODUCTION_CLONE_OBSERVATION_READY:"
                f"output={args.output.name}:allocation=0:gtm=not_published:"
                "preview-repointed=false:paid-upgrade=false"
            )
            return 0
        result = load_validate_and_record(
            observation_path=args.observation,
            workspace_path=args.workspace,
            expected_observation_sha256=args.observation_sha256,
        )
        _write_json_atomic(args.output, result)
        print(
            "VEVO_GROWTHBOOK_PRODUCTION_CLONE_EVIDENCE_RECORDED:"
            f"output={args.output.name}:allocation=0:gtm=not_published:"
            "clone=verified:production-aa=false"
        )
        return 0
    except (OSError, json.JSONDecodeError, CloneEvidenceRecordingError) as exc:
        print(f"VEVO_GROWTHBOOK_PRODUCTION_CLONE_EVIDENCE_INVALID:{exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
