#!/usr/bin/env python3
"""Evaluate VEVO's sanitized A/A evidence against the frozen acceptance gates.

The evaluator is deliberately offline and mutation-free. It accepts aggregate,
PII-free evidence only and returns PASS, FAIL, or NOT_READY. An A/A result can
never declare a winner or authorize the later CTA A/B experiment by itself.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import tempfile
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = ROOT / "projects" / "vevo" / "growthbook_aa_acceptance.json"

EXPECTED_CONFIG_KEYS = {
    "schema_version",
    "experiment_id",
    "timezone",
    "variations",
    "expected_variation_weights",
    "required_production_allocation_percent",
    "minimum_full_calendar_days",
    "minimum_eligible_devices",
    "minimum_measured_page_loads_per_arm",
    "minimum_exact_joined_transactions",
    "minimum_meta_exposures",
    "minimum_complete_stable_meta_dimension_exposures",
    "privacy_sample_max_rows",
    "srm_p_value_min",
    "split_percent_min",
    "split_percent_max",
    "pipeline_count_difference_max_percent",
    "growthbook_reporting_count_difference_max_percent",
    "duplicate_event_rate_max_percent",
    "exact_order_join_rate_min_percent",
    "lcp_degradation_absolute_ms",
    "lcp_degradation_relative_percent",
    "inp_degradation_absolute_ms",
    "inp_degradation_relative_percent",
    "cls_degradation_absolute_milli",
    "client_error_rate_increase_max_percentage_points",
}
EXPECTED_SNAPSHOT_KEYS = {
    "schema_version",
    "experiment_id",
    "full_allocation_started_at_utc",
    "evaluated_at_utc",
    "production_allocation_percent",
    "identical_variations_verified",
    "growthbook_srm_warning",
    "pipeline_counts",
    "growthbook_variation_counts",
    "reporting_quality",
    "meta_dimension_audit",
    "privacy_audit",
    "consent_audit",
    "commerce_health",
    "qa_checklist",
}
EXPECTED_PIPELINE_KEYS = {
    "collector_received_event_count",
    "collector_unique_accepted_event_count",
    "collector_duplicate_event_count",
    "athena_unique_event_count",
    "reporting_unique_event_count",
}
EXPECTED_QUALITY_KEYS = {
    "raw_event_count",
    "unique_event_count",
    "duplicate_event_count",
    "orphan_event_count",
    "eligible_device_count",
    "contaminated_device_count",
    "srm_p_value",
    "unique_transaction_count",
    "exact_joined_transaction_count",
    "unmatched_transaction_count",
    "ambiguous_transaction_count",
    "variation_health",
}
EXPECTED_VARIATION_HEALTH_KEYS = {
    "eligible_devices",
    "measured_page_loads",
    "lcp_p75_ms",
    "inp_p75_ms",
    "cls_p75_milli",
    "client_error_device_rate_pct",
}
EXPECTED_META_KEYS = {
    "meta_exposure_count",
    "complete_stable_dimension_exposure_count",
    "invalid_dimension_row_count",
    "forbidden_click_identifier_count",
}
EXPECTED_PRIVACY_KEYS = {
    "total_stored_row_count",
    "sampled_row_count",
    "pii_finding_count",
    "forbidden_field_finding_count",
    "raw_ip_address_stored_count",
    "full_url_stored_count",
    "click_identifier_stored_count",
    "customer_field_stored_count",
}
EXPECTED_CONSENT_KEYS = {
    "pre_consent_request_count",
    "non_analytical_consent_exposure_count",
    "post_withdrawal_event_count",
}
EXPECTED_COMMERCE_KEYS = {
    "checkout_runtime_error_count",
    "duplicate_ga4_purchase_event_count",
    "duplicate_meta_purchase_event_count",
    "price_cart_checkout_mutation_observed",
    "add_to_cart_behavior_regression_observed",
    "rollback_test_passed",
}
EXPECTED_QA_KEYS = {
    "desktop_passed",
    "mobile_passed",
    "consent_accept_passed",
    "consent_reject_passed",
    "consent_withdrawal_passed",
}
UTC_RE = re.compile(r"^20[2-9][0-9]-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")


class AaEvaluationError(ValueError):
    """Raised when the A/A evidence or acceptance contract is malformed."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AaEvaluationError(message)


def _require_exact_keys(
    value: Any, expected: set[str], field: str
) -> Mapping[str, Any]:
    _require(isinstance(value, dict), f"{field} must be an object")
    _require(set(value) == expected, f"{field} field set drift")
    return value


def _integer(value: Any, field: str, *, maximum: int | None = None) -> int:
    _require(
        type(value) is int and value >= 0, f"{field} must be a non-negative integer"
    )
    if maximum is not None:
        _require(value <= maximum, f"{field} exceeds its allowed maximum")
    return value


def _number(
    value: Any,
    field: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
    nullable: bool = False,
) -> float | None:
    if value is None and nullable:
        return None
    _require(type(value) in (int, float), f"{field} must be numeric")
    result = float(value)
    _require(math.isfinite(result), f"{field} must be finite")
    if minimum is not None:
        _require(result >= minimum, f"{field} is below its allowed minimum")
    if maximum is not None:
        _require(result <= maximum, f"{field} exceeds its allowed maximum")
    return result


def _boolean(value: Any, field: str) -> bool:
    _require(type(value) is bool, f"{field} must be boolean")
    return value


def _parse_utc(value: Any, field: str) -> datetime:
    text = str(value or "")
    _require(
        UTC_RE.fullmatch(text) is not None,
        f"{field} must use whole-second UTC Z format",
    )
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
    except ValueError as exc:
        raise AaEvaluationError(f"{field} is invalid") from exc


def _percentage_difference(actual: int, reference: int) -> float | None:
    if reference == 0:
        return 0.0 if actual == 0 else None
    return round(100.0 * abs(actual - reference) / reference, 6)


def _srm(counts: Mapping[str, int], weights: Mapping[str, float]) -> float:
    total = sum(counts.values())
    if total == 0:
        return 1.0
    chi_square = 0.0
    for variation, weight in weights.items():
        expected = total * weight
        chi_square += ((counts[variation] - expected) ** 2) / expected
    return round(math.erfc(math.sqrt(chi_square / 2.0)), 12)


def _full_calendar_days(started: datetime, evaluated: datetime, zone: ZoneInfo) -> int:
    _require(evaluated >= started, "evaluated_at_utc predates full allocation")
    local_start = started.astimezone(zone)
    local_evaluated = evaluated.astimezone(zone)
    start_midnight = datetime.combine(local_start.date(), time.min, tzinfo=zone)
    first_full_day = (
        start_midnight
        if local_start == start_midnight
        else start_midnight + timedelta(days=1)
    )
    if local_evaluated < first_full_day:
        return 0
    return max(0, (local_evaluated.date() - first_full_day.date()).days)


def validate_config(config: Mapping[str, Any]) -> None:
    root = _require_exact_keys(config, EXPECTED_CONFIG_KEYS, "A/A acceptance config")
    _require(root["schema_version"] == 1, "A/A acceptance config schema drift")
    _require(root["experiment_id"] == "vevo-sk-aa-001", "A/A experiment ID drift")
    _require(root["timezone"] == "Europe/Bratislava", "A/A timezone drift")
    _require(root["variations"] == ["control", "variant"], "A/A variation order drift")
    _require(
        root["expected_variation_weights"] == {"control": 0.5, "variant": 0.5},
        "A/A variation weights drift",
    )
    integer_fields = {
        "required_production_allocation_percent": (1, 100),
        "minimum_full_calendar_days": (1, None),
        "minimum_eligible_devices": (1, None),
        "minimum_measured_page_loads_per_arm": (1, None),
        "minimum_exact_joined_transactions": (1, None),
        "minimum_meta_exposures": (1, None),
        "minimum_complete_stable_meta_dimension_exposures": (1, None),
        "privacy_sample_max_rows": (1, None),
        "split_percent_min": (1, 99),
        "split_percent_max": (1, 99),
        "lcp_degradation_absolute_ms": (0, None),
        "inp_degradation_absolute_ms": (0, None),
        "cls_degradation_absolute_milli": (0, None),
    }
    for field, (minimum, maximum) in integer_fields.items():
        value = _integer(root[field], field, maximum=maximum)
        _require(value >= minimum, f"{field} is below its allowed minimum")
    _require(
        root["split_percent_min"] < root["split_percent_max"], "A/A split range drift"
    )
    for field in (
        "srm_p_value_min",
        "pipeline_count_difference_max_percent",
        "growthbook_reporting_count_difference_max_percent",
        "duplicate_event_rate_max_percent",
        "exact_order_join_rate_min_percent",
        "lcp_degradation_relative_percent",
        "inp_degradation_relative_percent",
        "client_error_rate_increase_max_percentage_points",
    ):
        _number(root[field], field, minimum=0, maximum=100)


def load_config(path: Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AaEvaluationError("A/A acceptance config is unreadable") from exc
    _require(isinstance(payload, dict), "A/A acceptance config must be an object")
    validate_config(payload)
    return payload


def _validate_snapshot(
    snapshot: Mapping[str, Any], config: Mapping[str, Any]
) -> dict[str, Any]:
    root = dict(_require_exact_keys(snapshot, EXPECTED_SNAPSHOT_KEYS, "A/A snapshot"))
    _require(root["schema_version"] == 1, "A/A snapshot schema drift")
    _require(
        root["experiment_id"] == config["experiment_id"],
        "A/A snapshot experiment drift",
    )
    _integer(
        root["production_allocation_percent"],
        "production_allocation_percent",
        maximum=100,
    )
    _boolean(root["identical_variations_verified"], "identical_variations_verified")
    _boolean(root["growthbook_srm_warning"], "growthbook_srm_warning")
    started = _parse_utc(
        root["full_allocation_started_at_utc"], "full_allocation_started_at_utc"
    )
    evaluated = _parse_utc(root["evaluated_at_utc"], "evaluated_at_utc")
    _require(evaluated >= started, "A/A snapshot evaluation predates full allocation")

    pipeline = _require_exact_keys(
        root["pipeline_counts"], EXPECTED_PIPELINE_KEYS, "pipeline_counts"
    )
    for field in EXPECTED_PIPELINE_KEYS:
        _integer(pipeline[field], f"pipeline_counts.{field}")
    _require(
        pipeline["collector_received_event_count"]
        == pipeline["collector_unique_accepted_event_count"]
        + pipeline["collector_duplicate_event_count"],
        "collector received/unique/duplicate count identity drift",
    )

    variations = set(config["variations"])
    growthbook_counts = _require_exact_keys(
        root["growthbook_variation_counts"], variations, "growthbook_variation_counts"
    )
    for variation in variations:
        _integer(
            growthbook_counts[variation], f"growthbook_variation_counts.{variation}"
        )

    quality = _require_exact_keys(
        root["reporting_quality"], EXPECTED_QUALITY_KEYS, "reporting_quality"
    )
    for field in EXPECTED_QUALITY_KEYS - {"srm_p_value", "variation_health"}:
        _integer(quality[field], f"reporting_quality.{field}")
    _number(
        quality["srm_p_value"], "reporting_quality.srm_p_value", minimum=0, maximum=1
    )
    _require(
        quality["raw_event_count"]
        == quality["unique_event_count"] + quality["duplicate_event_count"],
        "reporting raw/unique/duplicate event identity drift",
    )
    _require(
        pipeline["reporting_unique_event_count"] == quality["unique_event_count"],
        "pipeline/reporting unique event count drift",
    )
    _require(
        quality["exact_joined_transaction_count"]
        <= quality["unique_transaction_count"],
        "exact joined transactions exceed unique transactions",
    )
    _require(
        quality["unmatched_transaction_count"] <= quality["unique_transaction_count"],
        "unmatched transactions exceed unique transactions",
    )
    health = _require_exact_keys(
        quality["variation_health"], variations, "variation_health"
    )
    reporting_counts: dict[str, int] = {}
    for variation in config["variations"]:
        row = _require_exact_keys(
            health[variation],
            EXPECTED_VARIATION_HEALTH_KEYS,
            f"variation_health.{variation}",
        )
        for field in ("eligible_devices", "measured_page_loads"):
            _integer(row[field], f"variation_health.{variation}.{field}")
        for field in ("lcp_p75_ms", "inp_p75_ms", "cls_p75_milli"):
            _number(
                row[field],
                f"variation_health.{variation}.{field}",
                minimum=0,
                nullable=True,
            )
        _number(
            row["client_error_device_rate_pct"],
            f"variation_health.{variation}.client_error_device_rate_pct",
            minimum=0,
            maximum=100,
            nullable=True,
        )
        reporting_counts[variation] = row["eligible_devices"]
    _require(
        sum(reporting_counts.values()) == quality["eligible_device_count"],
        "eligible device count does not equal per-variation counts",
    )
    independent_srm = _srm(reporting_counts, config["expected_variation_weights"])
    _require(
        abs(independent_srm - float(quality["srm_p_value"])) <= 1e-9,
        "reporting SRM value does not match independent recomputation",
    )

    meta = _require_exact_keys(
        root["meta_dimension_audit"], EXPECTED_META_KEYS, "meta_dimension_audit"
    )
    privacy = _require_exact_keys(
        root["privacy_audit"], EXPECTED_PRIVACY_KEYS, "privacy_audit"
    )
    consent = _require_exact_keys(
        root["consent_audit"], EXPECTED_CONSENT_KEYS, "consent_audit"
    )
    for field in EXPECTED_META_KEYS:
        _integer(meta[field], f"meta_dimension_audit.{field}")
    for field in EXPECTED_PRIVACY_KEYS:
        _integer(privacy[field], f"privacy_audit.{field}")
    for field in EXPECTED_CONSENT_KEYS:
        _integer(consent[field], f"consent_audit.{field}")
    _require(
        privacy["sampled_row_count"] <= privacy["total_stored_row_count"],
        "privacy sample exceeds stored row population",
    )

    commerce = _require_exact_keys(
        root["commerce_health"], EXPECTED_COMMERCE_KEYS, "commerce_health"
    )
    for field in (
        "checkout_runtime_error_count",
        "duplicate_ga4_purchase_event_count",
        "duplicate_meta_purchase_event_count",
    ):
        _integer(commerce[field], f"commerce_health.{field}")
    for field in (
        "price_cart_checkout_mutation_observed",
        "add_to_cart_behavior_regression_observed",
        "rollback_test_passed",
    ):
        _boolean(commerce[field], f"commerce_health.{field}")

    qa = _require_exact_keys(root["qa_checklist"], EXPECTED_QA_KEYS, "qa_checklist")
    for field in EXPECTED_QA_KEYS:
        _boolean(qa[field], f"qa_checklist.{field}")
    return {
        "root": root,
        "started": started,
        "evaluated": evaluated,
        "pipeline": pipeline,
        "growthbook_counts": growthbook_counts,
        "quality": quality,
        "health": health,
        "reporting_counts": reporting_counts,
        "meta": meta,
        "privacy": privacy,
        "consent": consent,
        "commerce": commerce,
        "qa": qa,
    }


def evaluate(snapshot: Mapping[str, Any], config: Mapping[str, Any]) -> dict[str, Any]:
    """Return a deterministic sanitized decision for one A/A snapshot."""

    validate_config(config)
    values = _validate_snapshot(snapshot, config)
    root = values["root"]
    pipeline = values["pipeline"]
    quality = values["quality"]
    health = values["health"]
    reporting_counts = values["reporting_counts"]
    growthbook_counts = values["growthbook_counts"]
    meta = values["meta"]
    privacy = values["privacy"]
    consent = values["consent"]
    commerce = values["commerce"]
    qa = values["qa"]
    full_days = _full_calendar_days(
        values["started"], values["evaluated"], ZoneInfo(config["timezone"])
    )
    eligible_devices = sum(reporting_counts.values())
    gates: list[dict[str, Any]] = []

    def add_gate(key: str, status: str, observed: Any, requirement: Any) -> None:
        gates.append(
            {
                "key": key,
                "status": status,
                "observed": observed,
                "requirement": requirement,
            }
        )

    add_gate(
        "production_allocation",
        "pass"
        if root["production_allocation_percent"]
        == config["required_production_allocation_percent"]
        else "not_ready",
        root["production_allocation_percent"],
        config["required_production_allocation_percent"],
    )
    add_gate(
        "minimum_full_calendar_days",
        "pass" if full_days >= config["minimum_full_calendar_days"] else "not_ready",
        full_days,
        {"minimum": config["minimum_full_calendar_days"]},
    )
    add_gate(
        "minimum_eligible_devices",
        "pass"
        if eligible_devices >= config["minimum_eligible_devices"]
        else "not_ready",
        eligible_devices,
        {"minimum": config["minimum_eligible_devices"]},
    )
    add_gate(
        "identical_variations",
        "pass" if root["identical_variations_verified"] else "fail",
        root["identical_variations_verified"],
        True,
    )

    reporting_split = {
        variation: (
            round(100.0 * count / eligible_devices, 6) if eligible_devices else None
        )
        for variation, count in reporting_counts.items()
    }
    sample_ready = eligible_devices >= config["minimum_eligible_devices"]
    split_ok = sample_ready and all(
        config["split_percent_min"] <= value <= config["split_percent_max"]
        for value in reporting_split.values()
        if value is not None
    )
    add_gate(
        "eligible_exposure_split",
        "pass" if split_ok else ("fail" if sample_ready else "not_ready"),
        reporting_split,
        {
            "minimum_percent": config["split_percent_min"],
            "maximum_percent": config["split_percent_max"],
        },
    )
    srm_ok = quality["srm_p_value"] >= config["srm_p_value_min"]
    add_gate(
        "independent_srm",
        "pass"
        if srm_ok and sample_ready
        else ("fail" if sample_ready else "not_ready"),
        quality["srm_p_value"],
        {"minimum_p_value": config["srm_p_value_min"]},
    )
    add_gate(
        "growthbook_srm_warning",
        "pass" if not root["growthbook_srm_warning"] else "fail",
        root["growthbook_srm_warning"],
        False,
    )

    collector_athena_diff = _percentage_difference(
        pipeline["athena_unique_event_count"],
        pipeline["collector_unique_accepted_event_count"],
    )
    athena_reporting_diff = _percentage_difference(
        pipeline["reporting_unique_event_count"], pipeline["athena_unique_event_count"]
    )
    pipeline_has_events = pipeline["collector_unique_accepted_event_count"] > 0
    pipeline_ok = (
        collector_athena_diff is not None
        and athena_reporting_diff is not None
        and collector_athena_diff <= config["pipeline_count_difference_max_percent"]
        and athena_reporting_diff <= config["pipeline_count_difference_max_percent"]
    )
    add_gate(
        "collector_athena_reporting_count_parity",
        "pass"
        if pipeline_ok and pipeline_has_events
        else ("fail" if pipeline_has_events else "not_ready"),
        {
            "collector_to_athena_difference_percent": collector_athena_diff,
            "athena_to_reporting_difference_percent": athena_reporting_diff,
        },
        {"maximum_percent": config["pipeline_count_difference_max_percent"]},
    )
    duplicate_rate = (
        round(
            100.0
            * pipeline["collector_duplicate_event_count"]
            / pipeline["collector_received_event_count"],
            6,
        )
        if pipeline["collector_received_event_count"]
        else None
    )
    add_gate(
        "duplicate_accepted_event_rate",
        "not_ready"
        if duplicate_rate is None
        else (
            "pass"
            if duplicate_rate <= config["duplicate_event_rate_max_percent"]
            else "fail"
        ),
        duplicate_rate,
        {"maximum_percent": config["duplicate_event_rate_max_percent"]},
    )
    add_gate(
        "orphan_events",
        "pass" if quality["orphan_event_count"] == 0 else "fail",
        quality["orphan_event_count"],
        0,
    )
    add_gate(
        "variation_contamination",
        "pass" if quality["contaminated_device_count"] == 0 else "fail",
        quality["contaminated_device_count"],
        0,
    )

    growthbook_reporting_diffs = {
        variation: _percentage_difference(
            growthbook_counts[variation], reporting_counts[variation]
        )
        for variation in config["variations"]
    }
    count_population_ready = (
        eligible_devices > 0 and sum(growthbook_counts.values()) > 0
    )
    growthbook_reporting_ok = count_population_ready and all(
        value is not None
        and value <= config["growthbook_reporting_count_difference_max_percent"]
        for value in growthbook_reporting_diffs.values()
    )
    add_gate(
        "growthbook_reporting_variation_parity",
        "pass"
        if growthbook_reporting_ok
        else ("fail" if count_population_ready else "not_ready"),
        growthbook_reporting_diffs,
        {
            "maximum_percent": config[
                "growthbook_reporting_count_difference_max_percent"
            ]
        },
    )

    transaction_count = quality["unique_transaction_count"]
    exact_joined = quality["exact_joined_transaction_count"]
    exact_join_rate = (
        round(100.0 * exact_joined / transaction_count, 6)
        if transaction_count
        else None
    )
    join_sample_ready = exact_joined >= config["minimum_exact_joined_transactions"]
    join_ok = (
        join_sample_ready
        and exact_join_rate is not None
        and exact_join_rate >= config["exact_order_join_rate_min_percent"]
        and quality["ambiguous_transaction_count"] == 0
    )
    add_gate(
        "exact_order_join",
        "pass" if join_ok else ("fail" if transaction_count else "not_ready"),
        {
            "unique_transactions": transaction_count,
            "exact_joined_transactions": exact_joined,
            "join_rate_percent": exact_join_rate,
            "ambiguous_transactions": quality["ambiguous_transaction_count"],
        },
        {
            "minimum_joined_transactions": config["minimum_exact_joined_transactions"],
            "minimum_join_rate_percent": config["exact_order_join_rate_min_percent"],
            "ambiguous_transactions": 0,
        },
    )

    meta_ready = (
        meta["meta_exposure_count"] >= config["minimum_meta_exposures"]
        and meta["complete_stable_dimension_exposure_count"]
        >= config["minimum_complete_stable_meta_dimension_exposures"]
    )
    meta_safe = (
        meta["invalid_dimension_row_count"] == 0
        and meta["forbidden_click_identifier_count"] == 0
    )
    add_gate(
        "meta_dimension_contract",
        "pass"
        if meta_ready and meta_safe
        else ("fail" if not meta_safe else "not_ready"),
        dict(meta),
        {
            "minimum_meta_exposures": config["minimum_meta_exposures"],
            "minimum_complete_stable_dimension_exposures": config[
                "minimum_complete_stable_meta_dimension_exposures"
            ],
            "invalid_dimension_rows": 0,
            "forbidden_click_identifiers": 0,
        },
    )

    required_privacy_sample = min(
        privacy["total_stored_row_count"], config["privacy_sample_max_rows"]
    )
    privacy_ready = (
        required_privacy_sample > 0
        and privacy["sampled_row_count"] >= required_privacy_sample
    )
    privacy_findings = sum(
        privacy[field]
        for field in EXPECTED_PRIVACY_KEYS
        if field.endswith("_count")
        and field not in {"total_stored_row_count", "sampled_row_count"}
    )
    add_gate(
        "privacy_sample",
        "pass"
        if privacy_ready and privacy_findings == 0
        else ("fail" if privacy_findings else "not_ready"),
        {
            "sampled_rows": privacy["sampled_row_count"],
            "total_rows": privacy["total_stored_row_count"],
            "finding_count": privacy_findings,
        },
        {"minimum_sampled_rows": required_privacy_sample, "finding_count": 0},
    )
    consent_violations = sum(consent.values())
    add_gate(
        "consent_boundary",
        "pass" if consent_violations == 0 else "fail",
        dict(consent),
        {field: 0 for field in sorted(EXPECTED_CONSENT_KEYS)},
    )

    commerce_failures = (
        commerce["checkout_runtime_error_count"]
        + commerce["duplicate_ga4_purchase_event_count"]
        + commerce["duplicate_meta_purchase_event_count"]
        + int(commerce["price_cart_checkout_mutation_observed"])
        + int(commerce["add_to_cart_behavior_regression_observed"])
        + int(not commerce["rollback_test_passed"])
    )
    add_gate(
        "commerce_health_and_rollback",
        "pass" if commerce_failures == 0 else "fail",
        dict(commerce),
        {
            "all_error_and_duplicate_counts": 0,
            "mutation_or_regression_observed": False,
            "rollback_test_passed": True,
        },
    )
    qa_failures = [field for field, passed in qa.items() if not passed]
    add_gate(
        "desktop_mobile_consent_qa",
        "pass" if not qa_failures else "fail",
        {"failed_checks": sorted(qa_failures)},
        {"failed_checks": []},
    )

    control = health["control"]
    variant = health["variant"]
    performance_sample_ready = all(
        health[variation]["measured_page_loads"]
        >= config["minimum_measured_page_loads_per_arm"]
        for variation in config["variations"]
    )
    performance_values_complete = all(
        health[variation][field] is not None
        for variation in config["variations"]
        for field in (
            "lcp_p75_ms",
            "inp_p75_ms",
            "cls_p75_milli",
            "client_error_device_rate_pct",
        )
    )
    performance_ok = False
    performance_observed: dict[str, Any] = {
        "control_measured_page_loads": control["measured_page_loads"],
        "variant_measured_page_loads": variant["measured_page_loads"],
    }
    if performance_sample_ready and performance_values_complete:
        lcp_limit = max(
            config["lcp_degradation_absolute_ms"],
            control["lcp_p75_ms"] * config["lcp_degradation_relative_percent"] / 100.0,
        )
        inp_limit = max(
            config["inp_degradation_absolute_ms"],
            control["inp_p75_ms"] * config["inp_degradation_relative_percent"] / 100.0,
        )
        deltas = {
            "lcp_increase_ms": round(variant["lcp_p75_ms"] - control["lcp_p75_ms"], 6),
            "inp_increase_ms": round(variant["inp_p75_ms"] - control["inp_p75_ms"], 6),
            "cls_increase_milli": round(
                variant["cls_p75_milli"] - control["cls_p75_milli"], 6
            ),
            "client_error_rate_increase_percentage_points": round(
                variant["client_error_device_rate_pct"]
                - control["client_error_device_rate_pct"],
                6,
            ),
        }
        performance_observed.update(deltas)
        performance_ok = (
            deltas["lcp_increase_ms"] <= lcp_limit
            and deltas["inp_increase_ms"] <= inp_limit
            and deltas["cls_increase_milli"] <= config["cls_degradation_absolute_milli"]
            and deltas["client_error_rate_increase_percentage_points"]
            <= config["client_error_rate_increase_max_percentage_points"]
        )
    add_gate(
        "performance_guardrails",
        "pass"
        if performance_ok
        else ("fail" if performance_sample_ready else "not_ready"),
        performance_observed,
        {
            "minimum_page_loads_per_arm": config["minimum_measured_page_loads_per_arm"],
            "lcp_increase_max": "max(200ms,10%)",
            "inp_increase_max": "max(20ms,10%)",
            "cls_increase_max_milli": config["cls_degradation_absolute_milli"],
            "client_error_rate_increase_max_percentage_points": config[
                "client_error_rate_increase_max_percentage_points"
            ],
        },
    )

    statuses = {gate["status"] for gate in gates}
    verdict = (
        "FAIL"
        if "fail" in statuses
        else ("NOT_READY" if "not_ready" in statuses else "PASS")
    )
    return {
        "schema_version": 1,
        "evidence_type": "vevo_growthbook_aa_acceptance_decision",
        "experiment_id": config["experiment_id"],
        "evaluated_at_utc": root["evaluated_at_utc"],
        "verdict": verdict,
        "winner_calls_allowed": False,
        "summary": {
            "full_calendar_days": full_days,
            "eligible_devices": eligible_devices,
            "reporting_variation_counts": dict(reporting_counts),
            "growthbook_variation_counts": dict(growthbook_counts),
            "reporting_split_percent": reporting_split,
            "srm_p_value": quality["srm_p_value"],
            "exact_order_join_rate_percent": exact_join_rate,
            "duplicate_event_rate_percent": duplicate_rate,
            "complete_stable_meta_dimension_exposures": meta[
                "complete_stable_dimension_exposure_count"
            ],
        },
        "gates": gates,
    }


def _canonical_json(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _write_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=str(path.parent)
    )
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(_canonical_json(payload))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    except Exception:
        try:
            os.unlink(temporary_name)
        except OSError:
            pass
        raise


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", required=True, type=Path)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--require-pass", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        snapshot = json.loads(args.snapshot.read_text(encoding="utf-8"))
        _require(isinstance(snapshot, dict), "A/A snapshot must contain an object")
        result = evaluate(snapshot, load_config(args.config))
    except (
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        AaEvaluationError,
    ) as exc:
        print(f"VEVO_GROWTHBOOK_AA_EVALUATION_INVALID:{exc}")
        return 2
    if args.output:
        _write_atomic(args.output, result)
    else:
        print(_canonical_json(result).decode("utf-8"), end="")
    print(
        "VEVO_GROWTHBOOK_AA_EVALUATED:"
        f"verdict={result['verdict']}:devices={result['summary']['eligible_devices']}:"
        f"days={result['summary']['full_calendar_days']}:winner=false"
    )
    if args.require_pass and result["verdict"] != "PASS":
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
