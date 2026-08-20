#!/usr/bin/env python3
"""Deterministic, PII-free experiment fact generation.

The browser event stream is anonymous and append-only. Authoritative order value
enters only through the exact, deliberately small ``AuthoritativeOrder`` schema.
No customer or order-line field can cross into a curated GrowthBook row.
"""

from __future__ import annotations

import json
import math
import re
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple


RAW_COMMON_FIELDS = frozenset(
    {
        "schema_version",
        "event_id",
        "event_name",
        "occurred_at",
        "received_at",
        "event_date",
        "device_id",
        "page_path",
        "page_type",
        "consent_state",
        "experiment_id",
        "variation_id",
        "utm_source",
        "utm_medium",
        "meta_campaign_id",
        "meta_adset_id",
        "meta_ad_id",
        "meta_placement",
        "collector_version",
        "risk_result",
    }
)
EVENT_FIELDS = {
    "experiment_exposure": frozenset(),
    "add_to_cart": frozenset({"product_id"}),
    "order_completed": frozenset({"transaction_id"}),
    "performance_vital": frozenset({"page_load_id", "vital_name", "vital_value"}),
    "client_error_observed": frozenset({"page_load_id", "error_kind"}),
}
ALL_EVENT_FIELDS = RAW_COMMON_FIELDS | frozenset().union(*EVENT_FIELDS.values())
ORDER_FIELDS = frozenset(
    {
        "order_num",
        "order_at",
        "net_revenue_eur",
        "cm1_eur",
        "lifecycle_state",
        "mature",
        "excluded",
    }
)
ORDER_STATES = frozenset({"realized", "pending", "cancelled", "refunded"})
VITAL_NAMES = frozenset({"lcp_ms", "inp_ms", "cls_milli"})
SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9-]{2,79}$")

DEVICE_FACT_FIELDS = frozenset(
    {
        "metric_contract_version",
        "experiment_id",
        "device_id",
        "first_exposure_at",
        "variation_id",
        "meta_campaign_id",
        "meta_adset_id",
        "meta_ad_id",
        "meta_placement",
        "add_to_cart_24h",
        "purchase_converted",
        "joined_order_count",
        "net_revenue_eur",
        "cm1_eur",
        "cancelled_order_count",
        "refunded_order_count",
        "immature_order_count",
        "client_error_observed",
        "contaminated",
        "eligible",
        "order_attribution_eligible",
        "order_attribution_issue",
        "unmatched_transaction_count",
        "ambiguous_transaction_count",
        "exclusion_reason",
        "facts_generated_at",
    }
)
PERFORMANCE_FACT_FIELDS = frozenset(
    {
        "metric_contract_version",
        "experiment_id",
        "device_id",
        "variation_id",
        "page_load_id",
        "vital_name",
        "vital_value",
        "measured_at",
        "eligible",
        "exclusion_reason",
        "facts_generated_at",
    }
)


class ExperimentDataError(ValueError):
    """The source data cannot safely produce a decision dataset."""


@dataclass(frozen=True)
class ExperimentBuildConfig:
    metric_contract_version: str
    expected_variation_weights: Mapping[str, Mapping[str, float]]
    cart_window_hours: int = 24
    order_window_days: int = 7
    health_window_hours: int = 24
    maturity_checkpoint_days: int = 14

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[A-Za-z0-9._-]{5,100}", self.metric_contract_version):
            raise ExperimentDataError("invalid metric_contract_version")
        for name, value, upper in (
            ("cart_window_hours", self.cart_window_hours, 168),
            ("order_window_days", self.order_window_days, 30),
            ("health_window_hours", self.health_window_hours, 168),
            ("maturity_checkpoint_days", self.maturity_checkpoint_days, 90),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= upper:
                raise ExperimentDataError(f"invalid {name}")
        for experiment_id, weights in self.expected_variation_weights.items():
            _slug(experiment_id, "experiment_id")
            if len(weights) < 2:
                raise ExperimentDataError(f"{experiment_id} must define at least two variations")
            total = 0.0
            for variation_id, weight in weights.items():
                _slug(variation_id, "variation_id")
                if isinstance(weight, bool) or not math.isfinite(float(weight)) or float(weight) <= 0:
                    raise ExperimentDataError(f"invalid weight for {experiment_id}/{variation_id}")
                total += float(weight)
            if not math.isclose(total, 1.0, rel_tol=0.0, abs_tol=1e-9):
                raise ExperimentDataError(f"weights for {experiment_id} must sum to 1")


@dataclass(frozen=True)
class ExperimentFactBundle:
    device_facts: Tuple[Dict[str, Any], ...]
    performance_facts: Tuple[Dict[str, Any], ...]
    quality_reports: Tuple[Dict[str, Any], ...]


def load_experiment_build_config(path: Path | str) -> ExperimentBuildConfig:
    config_path = Path(path)
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ExperimentDataError("experiment reporting config is unreadable") from exc
    if not isinstance(payload, dict) or set(payload) != {
        "schema_version",
        "metric_contract_version",
        "cart_window_hours",
        "order_window_days",
        "health_window_hours",
        "maturity_checkpoint_days",
        "expected_variation_weights",
    }:
        raise ExperimentDataError("experiment reporting config has an unexpected schema")
    if payload["schema_version"] != 1:
        raise ExperimentDataError("unsupported experiment reporting config version")
    weights = payload["expected_variation_weights"]
    if not isinstance(weights, dict) or not all(isinstance(value, dict) for value in weights.values()):
        raise ExperimentDataError("expected_variation_weights must be an object of objects")
    return ExperimentBuildConfig(
        metric_contract_version=payload["metric_contract_version"],
        expected_variation_weights=weights,
        cart_window_hours=payload["cart_window_hours"],
        order_window_days=payload["order_window_days"],
        health_window_hours=payload["health_window_hours"],
        maturity_checkpoint_days=payload["maturity_checkpoint_days"],
    )


def _slug(value: Any, field: str) -> str:
    if not isinstance(value, str) or not SLUG_RE.fullmatch(value):
        raise ExperimentDataError(f"invalid {field}")
    return value


def _uuid4(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ExperimentDataError(f"invalid {field}")
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError) as exc:
        raise ExperimentDataError(f"invalid {field}") from exc
    if parsed.version != 4 or str(parsed) != value.lower():
        raise ExperimentDataError(f"invalid {field}")
    return value.lower()


def _utc_datetime(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ExperimentDataError(f"invalid {field}")
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ExperimentDataError(f"invalid {field}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExperimentDataError(f"{field} must include an offset")
    return parsed.astimezone(timezone.utc)


def _iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _money(value: Any, field: str) -> Decimal:
    if isinstance(value, bool):
        raise ExperimentDataError(f"invalid {field}")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ExperimentDataError(f"invalid {field}") from exc
    if not parsed.is_finite() or abs(parsed) > Decimal("1000000"):
        raise ExperimentDataError(f"invalid {field}")
    return parsed.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _canonical_for_duplicate(value: Mapping[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _normalize_event(row: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(row, Mapping):
        raise ExperimentDataError("event row must be an object")
    unknown = set(row) - ALL_EVENT_FIELDS
    if unknown:
        raise ExperimentDataError("event row contains non-contract fields")
    missing = RAW_COMMON_FIELDS - set(row)
    if missing:
        raise ExperimentDataError("event row is missing common fields")
    if row.get("schema_version") != 1:
        raise ExperimentDataError("unsupported event schema_version")
    event_name = row.get("event_name")
    if event_name not in EVENT_FIELDS:
        raise ExperimentDataError("unsupported event_name")
    required_specific = EVENT_FIELDS[event_name]
    if required_specific - set(row):
        raise ExperimentDataError("event row is missing event-specific fields")
    for field in frozenset().union(*EVENT_FIELDS.values()) - required_specific:
        if row.get(field) not in (None, ""):
            raise ExperimentDataError("event row mixes event-specific fields")
    if row.get("consent_state") != "analytics_granted" or row.get("risk_result") != "accepted":
        raise ExperimentDataError("event row is not eligible collector output")

    event_id = _uuid4(row.get("event_id"), "event_id")
    device_id = _uuid4(row.get("device_id"), "device_id")
    experiment_id = _slug(row.get("experiment_id"), "experiment_id")
    variation_id = _slug(row.get("variation_id"), "variation_id")
    received_at = _utc_datetime(row.get("received_at"), "received_at")
    _utc_datetime(row.get("occurred_at"), "occurred_at")

    normalized = dict(row)
    normalized.update(
        {
            "event_id": event_id,
            "device_id": device_id,
            "experiment_id": experiment_id,
            "variation_id": variation_id,
            "_received_at": received_at,
        }
    )
    if event_name in {"performance_vital", "client_error_observed"}:
        normalized["page_load_id"] = _uuid4(row.get("page_load_id"), "page_load_id")
    if event_name == "add_to_cart":
        product_id = row.get("product_id")
        if not isinstance(product_id, str) or not re.fullmatch(r"[A-Za-z0-9._-]{1,64}", product_id):
            raise ExperimentDataError("invalid product_id")
    if event_name == "performance_vital":
        if row.get("vital_name") not in VITAL_NAMES:
            raise ExperimentDataError("invalid vital_name")
        vital_value = row.get("vital_value")
        if isinstance(vital_value, bool) or not isinstance(vital_value, int) or not 0 <= vital_value <= 60000:
            raise ExperimentDataError("invalid vital_value")
    if event_name == "client_error_observed" and row.get("error_kind") not in {
        "runtime_error",
        "unhandled_rejection",
    }:
        raise ExperimentDataError("invalid error_kind")
    if event_name == "order_completed":
        transaction_id = row.get("transaction_id")
        if not isinstance(transaction_id, str) or not re.fullmatch(r"[A-Za-z0-9._-]{1,64}", transaction_id):
            raise ExperimentDataError("invalid transaction_id")
    return normalized


def _normalize_order(row: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(row, Mapping) or set(row) != ORDER_FIELDS:
        raise ExperimentDataError("authoritative order must use the exact PII-free schema")
    order_num = row.get("order_num")
    if not isinstance(order_num, str) or not re.fullmatch(r"[A-Za-z0-9._-]{1,64}", order_num):
        raise ExperimentDataError("invalid order_num")
    lifecycle_state = row.get("lifecycle_state")
    if lifecycle_state not in ORDER_STATES:
        raise ExperimentDataError("invalid lifecycle_state")
    if type(row.get("mature")) is not bool or type(row.get("excluded")) is not bool:
        raise ExperimentDataError("order maturity/exclusion flags must be booleans")
    return {
        "order_num": order_num,
        "order_at": _utc_datetime(row.get("order_at"), "order_at"),
        "net_revenue_eur": _money(row.get("net_revenue_eur"), "net_revenue_eur"),
        "cm1_eur": _money(row.get("cm1_eur"), "cm1_eur"),
        "lifecycle_state": lifecycle_state,
        "mature": row["mature"],
        "excluded": row["excluded"],
    }


def _deduplicate_events(
    raw_events: Iterable[Mapping[str, Any]],
) -> Tuple[List[Dict[str, Any]], Counter[str]]:
    by_id: Dict[str, Dict[str, Any]] = {}
    duplicate_counts: Counter[str] = Counter()
    for raw in raw_events:
        event = _normalize_event(raw)
        event_id = event["event_id"]
        existing = by_id.get(event_id)
        if existing is None:
            by_id[event_id] = event
            continue
        existing_compare = {key: value for key, value in existing.items() if not key.startswith("_")}
        event_compare = {key: value for key, value in event.items() if not key.startswith("_")}
        if _canonical_for_duplicate(existing_compare) != _canonical_for_duplicate(event_compare):
            raise ExperimentDataError("one event_id has conflicting payloads")
        duplicate_counts[event["experiment_id"]] += 1
    return sorted(by_id.values(), key=lambda item: (item["_received_at"], item["event_id"])), duplicate_counts


def _deduplicate_orders(raw_orders: Iterable[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    by_num: Dict[str, Dict[str, Any]] = {}
    for raw in raw_orders:
        order = _normalize_order(raw)
        existing = by_num.get(order["order_num"])
        if existing is not None and existing != order:
            raise ExperimentDataError("one order_num has conflicting authoritative facts")
        by_num[order["order_num"]] = order
    return by_num


def _p75(values: Sequence[int]) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * 0.75
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return float(ordered[lower])
    fraction = position - lower
    return round(ordered[lower] + (ordered[upper] - ordered[lower]) * fraction, 3)


def _srm(
    counts: Mapping[str, int], weights: Optional[Mapping[str, float]]
) -> Tuple[Optional[float], Optional[float], bool]:
    if not weights or len(weights) != 2:
        return None, None, False
    total = sum(counts.get(variation, 0) for variation in weights)
    if total == 0:
        return 0.0, 1.0, False
    chi_square = 0.0
    for variation, weight in weights.items():
        expected = total * float(weight)
        chi_square += ((counts.get(variation, 0) - expected) ** 2) / expected
    p_value = math.erfc(math.sqrt(chi_square / 2.0))
    return round(chi_square, 8), round(p_value, 12), p_value < 0.001


def build_experiment_facts(
    raw_events: Iterable[Mapping[str, Any]],
    authoritative_orders: Iterable[Mapping[str, Any]],
    *,
    config: ExperimentBuildConfig,
    generated_at: datetime,
) -> ExperimentFactBundle:
    if generated_at.tzinfo is None or generated_at.utcoffset() is None:
        raise ExperimentDataError("generated_at must be timezone-aware")
    generated_at = generated_at.astimezone(timezone.utc)
    generated_at_text = _iso_utc(generated_at)
    events, duplicate_counts = _deduplicate_events(raw_events)
    orders = _deduplicate_orders(authoritative_orders)

    for event in events:
        weights = config.expected_variation_weights.get(event["experiment_id"])
        if weights is None:
            raise ExperimentDataError("event belongs to an unconfigured experiment")
        if event["variation_id"] not in weights:
            raise ExperimentDataError("event belongs to an unconfigured variation")

    events_by_subject: MutableMapping[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    events_by_experiment: MutableMapping[str, List[Dict[str, Any]]] = defaultdict(list)
    for event in events:
        key = (event["experiment_id"], event["device_id"])
        events_by_subject[key].append(event)
        events_by_experiment[event["experiment_id"]].append(event)

    subjects: Dict[Tuple[str, str], Dict[str, Any]] = {}
    orphan_counts: Counter[str] = Counter()
    for key, subject_events in events_by_subject.items():
        exposures = [event for event in subject_events if event["event_name"] == "experiment_exposure"]
        if not exposures:
            orphan_counts[key[0]] += len(subject_events)
            continue
        first = min(exposures, key=lambda event: (event["_received_at"], event["event_id"]))
        exposure_variations = {event["variation_id"] for event in exposures}
        downstream_variations = {
            event["variation_id"]
            for event in subject_events
            if event["event_name"] != "experiment_exposure"
        }
        variation_contamination = len(exposure_variations) > 1
        variation_mismatch = any(value != first["variation_id"] for value in downstream_variations)
        reasons = []
        if variation_contamination:
            reasons.append("variation_contamination")
        if variation_mismatch:
            reasons.append("variation_mismatch")
        subjects[key] = {
            "events": subject_events,
            "first": first,
            "contaminated": variation_contamination or variation_mismatch,
            "exclusion_reason": "|".join(reasons),
        }

    transaction_subjects: MutableMapping[Tuple[str, str], set[str]] = defaultdict(set)
    subject_transactions: MutableMapping[Tuple[str, str], Dict[str, Dict[str, Any]]] = defaultdict(dict)
    order_window = timedelta(days=config.order_window_days)
    for key, subject in subjects.items():
        first = subject["first"]
        exposure_at = first["_received_at"]
        for event in subject["events"]:
            if event["event_name"] != "order_completed":
                continue
            if event["variation_id"] != first["variation_id"]:
                continue
            if not exposure_at <= event["_received_at"] <= exposure_at + order_window:
                continue
            transaction_id = event["transaction_id"]
            existing = subject_transactions[key].get(transaction_id)
            if existing is None or event["_received_at"] < existing["_received_at"]:
                subject_transactions[key][transaction_id] = event
            transaction_subjects[(key[0], transaction_id)].add(key[1])

    ambiguous_transactions = {
        key for key, devices in transaction_subjects.items() if len(devices) > 1
    }
    device_facts: List[Dict[str, Any]] = []
    performance_facts: List[Dict[str, Any]] = []
    performance_duplicate_counts: Counter[str] = Counter()

    joined_transactions_by_experiment: MutableMapping[str, set[str]] = defaultdict(set)
    unmatched_transactions_by_experiment: MutableMapping[str, set[str]] = defaultdict(set)
    ambiguous_transactions_by_experiment: MutableMapping[str, set[str]] = defaultdict(set)
    attributed_transactions_by_experiment: MutableMapping[str, set[str]] = defaultdict(set)

    for key in sorted(subjects):
        experiment_id, device_id = key
        subject = subjects[key]
        first = subject["first"]
        exposure_at = first["_received_at"]
        variation_id = first["variation_id"]
        eligible = not subject["contaminated"]

        cart_deadline = exposure_at + timedelta(hours=config.cart_window_hours)
        health_deadline = exposure_at + timedelta(hours=config.health_window_hours)
        add_to_cart = any(
            event["event_name"] == "add_to_cart"
            and event["variation_id"] == variation_id
            and exposure_at <= event["_received_at"] <= cart_deadline
            for event in subject["events"]
        )
        client_error = any(
            event["event_name"] == "client_error_observed"
            and event["variation_id"] == variation_id
            and exposure_at <= event["_received_at"] <= health_deadline
            for event in subject["events"]
        )

        performance_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for event in subject["events"]:
            if event["event_name"] != "performance_vital":
                continue
            if event["variation_id"] != variation_id:
                continue
            if not exposure_at <= event["_received_at"] <= health_deadline:
                continue
            perf_key = (event["page_load_id"], event["vital_name"])
            existing = performance_by_key.get(perf_key)
            if existing is None or (event["_received_at"], event["event_id"]) < (
                existing["_received_at"],
                existing["event_id"],
            ):
                if existing is not None:
                    performance_duplicate_counts[experiment_id] += 1
                performance_by_key[perf_key] = event
            else:
                performance_duplicate_counts[experiment_id] += 1
        for perf_key in sorted(performance_by_key):
            event = performance_by_key[perf_key]
            performance_facts.append(
                {
                    "metric_contract_version": config.metric_contract_version,
                    "experiment_id": experiment_id,
                    "device_id": device_id,
                    "variation_id": variation_id,
                    "page_load_id": event["page_load_id"],
                    "vital_name": event["vital_name"],
                    "vital_value": event["vital_value"],
                    "measured_at": _iso_utc(event["_received_at"]),
                    "eligible": int(eligible),
                    "exclusion_reason": subject["exclusion_reason"],
                    "facts_generated_at": generated_at_text,
                }
            )

        joined_orders: List[Dict[str, Any]] = []
        unmatched_count = 0
        ambiguous_count = 0
        outside_window_count = 0
        excluded_count = 0
        for transaction_id, _event in sorted(subject_transactions.get(key, {}).items()):
            transaction_key = (experiment_id, transaction_id)
            order = orders.get(transaction_id)
            if order is None:
                unmatched_count += 1
                unmatched_transactions_by_experiment[experiment_id].add(transaction_id)
                continue
            joined_transactions_by_experiment[experiment_id].add(transaction_id)
            if transaction_key in ambiguous_transactions:
                ambiguous_count += 1
                ambiguous_transactions_by_experiment[experiment_id].add(transaction_id)
                continue
            if not exposure_at <= order["order_at"] <= exposure_at + order_window:
                outside_window_count += 1
                continue
            if order["excluded"]:
                excluded_count += 1
                continue
            joined_orders.append(order)
            attributed_transactions_by_experiment[experiment_id].add(transaction_id)

        order_issues = []
        if unmatched_count:
            order_issues.append("unmatched_transaction")
        if ambiguous_count:
            order_issues.append("ambiguous_transaction_device")
        if outside_window_count:
            order_issues.append("authoritative_order_outside_window")
        if excluded_count:
            order_issues.append("excluded_order")
        if subject["contaminated"]:
            order_issues.append("ineligible_assignment")

        revenue = sum((order["net_revenue_eur"] for order in joined_orders), Decimal("0"))
        cm1 = sum((order["cm1_eur"] for order in joined_orders), Decimal("0"))
        device_facts.append(
            {
                "metric_contract_version": config.metric_contract_version,
                "experiment_id": experiment_id,
                "device_id": device_id,
                "first_exposure_at": _iso_utc(exposure_at),
                "variation_id": variation_id,
                "meta_campaign_id": first.get("meta_campaign_id"),
                "meta_adset_id": first.get("meta_adset_id"),
                "meta_ad_id": first.get("meta_ad_id"),
                "meta_placement": first.get("meta_placement"),
                "add_to_cart_24h": int(add_to_cart),
                "purchase_converted": int(bool(joined_orders)),
                "joined_order_count": len(joined_orders),
                "net_revenue_eur": float(revenue.quantize(Decimal("0.01"))),
                "cm1_eur": float(cm1.quantize(Decimal("0.01"))),
                "cancelled_order_count": sum(
                    order["lifecycle_state"] == "cancelled" for order in joined_orders
                ),
                "refunded_order_count": sum(
                    order["lifecycle_state"] == "refunded" for order in joined_orders
                ),
                "immature_order_count": sum(not order["mature"] for order in joined_orders),
                "client_error_observed": int(client_error),
                "contaminated": int(subject["contaminated"]),
                "eligible": int(eligible),
                "order_attribution_eligible": int(eligible and not order_issues),
                "order_attribution_issue": "|".join(order_issues),
                "unmatched_transaction_count": unmatched_count,
                "ambiguous_transaction_count": ambiguous_count,
                "exclusion_reason": subject["exclusion_reason"],
                "facts_generated_at": generated_at_text,
            }
        )

    quality_reports: List[Dict[str, Any]] = []
    experiments = sorted(set(events_by_experiment) | set(config.expected_variation_weights))
    for experiment_id in experiments:
        experiment_facts = [row for row in device_facts if row["experiment_id"] == experiment_id]
        experiment_perf = [row for row in performance_facts if row["experiment_id"] == experiment_id]
        variation_counts = Counter(row["variation_id"] for row in experiment_facts)
        weights = config.expected_variation_weights.get(experiment_id)
        if weights:
            for variation_id in weights:
                variation_counts.setdefault(variation_id, 0)
        srm_chi_square, srm_p_value, srm_alert = _srm(variation_counts, weights)

        variation_health: Dict[str, Dict[str, Any]] = {}
        all_variations = sorted(set(variation_counts) | {row["variation_id"] for row in experiment_perf})
        for variation_id in all_variations:
            eligible_variation_facts = [
                row
                for row in experiment_facts
                if row["variation_id"] == variation_id and row["eligible"] == 1
            ]
            variation_perf = [
                row
                for row in experiment_perf
                if row["variation_id"] == variation_id and row["eligible"] == 1
            ]
            eligible_count = len(eligible_variation_facts)
            variation_health[variation_id] = {
                "eligible_devices": eligible_count,
                "client_error_devices": sum(
                    row["client_error_observed"] for row in eligible_variation_facts
                ),
                "client_error_device_rate_pct": (
                    round(
                        100.0
                        * sum(row["client_error_observed"] for row in eligible_variation_facts)
                        / eligible_count,
                        4,
                    )
                    if eligible_count
                    else None
                ),
                "measured_page_loads": len({row["page_load_id"] for row in variation_perf}),
                "lcp_p75_ms": _p75(
                    [row["vital_value"] for row in variation_perf if row["vital_name"] == "lcp_ms"]
                ),
                "inp_p75_ms": _p75(
                    [row["vital_value"] for row in variation_perf if row["vital_name"] == "inp_ms"]
                ),
                "cls_p75_milli": _p75(
                    [
                        row["vital_value"]
                        for row in variation_perf
                        if row["vital_name"] == "cls_milli"
                    ]
                ),
            }

        unique_transactions = {
            transaction_id
            for (candidate_experiment, transaction_id), _devices in transaction_subjects.items()
            if candidate_experiment == experiment_id
        }
        joined_transactions = joined_transactions_by_experiment[experiment_id]
        quality_reports.append(
            {
                "metric_contract_version": config.metric_contract_version,
                "experiment_id": experiment_id,
                "facts_generated_at": generated_at_text,
                "raw_event_count": len(events_by_experiment[experiment_id])
                + duplicate_counts[experiment_id],
                "unique_event_count": len(events_by_experiment[experiment_id]),
                "duplicate_event_count": duplicate_counts[experiment_id],
                "orphan_event_count": orphan_counts[experiment_id],
                "exposed_device_count": len(experiment_facts),
                "eligible_device_count": sum(row["eligible"] for row in experiment_facts),
                "contaminated_device_count": sum(row["contaminated"] for row in experiment_facts),
                "variation_counts": dict(sorted(variation_counts.items())),
                "srm_chi_square": srm_chi_square,
                "srm_p_value": srm_p_value,
                "srm_alert": srm_alert,
                "unique_transaction_count": len(unique_transactions),
                "exact_joined_transaction_count": len(joined_transactions),
                "exact_join_rate_pct": (
                    round(100.0 * len(joined_transactions) / len(unique_transactions), 4)
                    if unique_transactions
                    else None
                ),
                "unmatched_transaction_count": len(
                    unmatched_transactions_by_experiment[experiment_id]
                ),
                "ambiguous_transaction_count": len(
                    ambiguous_transactions_by_experiment[experiment_id]
                ),
                "attributed_transaction_count": len(
                    attributed_transactions_by_experiment[experiment_id]
                ),
                "performance_duplicate_count": performance_duplicate_counts[experiment_id],
                "variation_health": variation_health,
            }
        )

    for fact in device_facts:
        if set(fact) != DEVICE_FACT_FIELDS:
            raise AssertionError("device fact schema drift")
    for fact in performance_facts:
        if set(fact) != PERFORMANCE_FACT_FIELDS:
            raise AssertionError("performance fact schema drift")
    return ExperimentFactBundle(
        device_facts=tuple(device_facts),
        performance_facts=tuple(performance_facts),
        quality_reports=tuple(quality_reports),
    )


def _put_json(
    s3: Any,
    *,
    bucket: str,
    key: str,
    payload: Mapping[str, Any],
) -> None:
    body = (json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n").encode(
        "utf-8"
    )
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        CacheControl="no-store",
        ContentType="application/json; charset=utf-8",
        ServerSideEncryption="AES256",
    )


def publish_experiment_facts(
    s3: Any,
    *,
    bucket: str,
    bundle: ExperimentFactBundle,
    prefix: str = "experiment-events/curated",
) -> Dict[str, int]:
    normalized_prefix = prefix.strip(" /")
    if not normalized_prefix or ".." in normalized_prefix:
        raise ExperimentDataError("invalid curated prefix")
    if not isinstance(bucket, str) or not bucket.strip():
        raise ExperimentDataError("invalid bucket")

    for fact in bundle.device_facts:
        if set(fact) != DEVICE_FACT_FIELDS:
            raise ExperimentDataError("invalid device fact schema")
        payload = {key: value for key, value in fact.items() if key != "experiment_id"}
        key = (
            f"{normalized_prefix}/device_facts/experiment_id={fact['experiment_id']}/"
            f"{fact['device_id']}.json"
        )
        _put_json(s3, bucket=bucket, key=key, payload=payload)

    for fact in bundle.performance_facts:
        if set(fact) != PERFORMANCE_FACT_FIELDS:
            raise ExperimentDataError("invalid performance fact schema")
        payload = {key: value for key, value in fact.items() if key != "experiment_id"}
        key = (
            f"{normalized_prefix}/performance_facts/experiment_id={fact['experiment_id']}/"
            f"{fact['page_load_id']}-{fact['vital_name']}.json"
        )
        _put_json(s3, bucket=bucket, key=key, payload=payload)

    for report in bundle.quality_reports:
        experiment_id = _slug(report.get("experiment_id"), "experiment_id")
        generated_at = _utc_datetime(report.get("facts_generated_at"), "facts_generated_at")
        marker = generated_at.strftime("%Y%m%dT%H%M%SZ")
        key = (
            f"{normalized_prefix}/quality/experiment_id={experiment_id}/"
            f"facts_generated_at={marker}.json"
        )
        _put_json(s3, bucket=bucket, key=key, payload=report)

    return {
        "device_facts": len(bundle.device_facts),
        "performance_facts": len(bundle.performance_facts),
        "quality_reports": len(bundle.quality_reports),
    }
