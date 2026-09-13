"""Pure gates for one privately reviewed, non-billable COD obligation.

Closing the business obligation never resolves or consumes the original
financial uncertainty. A marker with unknown provenance blocks writes but
cannot hide that uncertainty from the active review/backlog.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
from typing import Any
import unicodedata


CLOSURE_FIELD = "reviewed_uncollected_closure"
# The private explicit confirmation seals scope without publishing business data.
CONFIRMATION_KEY = "data/vevo/order-automation/audits/2026-09-13/reviewed-uncollected-confirmation-20260913.json"
CONFIRMATION_SHA256 = "5b6ea4ae7b557003a58d7848b880120169c893f5d7e027a3be581ed813e6a8d0"
CONFIRMATION_CANONICAL_SHA256 = "5b6ea4ae7b557003a58d7848b880120169c893f5d7e027a3be581ed813e6a8d0"
FINANCIAL_FIELDS = (
    "order_num", "order_id", "phase", "preinvoice_id", "claimed_preinvoice_id",
    "native_preinvoice_key", "attempted_at", "last_creation_failure", "invoice_id",
    "invoice_num", "email_state", "email_policy",
)


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False, allow_nan=False).encode("utf-8")).hexdigest()


def financial_projection(record: Mapping[str, Any]) -> dict[str, Any]:
    return {key: record.get(key) for key in FINANCIAL_FIELDS}


def _identity(value: Any) -> bool:
    return type(value) is str and re.fullmatch(r"[1-9][0-9]*", value) is not None


def _aware_time(value: Any) -> bool:
    try:
        return type(value) is str and datetime.fromisoformat(value.replace("Z", "+00:00")).tzinfo is not None
    except ValueError:
        return False


def _status(value: Any) -> bool:
    return (isinstance(value, Mapping) and _identity(value.get("id"))
            and type(value.get("name")) is str and bool(value["name"].strip()))


def normalized_status_name(value: str) -> str:
    value = "".join(char for char in unicodedata.normalize("NFKD", value) if not unicodedata.combining(char))
    return " ".join(value.casefold().replace("-", " ").replace("\u2013", " ").replace("\u2014", " ").split())


def is_noncollection_target(value: Any) -> bool:
    return _status(value) and normalized_status_name(value["name"]) in {"neprevzate storno", "storno"}


def has_reviewed_closure(record: Any) -> bool:
    """Presence alone blocks automatic financial and status writes."""
    return isinstance(record, Mapping) and CLOSURE_FIELD in record


def validate_confirmation(document: Any) -> bool:
    """Validate the sealed private scope; callers must also verify raw S3 bytes."""
    if (not isinstance(document, dict) or type(document.get("schema_version")) is not int
            or document["schema_version"] != 1 or document.get("kind") != "reviewed_uncollected_cod"
            or document.get("project") != "vevo" or not _identity(document.get("order_id"))
            or not _identity(document.get("order_num")) or not _identity(document.get("payment_reference_id"))
            or not _status(document.get("source_status"))):
        return False
    try:
        total = document["total"]
        if not isinstance(total, dict) or type(total.get("value")) is not str or total.get("currency") != "EUR":
            return False
        amount = Decimal(total["value"])
        if not amount.is_finite() or amount <= 0:
            return False
        confirmation = document["confirmation"]
        if (not isinstance(confirmation, dict) or confirmation.get("source") != "explicit_user_confirmation"
                or confirmation.get("outcome") != "uncollected"
                or confirmation.get("resolution") != "correct_status_and_close_invoice_obligation"
                or not _aware_time(confirmation.get("confirmed_at"))):
            return False
        refs = document["evidence_refs"]
        if (not isinstance(refs, list) or len(refs) != 3 or not all(isinstance(row, dict) for row in refs)
                or {row.get("role") for row in refs} != {"api", "history", "preparation_uncertainty"}):
            return False
        for row in refs:
            if (type(row.get("key")) is not str or not row["key"].startswith("data/vevo/order-automation/")
                    or not row["key"].endswith(".json") or ".." in row["key"].split("/")
                    or re.fullmatch(r"[a-f0-9]{64}", row.get("sha256", "")) is None):
                return False
        prior = document["original_financial_operation"]
        if (not isinstance(prior, dict) or set(prior) != set(FINANCIAL_FIELDS)
                or prior.get("order_num") != document["order_num"] or prior.get("order_id") != document["order_id"]
                or prior.get("phase") != "prepare_ambiguous" or not _aware_time(prior.get("attempted_at"))
                or any(prior.get(key) is not None for key in ("preinvoice_id", "claimed_preinvoice_id",
                    "native_preinvoice_key", "invoice_id", "invoice_num", "email_state"))
                or not isinstance(prior.get("last_creation_failure"), dict)
                or prior["last_creation_failure"].get("stage") != "preparation"):
            return False
        return (re.fullmatch(r"[a-f0-9]{64}", CONFIRMATION_SHA256) is not None
                and bool(CONFIRMATION_KEY)
                and canonical_sha256(document) == CONFIRMATION_CANONICAL_SHA256)
    except (KeyError, TypeError, ValueError, InvalidOperation):
        return False


@dataclass(frozen=True)
class ClosureDecision:
    present: bool
    closed: bool
    reason: str
    regression: bool = False


def closure_decision(record: Any, *, project: str | None, order_num: str | None = None,
                     current_order: Mapping[str, Any] | None = None) -> ClosureDecision:
    if not has_reviewed_closure(record):
        return ClosureDecision(False, False, "no_reviewed_closure")
    invalid = ClosureDecision(True, False, "reviewed_closure_invalid")
    marker = record[CLOSURE_FIELD]
    try:
        if (not isinstance(marker, dict) or type(marker.get("schema_version")) is not int
                or marker["schema_version"] != 1 or marker.get("financial_outcome") != "unknown"):
            return invalid
        evidence = marker["confirmation"]
        if (not isinstance(evidence, dict) or evidence.get("key") != CONFIRMATION_KEY
                or evidence.get("sha256") != CONFIRMATION_SHA256):
            return invalid
        document = evidence["document"]
        if (not validate_confirmation(document) or document["project"] != project
                or document["order_num"] != record.get("order_num")
                or order_num is not None and document["order_num"] != order_num
                or financial_projection(record) != document["original_financial_operation"]
                or marker.get("original_financial_sha256") != canonical_sha256(document["original_financial_operation"])):
            return invalid
        mutation = marker["status_mutation"]
        if (not isinstance(mutation, dict) or mutation != record.get("status_mutation")
                or type(mutation.get("schema_version")) is not int or mutation["schema_version"] != 1
                or re.fullmatch(r"[a-f0-9]{32}", mutation.get("intent_id", "")) is None
                or mutation.get("silent") is not True or not _aware_time(mutation.get("attempted_at"))
                or mutation.get("source_status") != document["source_status"]
                or not is_noncollection_target(mutation.get("target_status"))
                or mutation["target_status"]["id"] == document["source_status"]["id"]
                or mutation.get("state") not in {"intent", "uncertain", "verified"}
                or datetime.fromisoformat(mutation["attempted_at"].replace("Z", "+00:00"))
                < datetime.fromisoformat(document["confirmation"]["confirmed_at"].replace("Z", "+00:00"))):
            return invalid
        if marker.get("state") in {"intent", "uncertain"} and marker.get("invoice_obligation") == "closing":
            return ClosureDecision(True, False, "reviewed_closure_unconfirmed")
        if (marker.get("state") != "verified" or marker.get("invoice_obligation") != "closed"
                or mutation.get("state") != "verified" or not _aware_time(marker.get("verified_at"))):
            return invalid
        if (datetime.fromisoformat(marker["verified_at"].replace("Z", "+00:00"))
                < datetime.fromisoformat(mutation["attempted_at"].replace("Z", "+00:00"))):
            return invalid
        regression = False
        if current_order is not None:
            regression = not _matches_closed_readback(current_order, document, mutation["target_status"])
        return ClosureDecision(True, True, "reviewed_closure_regression" if regression else "reviewed_obligation_closed", regression)
    except (KeyError, TypeError, ValueError):
        return invalid


def _matches_closed_readback(order: Any, document: Mapping[str, Any], target: Mapping[str, Any]) -> bool:
    """A closed obligation cannot authorize a changed or incomplete live context."""
    try:
        if (not isinstance(order, Mapping) or str(order.get("id")) != document["order_id"]
                or order.get("order_num") != document["order_num"] or order.get("blocked") is not False
                or not isinstance(order.get("status"), Mapping)
                or {key: str(order["status"].get(key, "")) for key in ("id", "name")} != target):
            return False
        for key in ("invoices", "preinvoices"):
            if key not in order or not (order[key] is None or isinstance(order[key], list) and not order[key]):
                return False
        money = order["sum"]
        if (not isinstance(money, Mapping) or isinstance(money.get("value"), bool)
                or not isinstance(money.get("currency"), Mapping)
                or money["currency"].get("code") != document["total"]["currency"]):
            return False
        amount = Decimal(str(money.get("value")))
        return amount.is_finite() and amount == Decimal(document["total"]["value"])
    except (KeyError, TypeError, ValueError, InvalidOperation):
        return False
