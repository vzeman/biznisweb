"""Private explicit bank-settlement attestations; never synthesize API receipts."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
from typing import Any, Mapping


FIELD = "manual_settlement"
TARGETS = {"Odoslaná", "Platba online - zaplatené"}


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def amount_text(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError("manual_settlement_amount_invalid")
    try:
        amount = Decimal(str(value))
    except InvalidOperation:
        raise ValueError("manual_settlement_amount_invalid") from None
    if not amount.is_finite() or amount <= 0 or amount.adjusted() > 18 or amount.as_tuple().exponent < -8:
        raise ValueError("manual_settlement_amount_invalid")
    text = format(amount, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def positive_id(value: Any) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[1-9][0-9]{0,29}", value) is None:
        raise ValueError("manual_settlement_identity_invalid")
    return value


def validate_proof(proof: Any) -> dict[str, Any]:
    common = {"schema_version", "project", "order_id", "order_num", "source", "confirmed_at", "operation", "supporting_evidence"}
    if not isinstance(proof, dict) or proof.get("project") not in {"roy", "vevo"} or type(proof.get("schema_version")) is not int or proof["schema_version"] != 1:
        raise ValueError("manual_settlement_proof_invalid")
    positive_id(proof.get("order_id"))
    positive_id(proof.get("order_num"))
    refs = proof.get("supporting_evidence")
    if not isinstance(refs, list) or not 1 <= len(refs) <= 4:
        raise ValueError("manual_settlement_supporting_evidence_invalid")
    keys = set()
    for ref in refs:
        if (not isinstance(ref, dict) or set(ref) != {"key", "sha256"} or not isinstance(ref.get("key"), str)
                or re.fullmatch(rf"data/{proof['project']}/order-automation/audits/[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}/[a-zA-Z0-9_-]+\.json", ref["key"]) is None
                or not isinstance(ref.get("sha256"), str) or re.fullmatch(r"[a-f0-9]{64}", ref["sha256"]) is None
                or ref["key"] in keys):
            raise ValueError("manual_settlement_supporting_evidence_invalid")
        keys.add(ref["key"])
    raw_date = proof.get("confirmed_at")
    try:
        stamp = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
        if stamp.tzinfo is None or stamp > datetime.now(timezone.utc):
            raise ValueError()
    except (AttributeError, TypeError, ValueError):
        raise ValueError("manual_settlement_confirmation_date_invalid") from None
    if proof.get("operation") == "confirm":
        if (set(proof) != common | {"total", "currency", "recovery_status"}
                or proof.get("source") != "explicit_user_confirmed_full_external_bank_transfer"
                or not isinstance(proof.get("total"), str) or amount_text(proof["total"]) != proof["total"]
                or not isinstance(proof.get("currency"), str) or re.fullmatch(r"[A-Z]{3}", proof["currency"]) is None):
            raise ValueError("manual_settlement_confirmation_invalid")
        status = proof.get("recovery_status")
        if not isinstance(status, dict) or set(status) != {"id", "name"} or status.get("name") not in TARGETS:
            raise ValueError("manual_settlement_target_invalid")
        positive_id(status.get("id"))
    elif proof.get("operation") == "revoke":
        if (set(proof) != common | {"revokes_sha256"} or proof.get("source") != "explicit_user_withdrawal"
                or not isinstance(proof.get("revokes_sha256"), str) or re.fullmatch(r"[a-f0-9]{64}", proof["revokes_sha256"]) is None):
            raise ValueError("manual_settlement_revocation_invalid")
    else:
        raise ValueError("manual_settlement_operation_invalid")
    return deepcopy(proof)


def proof_reference(proof: Any, key: str, digest: str) -> dict[str, Any]:
    checked = validate_proof(proof)
    actual = hashlib.sha256(canonical_bytes(checked)).hexdigest()
    if digest != actual or key != f"data/{checked['project']}/order-automation/manual-settlements/{actual}.json":
        raise ValueError("manual_settlement_evidence_binding_invalid")
    return {"proof": checked, "evidence_key": key, "evidence_sha256": digest}


def confirmed_proof(record: Any, project: str, order: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(record, dict) or set(record) != {"schema_version", "state", "proof", "evidence_key", "evidence_sha256"}:
        raise ValueError("manual_settlement_record_invalid_or_revoked")
    if type(record.get("schema_version")) is not int or record["schema_version"] != 1 or record.get("state") != "confirmed":
        raise ValueError("manual_settlement_record_invalid_or_revoked")
    proof = proof_reference(record["proof"], record["evidence_key"], record["evidence_sha256"])["proof"]
    total = order.get("sum")
    if (proof["operation"] != "confirm" or proof["project"] != project or str(order.get("id")) != proof["order_id"]
            or str(order.get("order_num")) != proof["order_num"] or not isinstance(total, Mapping)
            or amount_text(total.get("value")) != proof["total"]
            or not isinstance(total.get("currency"), Mapping) or total["currency"].get("code") != proof["currency"]):
        raise ValueError("manual_settlement_current_order_binding_changed")
    return proof


def uncertainty_reason(record: Mapping[str, Any]) -> str:
    """Manual status repair cannot reconcile unfinished financial/email effects."""
    if record.get("phase") not in {None, "complete", "pending", "create_failed", "email"}:
        return "manual_settlement_financial_operation_uncertain"
    if record.get("email_state") not in {None, "pending", "failed", "held", "disabled", "sent"}:
        return "manual_settlement_email_operation_uncertain"
    return ""


def status_attempt_fields(record: Mapping[str, Any], operation: dict) -> dict:
    """Archive a previously verified correction in the same journal CAS write."""
    fields = {"status_mutation": operation}
    previous = record.get("status_mutation")
    if operation.get("manual_settlement_sha256") and previous:
        history = record.get("status_mutation_history", [])
        if (not isinstance(history, list) or any(not isinstance(row, dict) for row in history)
                or not isinstance(previous, dict) or previous.get("state") != "verified"):
            raise ValueError("manual_settlement_previous_attempt_invalid")
        fields["status_mutation_history"] = deepcopy(history) + [deepcopy(previous)]
    return fields


def updated_record(existing: Any, reference: dict, *, project: str, order: Mapping[str, Any]) -> dict:
    """Confirm once or withdraw explicitly; never replace/re-enable a consumed proof."""
    reference = proof_reference(reference["proof"], reference["evidence_key"], reference["evidence_sha256"])
    proof = reference["proof"]
    if (proof["project"] != project or proof["order_id"] != str(order.get("id"))
            or proof["order_num"] != str(order.get("order_num"))):
        raise ValueError("manual_settlement_record_identity_changed")
    if proof["operation"] == "confirm":
        candidate = {"schema_version": 1, "state": "confirmed", **reference}
        confirmed_proof(candidate, project, order)
        if existing is not None and existing != candidate:
            raise ValueError("manual_settlement_existing_proof_cannot_change")
        return candidate
    if not isinstance(existing, dict):
        raise ValueError("manual_settlement_revocation_original_missing")
    original = proof_reference(existing["proof"], existing["evidence_key"], existing["evidence_sha256"])["proof"]
    if (original["operation"] != "confirm" or proof["revokes_sha256"] != existing["evidence_sha256"]
            or any(original[key] != proof[key] for key in ("project", "order_id", "order_num"))):
        raise ValueError("manual_settlement_revocation_identity_changed")
    candidate = {**deepcopy(existing), "state": "revoked", "revocation": reference}
    if existing.get("state") not in {"confirmed", "revoked"} or (existing.get("state") == "revoked" and existing != candidate):
        raise ValueError("manual_settlement_existing_revocation_cannot_change")
    return candidate
