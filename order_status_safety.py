"""Evidence-based decisions and verified, single-attempt order status writes.

Invoices, payments and fulfillment are independent facts. Missing API fields are
unknown evidence, never an empty payment or shipment history. No decision here
infers dispatch from a tracking label or settlement from an invoice's existence.
"""

from __future__ import annotations

import time
import unicodedata
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping

from gql import gql


MONEY_FIELDS = "value is_net_price currency { code }"
RECEIPT_FIELDS = f"id pay_date sum {{ {MONEY_FIELDS} }}"
ORDER_SAFETY_QUERY = gql(
    """
query GetOrderAutomationSafetyContext($order_num: String!) {
  getOrder(order_num: $order_num) {
    id order_num pur_date last_change blocked
    status { id name }
    price_elements { type title value reference_id price { value formatted } }
    shipments { carrier status shipment_number }
    sum { MONEY }
    invoices {
      id invoice_num created paid pay_date sum { MONEY }
      payments { RECEIPT }
      preinvoice { id payments { RECEIPT } }
    }
    preinvoices { id payments { RECEIPT } }
  }
}
""".replace("MONEY", MONEY_FIELDS).replace("RECEIPT", RECEIPT_FIELDS)
)

_CHANGE_STATUS = gql(
    """
mutation ChangeOrderStatusSafely(
  $order_num: String!, $status_id: Int!, $send_notification: [NotificationRequest!]
) {
  changeOrderStatus(order_num: $order_num, status_id: $status_id,
                    send_notification: $send_notification) {
    order_num last_change status { id name }
  }
}
"""
)


def normalize_status(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    return " ".join("".join(c for c in text if not unicodedata.combining(c)).lower().split())


@dataclass(frozen=True)
class Evidence:
    state: str
    reason: str


@dataclass(frozen=True)
class RecoveryDecision:
    action: str
    reason: str
    target_status_name: str = ""


@dataclass(frozen=True)
class _Money:
    amount: Decimal
    currency: str
    net: bool | None


def _money(value: Any) -> _Money | None:
    if not isinstance(value, Mapping) or "is_net_price" not in value:
        return None
    raw = value.get("value")
    if isinstance(raw, bool) or not isinstance(raw, (int, float, str, Decimal)):
        return None
    try:
        amount = Decimal(str(raw))
    except (InvalidOperation, ValueError):
        return None
    currency = value.get("currency")
    code = str(currency.get("code") or "").strip().upper() if isinstance(currency, Mapping) else ""
    net = value.get("is_net_price")
    if not amount.is_finite() or len(code) != 3 or not code.isalpha() or (net is not None and not isinstance(net, bool)):
        return None
    return _Money(amount, code, net)


def _same_basis(left: _Money, right: _Money) -> bool:
    return left.currency == right.currency and left.net is right.net


def api_collection(value: Mapping[str, Any], key: str) -> list[Any] | None:
    """GraphQL nullable list null means empty; an omitted key means unknown."""
    if key not in value:
        return None
    rows = value[key]
    return [] if rows is None else rows if isinstance(rows, list) else None


def assess_payment_evidence(order: Mapping[str, Any]) -> Evidence:
    """Require full settlement; deduplicate receipts shared by invoice/preinvoice.

    A paid invoice must cover exactly the current order total in the same currency
    and tax basis. Otherwise complete, consistent receipt evidence is required.
    Negative receipts are retained, so reversals cannot inflate paid amounts.
    """
    total = _money(order.get("sum"))
    if total is None or total.amount <= 0:
        return Evidence("unknown", "invalid_order_total")
    invoices, preinvoices = api_collection(order, "invoices"), api_collection(order, "preinvoices")
    if invoices is None or preinvoices is None:
        return Evidence("unknown", "missing_payment_context")
    docs: list[Mapping[str, Any]] = []
    paid_invoice = False
    for invoice in invoices:
        if not isinstance(invoice, Mapping) or not invoice.get("id"):
            return Evidence("unknown", "invalid_invoice_context")
        invoice_total = _money(invoice.get("sum"))
        if invoice_total is None or not _same_basis(total, invoice_total):
            return Evidence("unknown", "invoice_amount_or_currency_unknown")
        if not isinstance(invoice.get("paid"), bool):
            return Evidence("unknown", "invoice_payment_flag_unknown")
        paid_invoice |= invoice["paid"] and invoice_total.amount == total.amount
        docs.append(invoice)
        if "preinvoice" not in invoice:
            return Evidence("unknown", "missing_linked_preinvoice_context")
        preinvoice = invoice["preinvoice"]
        if preinvoice is not None:
            if not isinstance(preinvoice, Mapping) or not preinvoice.get("id"):
                return Evidence("unknown", "invalid_preinvoice_context")
            docs.append(preinvoice)
    for preinvoice in preinvoices:
        if not isinstance(preinvoice, Mapping) or not preinvoice.get("id"):
            return Evidence("unknown", "invalid_preinvoice_context")
        docs.append(preinvoice)

    receipts: dict[str, tuple[_Money, str]] = {}
    for document in docs:
        rows = api_collection(document, "payments")
        if rows is None:
            return Evidence("unknown", "missing_receipts")
        for receipt in rows:
            if not isinstance(receipt, Mapping) or not receipt.get("id"):
                return Evidence("unknown", "invalid_receipt")
            amount = _money(receipt.get("sum"))
            pay_date = str(receipt.get("pay_date") or "").strip()
            if amount is None or not _same_basis(total, amount) or not pay_date:
                return Evidence("unknown", "receipt_amount_currency_or_date_unknown")
            identity = str(receipt["id"])
            fingerprint = (amount, pay_date)
            if identity in receipts and receipts[identity] != fingerprint:
                return Evidence("unknown", "conflicting_duplicate_receipt")
            receipts[identity] = fingerprint
    received = sum((entry[0].amount for entry in receipts.values()), Decimal(0))
    if any(entry[0].amount < 0 for entry in receipts.values()):
        return Evidence("unknown", "payment_reversal_requires_review")
    if received >= total.amount:
        return Evidence("confirmed", "full_receipt_settlement")
    if receipts:
        return Evidence("partial", "partial_receipt_settlement")
    if paid_invoice:
        return Evidence("confirmed", "full_invoice_marked_paid")
    return Evidence("unpaid" if not invoices else "unknown", "no_settlement_evidence")


def assess_fulfillment_evidence(order: Mapping[str, Any]) -> Evidence:
    shipments = api_collection(order, "shipments")
    if shipments is None:
        return Evidence("unknown", "missing_shipment_context")
    if not shipments:
        return Evidence("none", "no_shipment_report")
    statuses: list[str] = []
    incomplete = False
    for shipment in shipments:
        if not isinstance(shipment, Mapping) or "status" not in shipment or "shipment_number" not in shipment:
            return Evidence("unknown", "invalid_shipment_context")
        status = str(shipment.get("status") or "").strip().lower()
        number = str(shipment.get("shipment_number") or "").strip()
        # FLOX returns carrier/destination placeholders even before dispatch.
        # They carry neither a shipment status nor a tracking number.
        if not status and not number:
            continue
        statuses.append(status)
        if status in {"returned", "not_delivered"}:
            return Evidence("exception", "shipment_return_or_delivery_exception")
        if status != "delivered" or not number:
            incomplete = True
    if statuses and not incomplete:
        return Evidence("fulfilled", "all_reported_shipments_delivered")
    if not statuses:
        return Evidence("none", "no_shipment_report")
    return Evidence("unknown", "shipment_not_proven_delivered")


def creditnote_coverage_reason(order: Mapping[str, Any], creditnotes: list[dict[str, Any]]) -> str:
    """Only complete credit of the current order permits whole-order Storno."""
    total = _money(order.get("sum"))
    invoices = api_collection(order, "invoices")
    if total is None or total.amount <= 0 or total.net is None or not invoices:
        return "creditnote_order_amount_or_invoice_unknown"
    invoice_ids = {str(row.get("id") or "") for row in invoices if isinstance(row, Mapping)}
    if "" in invoice_ids or len(invoice_ids) != len(invoices):
        return "creditnote_invoice_identity_unknown"
    credited = Decimal(0)
    seen: set[str] = set()
    if not creditnotes:
        return "creditnote_missing"
    for document in creditnotes:
        if not isinstance(document, Mapping):
            return "creditnote_document_invalid"
        identity = str(document.get("id") or "")
        if not identity or identity in seen or str(document.get("invoice_id") or "") not in invoice_ids:
            return "creditnote_identity_or_invoice_mismatch"
        seen.add(identity)
        if document.get("currency") != total.currency:
            return "creditnote_currency_mismatch"
        raw_amount = document.get("net_amount" if total.net else "amount")
        if isinstance(raw_amount, bool) or raw_amount is None:
            return "creditnote_amount_unknown"
        try:
            amount = Decimal(str(raw_amount))
        except InvalidOperation:
            return "creditnote_amount_unknown"
        if not amount.is_finite() or amount <= 0:
            return "creditnote_amount_unknown"
        credited += amount
    if credited < total.amount:
        return "partial_creditnote"
    if credited > total.amount:
        return "creditnote_exceeds_current_order"
    return "full_creditnote"


_PROTECTED_STATUSES = frozenset(normalize_status(name) for name in (
    "Odoslaná", "Pripravené k odberu", "Storno", "Vrátené", "Dobropis",
    "Neprevzaté - storno", "Stripe - refunded", "Nezaplatená - zrušená objednávka",
))


def decide_recovery(
    order: Mapping[str, Any],
    source_statuses: Iterable[str],
    *,
    paid_target: str = "Platba online - zaplatené",
    shipped_target: str = "Odoslaná",
    creditnote_status: str = "unknown",
    verified_previous_status: str | None = None,
) -> RecoveryDecision:
    """A safe automatic recovery never substitutes an invoice for payment proof."""
    status = normalize_status((order.get("status") or {}).get("name"))
    if status in _PROTECTED_STATUSES:
        return RecoveryDecision("skip", "protected_order_status")
    if not status or status not in {normalize_status(name) for name in source_statuses}:
        return RecoveryDecision("skip", "not_recovery_status")
    if order.get("blocked") is not False:
        return RecoveryDecision("review", "order_blocked_or_block_flag_missing")
    if creditnote_status != "clear":
        return RecoveryDecision("review", "creditnote_present" if creditnote_status == "present" else "creditnote_context_missing")
    payment = assess_payment_evidence(order)
    if payment.state != "confirmed":
        return RecoveryDecision("review", payment.reason)
    fulfillment = assess_fulfillment_evidence(order)
    if fulfillment.state == "fulfilled":
        return RecoveryDecision("shipped", fulfillment.reason, shipped_target)
    if fulfillment.state == "exception":
        return RecoveryDecision("review", fulfillment.reason)
    if fulfillment.reason in {"missing_shipment_context", "invalid_shipment_context"}:
        return RecoveryDecision("review", fulfillment.reason)
    # Absence of a shipment report is not absence of a previous dispatch: some
    # integrations only record tracking in history, which this API cannot read.
    # This argument is supplied only by the trusted private runtime journal, not
    # by an order/client field or a source-controlled incident override.
    if verified_previous_status and normalize_status(verified_previous_status) == normalize_status(shipped_target):
        return RecoveryDecision("shipped", "verified_previous_shipped_status", shipped_target)
    if fulfillment.state == "none":
        return RecoveryDecision("review", "shipment_history_unavailable")
    return RecoveryDecision("review", fulfillment.reason)


def execute_read(client: Any, query: Any, *, variable_values: dict[str, Any], attempts: int = 3) -> dict[str, Any]:
    """Retry only an explicitly supplied read, never a mutation or partial data."""
    from graphql import OperationType

    document = getattr(query, "document", query)
    operations = [node for node in document.definitions if hasattr(node, "operation")]
    if not operations or any(node.operation != OperationType.QUERY for node in operations):
        raise ValueError("execute_read requires a query operation")
    for attempt in range(attempts):
        try:
            result = client.execute(query, variable_values=variable_values)
            if not isinstance(result, dict):
                raise RuntimeError("Order API returned an invalid response")
            return result
        except Exception as exc:
            code = getattr(exc, "code", None) or getattr(getattr(exc, "response", None), "status_code", None)
            transient = code in {429, 500, 502, 503, 504} or type(exc).__name__ in {
                "Timeout", "ReadTimeout", "ConnectTimeout", "ConnectionError", "TransportProtocolError",
            }
            if not transient or attempt + 1 == attempts:
                raise
            time.sleep(min(2 ** attempt, 4))
    raise RuntimeError("Order API read was not attempted")


def fetch_order_safety_context(client: Any, order_num: str) -> dict[str, Any]:
    result = execute_read(client, ORDER_SAFETY_QUERY, variable_values={"order_num": str(order_num)})
    order = result.get("getOrder")
    if not isinstance(order, dict) or str(order.get("order_num") or "") != str(order_num):
        raise RuntimeError("Order API detail identity does not match the requested order")
    return order


def change_status_verified(
    client: Any,
    order_num: str,
    status_id: int,
    status_name: str,
    *,
    silent: bool = False,
) -> dict[str, Any]:
    """Make one status write and independently verify it; never replay uncertainty.

    The caller holds the shared per-shop lease and rechecks eligibility directly
    before calling. A no-retry transport is mandatory for real gql clients.
    """
    transport = getattr(client, "transport", None)
    if transport is not None and getattr(transport, "retries", None) != 0:
        raise RuntimeError("Status mutations require a no-retry transport")
    if not isinstance(status_id, int) or isinstance(status_id, bool) or status_id <= 0 or not status_name.strip():
        raise ValueError("Invalid target status")
    variables: dict[str, Any] = {"order_num": str(order_num), "status_id": status_id}
    variables["send_notification"] = [{"type": "EMAIL_CUSTOMER", "if": ["NONE"]}] if silent else None
    try:
        result = client.execute(_CHANGE_STATUS, variable_values=variables)
    except Exception:
        # The request may already have committed. Resolve by a read, never by a
        # second write. If the target cannot be proved, the caller retains its
        # durable pending/uncertain record and requires review on future runs.
        verified = fetch_order_safety_context(client, order_num)
        _validate_target(verified, order_num, status_id, status_name)
        return verified
    changed = result.get("changeOrderStatus") if isinstance(result, dict) else None
    _validate_target(changed, order_num, status_id, status_name)
    verified = fetch_order_safety_context(client, order_num)
    _validate_target(verified, order_num, status_id, status_name)
    return verified


def status_write_block_reason(
    journal_order: Mapping[str, Any], *, next_reason: str = "", next_target_status_name: str = ""
) -> str:
    """Block uncertainty/regressions, allowing a proven subsequent full refund.

    The caller must freshly verify complete creditnote coverage before passing
    full_creditnote. That legitimate terminal transition must not be blocked by
    an earlier verified payment/fulfillment repair. No uncertain write qualifies.
    """
    previous = journal_order.get("status_mutation")
    if not previous:
        return ""
    if not isinstance(previous, Mapping):
        return "invalid_status_mutation_journal"
    if previous.get("state") == "verified":
        if (
            next_reason == "full_creditnote"
            and normalize_status(next_target_status_name) == "storno"
            and normalize_status(previous.get("target_status_name"))
            in {"odoslana", "platba online - zaplatene"}
        ):
            return ""
        return "repeated_status_regression"
    return "previous_status_mutation_unresolved"


def _validate_target(order: Any, order_num: str, status_id: int, status_name: str) -> None:
    if not isinstance(order, Mapping) or str(order.get("order_num") or "") != str(order_num):
        raise RuntimeError("Status mutation outcome identity is unverified")
    status = order.get("status") or {}
    try:
        actual_id = int(status.get("id"))
    except (TypeError, ValueError):
        actual_id = 0
    if actual_id != status_id or normalize_status(status.get("name")) != normalize_status(status_name):
        raise RuntimeError("Status mutation outcome differs from the intended target")
