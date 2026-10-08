"""Pure, source-bound revenue adjustments for issued partial credit notes.

An issued credit is document evidence, not proof of a cash repayment or stock
return. Its net amount reduces order-level contribution. Without credited line
evidence it must never be attributed to merchandise, shipping, or a product.
The caller owns current-status eligibility and the report's purchase-date window.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib
import json
from typing import Any


class CreditAdjustmentError(ValueError):
    """Financial-source ambiguity which must prevent publication."""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


def _fail(reason: str) -> None:
    raise CreditAdjustmentError(reason)


def _identity(value: Any, reason: str) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        _fail(reason)
    result = str(value).strip()
    if not result:
        _fail(reason)
    return result


def _money(value: Any, reason: str) -> Decimal:
    if value is None or isinstance(value, bool):
        _fail(reason)
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        _fail(reason)
    if not result.is_finite():
        _fail(reason)
    return result


def _native_order_totals(
    order: Mapping[str, Any], project: str, currency: str,
    proof: Mapping[str, Any] | None,
) -> tuple[Decimal, Decimal, str]:
    """Use explicit VAT totals or a caller's already verified native-total proof.

    The optional proof is not an inference API: its producer must have reconciled
    the exact source item/service amounts before calling this function. Identity,
    currency, grand total and any available VAT summary are checked again here.
    """
    payable = _money(order["sum"].get("value"), "credit_order_total_missing")
    if payable <= 0:
        _fail("credit_order_total_nonpositive")
    candidates: list[tuple[Decimal, Decimal, str]] = []
    taxation = order.get("vat_summary")
    if taxation is not None and not isinstance(taxation, list):
        _fail("credit_order_vat_summary_invalid")
    if taxation:
        net = Decimal(0)
        tax = Decimal(0)
        for row in taxation:
            if not isinstance(row, Mapping):
                _fail("credit_order_vat_summary_invalid")
            base = _money(row.get("tax_base"), "credit_order_vat_summary_invalid")
            vat = _money(row.get("amount"), "credit_order_vat_summary_invalid")
            if base < 0 or vat < 0:
                _fail("credit_order_vat_summary_invalid")
            net += base
            tax += vat
        candidates.append((net, net + tax, "source_vat_summary"))
    if proof is not None:
        if not isinstance(proof, Mapping):
            _fail("credit_order_totals_proof_invalid")
        if (
            proof.get("project") != project
            or proof.get("order_num") != order.get("order_num")
            or str(proof.get("order_id") or "") != str(order.get("id"))
            or proof.get("currency") != currency
            or proof.get("basis") not in {
                "verified_item_service_totals", "explicit_native_totals",
                "zero_vat_source_totals",
            }
        ):
            _fail("credit_order_totals_proof_unbound")
        candidates.append((
            _money(proof.get("net_amount"), "credit_order_totals_proof_invalid"),
            _money(proof.get("gross_amount"), "credit_order_totals_proof_invalid"),
            str(proof["basis"]),
        ))
    if not candidates:
        _fail("credit_order_tax_basis_unknown")
    for net, gross, _basis in candidates:
        if net <= 0 or gross < net or abs(gross - payable) > Decimal("0.01"):
            _fail("credit_order_totals_not_reconciled")
        if abs(net - candidates[0][0]) > Decimal("0.01"):
            _fail("credit_order_totals_proof_conflict")
    # The payable source amount remains the full-credit boundary even when its
    # native VAT summary differs by a permitted final cent of rounding.
    return candidates[0][0], payable, candidates[0][2]


def build_order_credit_adjustment(
    order: Mapping[str, Any],
    creditnotes: Sequence[Mapping[str, Any]],
    *,
    project: str,
    source_project: str,
    source_complete: bool,
    financially_included: bool,
    currency_rates_to_eur: Mapping[str, Any],
    verified_order_totals: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return one adjustment for an order from a verified complete document set.

    ``creditnotes`` uses the native normalized context: id, number, numbered,
    state, order_num, order_id, invoice_number, currency, amount (gross) and
    net_amount. Draft/open and voided notes do not reduce revenue. Excluded
    orders are audit-only: their revenue has already been removed by the caller.
    A full credit on an included order needs explicit eligibility reconciliation
    and fails closed instead of silently changing the inclusion policy.

    Returned monetary amounts are Decimal. EUR adjustments are negative and
    satisfy net + tax == gross after cent rounding. COGS is always unchanged.
    Apply the returned adjustment once per order, never once per item row.
    """
    if not isinstance(project, str) or not project or project != source_project:
        _fail("credit_source_project_mismatch")
    if source_complete is not True:
        _fail("credit_source_incomplete")
    if type(financially_included) is not bool:
        _fail("credit_order_eligibility_unknown")
    if not isinstance(order, Mapping):
        _fail("credit_order_invalid")
    order_num = _identity(order.get("order_num"), "credit_order_identity_unknown")
    order_id = _identity(order.get("id"), "credit_order_identity_unknown")
    try:
        purchase_date = datetime.fromisoformat(order["pur_date"]).date().isoformat()
    except (KeyError, TypeError, ValueError):
        _fail("credit_order_date_unknown")
    if not isinstance(creditnotes, (list, tuple)):
        _fail("credit_document_set_invalid")
    zero = Decimal(0)
    result: dict[str, Any] = {
        "project": project, "order_num": order_num, "order_id": order_id,
        "purchase_date": purchase_date, "status": "no_issued_credit",
        "creditnote_ids": [], "creditnote_numbers": [], "ignored_documents": [],
        "native_currency": None, "credit_net_original": zero,
        "credit_gross_original": zero, "credit_tax_original": zero,
        "revenue_credit_adjustment": zero, "credit_gross_adjustment": zero,
        "credit_tax_adjustment": zero, "cogs_adjustment": zero,
        "allocation": "unallocated_order_credit", "product_allocation": None,
        "recognition": "purchase_date_current_document_state",
        "settlement_basis": "issued_document_not_cash_repayment",
    }
    seen_ids: set[str] = set()
    seen_numbers: set[str] = set()
    issued = []
    for document in creditnotes:
        if not isinstance(document, Mapping):
            _fail("credit_document_invalid")
        identity = _identity(document.get("id"), "credit_document_identity_unknown")
        if identity in seen_ids:
            _fail("credit_document_duplicate")
        seen_ids.add(identity)
        if (
            document.get("order_num") != order_num
            or _identity(document.get("order_id"), "credit_document_order_unknown") != order_id
        ):
            _fail("credit_document_order_mismatch")
        if not financially_included:
            result["ignored_documents"].append({"id": identity, "reason": "order_excluded"})
            continue
        state = document.get("state")
        if state in {"open", "voided"}:
            result["ignored_documents"].append({"id": identity, "reason": state})
            continue
        if state != "issued" or document.get("numbered") is not True:
            _fail("credit_document_state_unknown")
        number = _identity(document.get("number"), "credit_document_number_unknown")
        if number in seen_numbers:
            _fail("credit_document_number_duplicate")
        seen_numbers.add(number)
        issued.append(document)
    if not financially_included:
        result["status"] = "order_excluded_no_adjustment"
        return result
    if not issued:
        return result

    total = order.get("sum")
    if not isinstance(total, Mapping) or not isinstance(total.get("currency"), Mapping):
        _fail("credit_order_currency_unknown")
    currency = _identity(total["currency"].get("code"), "credit_order_currency_unknown")
    if currency != currency.upper():
        _fail("credit_order_currency_unknown")
    invoices = order.get("invoices")
    if not isinstance(invoices, list) or not invoices:
        _fail("credit_order_invoice_unknown")
    invoice_numbers = set()
    for invoice in invoices:
        if not isinstance(invoice, Mapping):
            _fail("credit_order_invoice_unknown")
        if _identity(invoice.get("id"), "credit_order_invoice_unknown") != order_id:
            _fail("credit_order_invoice_identity_mismatch")
        number = _identity(invoice.get("invoice_num"), "credit_order_invoice_unknown")
        if number in invoice_numbers:
            _fail("credit_order_invoice_duplicate")
        invoice_numbers.add(number)
    original_net, original_gross, basis = _native_order_totals(order, project, currency, verified_order_totals)
    net = zero
    gross = zero
    fingerprint_documents = []
    for document in issued:
        if document.get("invoice_number") not in invoice_numbers:
            _fail("credit_document_invoice_mismatch")
        if document.get("currency") != currency:
            _fail("credit_document_currency_mismatch")
        doc_net = _money(document.get("net_amount"), "credit_document_amount_unknown")
        doc_gross = _money(document.get("amount"), "credit_document_amount_unknown")
        if doc_net <= 0 or doc_gross < doc_net:
            _fail("credit_document_amount_invalid")
        net += doc_net
        gross += doc_gross
        fingerprint_documents.append({
            "id": str(document["id"]), "number": str(document["number"]),
            "invoice_number": document["invoice_number"], "net": str(doc_net),
            "gross": str(doc_gross), "currency": currency,
        })
    if gross > original_gross or net > original_net + Decimal("0.01"):
        _fail("credit_exceeds_original_order")
    if gross == original_gross:
        _fail("full_credit_on_included_order_requires_reconciliation")
    if gross - net > original_gross - original_net + Decimal("0.01"):
        _fail("credit_tax_exceeds_original_order")
    if original_net == original_gross and gross != net:
        _fail("credit_zero_vat_order_tax_mismatch")
    if not isinstance(currency_rates_to_eur, Mapping) or currency not in currency_rates_to_eur:
        _fail("credit_currency_rate_missing")
    rate = _money(currency_rates_to_eur[currency], "credit_currency_rate_invalid")
    if rate <= 0:
        _fail("credit_currency_rate_invalid")
    net_eur = (net * rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    gross_eur = (gross * rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    fingerprint = {
        "project": project, "order_num": order_num, "order_id": order_id,
        "purchase_date": purchase_date, "currency": currency,
        "order_net": str(original_net), "order_gross": str(original_gross),
        "totals_basis": basis, "eur_rate": str(rate),
        "documents": sorted(fingerprint_documents, key=lambda d: d["id"]),
    }
    result.update({
        "status": "issued_partial_credit_adjusted",
        "creditnote_ids": sorted(str(d["id"]) for d in issued),
        "creditnote_numbers": sorted(str(d["number"]) for d in issued),
        "native_currency": currency, "credit_net_original": net,
        "credit_gross_original": gross, "credit_tax_original": gross - net,
        "revenue_credit_adjustment": -net_eur,
        "credit_gross_adjustment": -gross_eur,
        "credit_tax_adjustment": net_eur - gross_eur,
        "order_totals_basis": basis,
        "source_fingerprint": hashlib.sha256(json.dumps(fingerprint, sort_keys=True).encode()).hexdigest(),
    })
    return result
