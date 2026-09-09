#!/usr/bin/env python3
"""Turn creditnoted orders that still count in revenue into Storno orders."""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

from creditnote_export import (
    _creditnote_order_nums,
    build_creditnote_export_rows,
    creditnote_shipped_statuses,
    fetch_creditnote_automation_context,
    fetch_project_creditnotes,
    load_creditnote_status_change_audit,
    normalize_order_num,
    normalize_status_name,
    normalize_creditnote_automation_context,
    parse_date,
    save_creditnote_status_change_audit,
)
from logger_config import get_logger
from order_status_safety import creditnote_coverage_reason, fetch_order_safety_context, status_write_block_reason
from reporting_core import BASE_DEFAULT_PROJECT, load_project_env, load_project_settings, resolve_biznisweb_api_url
from unpaid_order_cancellation import build_client, change_order_status, normalize_text, resolve_target_status_id


logger = get_logger("creditnote_storno_guard")

DEFAULT_TARGET_STATUS_NAME = "Storno"
DEFAULT_LANG_CODE = "SK"
DEFAULT_FINAL_STATUSES = (
    "Storno",
    "Vratene",
    "Vr\u00e1ten\u00e9",
    "Dobropis",
    "Neprevzate - storno",
    "Neprevzat\u00e9 - storno",
    "Stripe - refunded",
    "Stripe - cancelled",
)


@dataclass(frozen=True)
class CreditnoteStornoSettings:
    enabled: bool = False
    target_status_name: str = DEFAULT_TARGET_STATUS_NAME
    target_status_id: Optional[int] = None
    lang_code: str = DEFAULT_LANG_CODE
    only_if_in_realized_revenue: bool = True
    final_statuses: Tuple[str, ...] = DEFAULT_FINAL_STATUSES
    creditnote_created_from: str = ""
    creditnote_created_to: str = ""
    normalized_target_status_name: str = field(init=False)
    normalized_final_statuses: Tuple[str, ...] = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "normalized_target_status_name", normalize_text(self.target_status_name))
        object.__setattr__(
            self,
            "normalized_final_statuses",
            tuple(normalize_text(value) for value in self.final_statuses if normalize_text(value)),
        )


@dataclass
class CreditnoteStornoSummary:
    project: str
    enabled: bool
    dry_run: bool
    target_status_name: str
    target_status_id: Optional[int] = None
    date_from: str = ""
    date_to: str = ""
    fetched_creditnotes: int = 0
    exported_creditnotes: int = 0
    creditnoted_orders: int = 0
    checked_orders: int = 0
    eligible_orders: int = 0
    updated_orders: int = 0
    failed_orders: int = 0
    review_required_orders: int = 0
    partial_creditnote_orders: int = 0
    skipped_by_reason: Dict[str, int] = field(default_factory=dict)
    eligible_order_nums: List[str] = field(default_factory=list)
    updated_order_nums: List[str] = field(default_factory=list)
    failed_order_nums: List[str] = field(default_factory=list)
    eligible_order_statuses: List[Dict[str, Any]] = field(default_factory=list)
    updated_order_statuses: List[Dict[str, Any]] = field(default_factory=list)
    status_audit_path: str = ""
    audit_errors: Dict[str, str] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _tuple_from_settings(value: Any, default: Sequence[str]) -> Tuple[str, ...]:
    if value is None:
        return tuple(default)
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Iterable):
        return tuple(str(item) for item in value if str(item or "").strip())
    return tuple(default)


def _optional_int(value: Any) -> Optional[int]:
    if value in ("", None):
        return None
    return int(value)


def resolve_creditnote_storno_settings(project_settings: Dict[str, Any]) -> CreditnoteStornoSettings:
    raw = project_settings.get("creditnote_storno_guard") or {}
    return CreditnoteStornoSettings(
        enabled=bool(raw.get("enabled", False)),
        target_status_name=str(raw.get("target_status_name") or DEFAULT_TARGET_STATUS_NAME),
        target_status_id=_optional_int(raw.get("target_status_id")),
        lang_code=str(raw.get("lang_code") or DEFAULT_LANG_CODE),
        only_if_in_realized_revenue=bool(raw.get("only_if_in_realized_revenue", True)),
        final_statuses=_tuple_from_settings(raw.get("final_statuses"), DEFAULT_FINAL_STATUSES),
        creditnote_created_from=str(raw.get("creditnote_created_from") or ""),
        creditnote_created_to=str(raw.get("creditnote_created_to") or ""),
    )


def _resolve_creditnote_window(
    settings: CreditnoteStornoSettings,
    date_from: Union[str, date, None] = None,
    date_to: Union[str, date, None] = None,
) -> Tuple[date, date]:
    raw_from = date_from or settings.creditnote_created_from
    raw_to = date_to or settings.creditnote_created_to
    start = parse_date(raw_from, "date_from") if raw_from else date(2000, 1, 1)
    end = parse_date(raw_to, "date_to") if raw_to else datetime.utcnow().date()
    if start > end:
        raise ValueError(f"date_from ({start}) cannot be after date_to ({end})")
    return start, end


def _order_status_name(order: Dict[str, Any]) -> str:
    return str(((order or {}).get("status") or {}).get("name") or "").strip()


def _eligibility_reason(
    order: Optional[Dict[str, Any]],
    decision: Optional[Dict[str, Any]],
    settings: CreditnoteStornoSettings,
) -> str:
    if not order:
        return "order_not_found"
    status_norm = normalize_text(_order_status_name(order))
    if not status_norm:
        return "missing_status"
    if status_norm == settings.normalized_target_status_name:
        return "already_target_status"
    if status_norm in settings.normalized_final_statuses:
        return "already_final_status"
    if order.get("blocked") is not False:
        return "order_blocked_or_block_flag_missing"
    if settings.only_if_in_realized_revenue and not bool((decision or {}).get("included")):
        return "not_in_realized_revenue"
    return "eligible"


def _build_exporter(project: str, project_settings: Dict[str, Any]) -> Any:
    from export_orders import BizniWebExporter

    api_url = resolve_biznisweb_api_url(project, project_settings)
    api_token = os.getenv("BIZNISWEB_API_TOKEN", "").strip()
    if not api_token:
        raise RuntimeError(f"BIZNISWEB_API_TOKEN missing for project '{project}'")
    exporter = BizniWebExporter(
        api_url=api_url,
        api_token=api_token,
        project_name=project,
        output_tag="creditnote_storno_guard",
        enable_period_bundle=False,
    )
    # This runner makes writes. Transport-level retries must never replay them;
    # shared explicit read helpers provide retries only for query operations.
    exporter.client = build_client(project, project_settings)
    return exporter


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _status_audit_entry(
    project: str,
    order: Dict[str, Any],
    settings: CreditnoteStornoSettings,
    target_status_id: Optional[int],
    shipped_statuses: Sequence[str],
) -> Dict[str, Any]:
    status = (order or {}).get("status") or {}
    previous_status = str(status.get("name") or "").strip()
    order_num = normalize_order_num((order or {}).get("order_num") or (order or {}).get("id"))
    return {
        "project": project,
        "order_num": order_num,
        "previous_status": previous_status,
        "previous_status_id": status.get("id"),
        "target_status": settings.target_status_name,
        "target_status_id": target_status_id,
        "sent_before_cancel": normalize_status_name(previous_status) in set(shipped_statuses),
        "source": "creditnote_storno_guard",
    }


def _merge_status_audit(project: str, audit: Dict[str, Any], entry: Dict[str, Any]) -> Dict[str, Any]:
    by_order: Dict[str, Dict[str, Any]] = {}
    for existing in audit.get("orders") or []:
        if not isinstance(existing, dict):
            continue
        order_num = normalize_order_num(existing.get("order_num"))
        if order_num:
            by_order[order_num] = existing

    order_num = normalize_order_num(entry.get("order_num"))
    if order_num:
        by_order[order_num] = entry

    return {
        **audit,
        "project": project,
        "updated_at": _utc_now_iso(),
        "orders": sorted(by_order.values(), key=lambda row: str(row.get("order_num") or "")),
    }


def run_creditnote_storno_guard(
    project_name: str,
    date_from: Union[str, date, None] = None,
    date_to: Union[str, date, None] = None,
    dry_run: bool = False,
    exporter: Optional[Any] = None,
    raw_creditnote_rows: Optional[Sequence[Dict[str, Any]]] = None,
    project_settings: Optional[Dict[str, Any]] = None,
    automation_state_store: Optional[Any] = None,
) -> CreditnoteStornoSummary:
    project = (project_name or BASE_DEFAULT_PROJECT).strip().lower() or BASE_DEFAULT_PROJECT
    os.environ["REPORT_PROJECT"] = project
    loaded_project_env = False
    if project_settings is None:
        load_project_env(project, logger=logger)
        loaded_project_env = True
        project_settings = load_project_settings(project)

    settings = resolve_creditnote_storno_settings(project_settings)
    start, end = _resolve_creditnote_window(settings, date_from=date_from, date_to=date_to)
    summary = CreditnoteStornoSummary(
        project=project,
        enabled=settings.enabled,
        dry_run=dry_run,
        target_status_name=settings.target_status_name,
        date_from=start.isoformat(),
        date_to=end.isoformat(),
    )
    if not settings.enabled:
        logger.info("Creditnote storno guard disabled for project=%s", project)
        return summary

    if raw_creditnote_rows is None:
        raw_rows, reported_total = fetch_project_creditnotes(project)
        summary.fetched_creditnotes = int(reported_total)
    else:
        raw_rows = list(raw_creditnote_rows)
        summary.fetched_creditnotes = len(raw_rows)
    if len(raw_rows) != summary.fetched_creditnotes:
        raise RuntimeError("Creditnote guard refuses an incomplete source scan")
    creditnote_context = normalize_creditnote_automation_context(raw_rows)

    creditnote_rows = build_creditnote_export_rows(project, raw_rows, start, end)
    summary.exported_creditnotes = len(creditnote_rows)
    order_nums = _creditnote_order_nums(creditnote_rows)
    summary.creditnoted_orders = len(order_nums)
    if not order_nums:
        return summary

    if exporter is None:
        if not loaded_project_env:
            load_project_env(project, logger=logger)
        exporter = _build_exporter(project, project_settings)

    target_status_id = resolve_target_status_id(exporter.client, settings)
    summary.target_status_id = target_status_id
    shipped_statuses = creditnote_shipped_statuses(project_settings)

    eligible_orders: List[Dict[str, Any]] = []

    def record_failure(order_num: str, reason: str) -> None:
        if order_num not in summary.failed_order_nums:
            summary.failed_orders += 1
            summary.review_required_orders += 1
            summary.failed_order_nums.append(order_num)
        summary.audit_errors[order_num] = reason

    def inspect(order_num: str, context: Dict[str, List[Dict[str, Any]]]) -> Tuple[Dict[str, Any], str]:
        order = fetch_order_safety_context(exporter.client, order_num)
        included, revenue_reason = exporter._realized_revenue_decision(order)
        reason = _eligibility_reason(order, {"included": included, "reason": revenue_reason}, settings)
        if reason == "eligible":
            coverage = creditnote_coverage_reason(order, context.get(order_num, []))
            reason = "eligible" if coverage == "full_creditnote" else coverage
        return order, reason

    normal_skips = {"already_target_status", "already_final_status", "not_in_realized_revenue", "partial_creditnote"}
    for order_num in order_nums:
        try:
            order, reason = inspect(order_num, creditnote_context)
        except Exception:
            record_failure(order_num, "creditnote_order_inspection_failed")
            continue
        if reason == "eligible":
            eligible_orders.append(order)
        else:
            summary.skipped_by_reason[reason] = summary.skipped_by_reason.get(reason, 0) + 1
            summary.partial_creditnote_orders += int(reason == "partial_creditnote")
            if reason not in normal_skips:
                record_failure(order_num, reason)

    summary.checked_orders = len(order_nums)
    summary.eligible_orders = len(eligible_orders)
    summary.eligible_order_nums = [str(order.get("order_num") or "") for order in eligible_orders]
    summary.eligible_order_statuses = [
        _status_audit_entry(project, order, settings, target_status_id, shipped_statuses)
        for order in eligible_orders
    ]

    logger.info(
        "Creditnote storno guard project=%s creditnoted_orders=%s eligible=%s dry_run=%s",
        project,
        summary.creditnoted_orders,
        summary.eligible_orders,
        dry_run,
    )

    if dry_run or not eligible_orders:
        return summary
    if automation_state_store is None:
        from invoice_automation_state import build_automation_state_store

        automation_state_store = build_automation_state_store(project, project_settings)
    with automation_state_store.lease(owner="creditnote-storno") as journal:
        journal.assert_owned()
        status_audit = load_creditnote_status_change_audit(project, project_settings, strict=True)
        for order in eligible_orders:
            journal.assert_owned()
            order_num = str(order["order_num"])
            prior = journal.get_order(order_num)
            blocked_reason = status_write_block_reason(
                prior, next_reason="full_creditnote", next_target_status_name=settings.target_status_name
            )
            if blocked_reason:
                record_failure(order_num, blocked_reason)
                journal.update_order(order_num, status_review_reason=blocked_reason)
                continue
            journal.assert_owned()
            try:
                # Re-read the complete creditnote context and current order before
                # each write. Explicit injected rows are immutable test evidence.
                fresh_context = creditnote_context if raw_creditnote_rows is not None else fetch_creditnote_automation_context(project, progress_callback=journal.assert_owned)
                live_order, reason = inspect(order_num, fresh_context)
                journal.assert_owned()
            except Exception:
                record_failure(order_num, "creditnote_pre_mutation_recheck_failed")
                continue
            if reason != "eligible":
                summary.skipped_by_reason[reason] = summary.skipped_by_reason.get(reason, 0) + 1
                summary.partial_creditnote_orders += int(reason == "partial_creditnote")
                if reason not in normal_skips:
                    record_failure(order_num, reason)
                continue
            audit_entry = _status_audit_entry(project, live_order, settings, target_status_id, shipped_statuses)
            audit_entry.update(changed_at=_utc_now_iso(), change_result="pending")
            status_audit = _merge_status_audit(project, status_audit, audit_entry)
            # Preserve actual pre-change fulfillment durably before the write;
            # inability to save it must prevent cancellation, not lose history.
            summary.status_audit_path = str(save_creditnote_status_change_audit(project, status_audit, project_settings, strict=True))
            journal.assert_owned()
            mutation_record = {
                "state": "pending", "source_status": live_order.get("status"),
                "source_last_change": live_order.get("last_change"),
                "target_status_id": target_status_id, "target_status_name": settings.target_status_name,
                "reason": "full_creditnote", "creditnote_ids": [row["id"] for row in fresh_context[order_num]],
            }
            journal.update_order(order_num, status_mutation=mutation_record)
            journal.assert_owned()
            try:
                change_order_status(exporter.client, order_num, target_status_id, settings.target_status_name, silent=True)
            except Exception:
                journal.update_order(order_num, status_mutation={**mutation_record, "state": "uncertain"})
                record_failure(order_num, "creditnote_status_mutation_unverified")
                continue
            journal.update_order(order_num, status_mutation={**mutation_record, "state": "verified"}, status_review_reason=None)
            summary.updated_orders += 1
            summary.updated_order_nums.append(order_num)
            audit_entry.update(changed_at=_utc_now_iso(), change_result="updated")
            summary.updated_order_statuses.append(audit_entry)
            status_audit = _merge_status_audit(project, status_audit, audit_entry)
            try:
                summary.status_audit_path = str(save_creditnote_status_change_audit(project, status_audit, project_settings, strict=True))
            except Exception:
                record_failure(order_num, "creditnote_verified_write_audit_save_failed")
            logger.info("Changed creditnoted order %s to status_id=%s", order_num, target_status_id)

    return summary
