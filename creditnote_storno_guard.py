#!/usr/bin/env python3
"""Turn creditnoted orders that still count in revenue into Storno orders."""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

from gql import Client

from creditnote_export import (
    _creditnote_order_nums,
    build_creditnote_export_rows,
    fetch_creditnote_orders_by_number,
    fetch_project_creditnotes,
    parse_date,
)
from logger_config import get_logger
from reporting_core import BASE_DEFAULT_PROJECT, load_project_env, load_project_settings, resolve_biznisweb_api_url
from unpaid_order_cancellation import change_order_status, normalize_text, resolve_target_status_id


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
    skipped_by_reason: Dict[str, int] = field(default_factory=dict)
    eligible_order_nums: List[str] = field(default_factory=list)
    updated_order_nums: List[str] = field(default_factory=list)
    failed_order_nums: List[str] = field(default_factory=list)
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
    if settings.only_if_in_realized_revenue and not bool((decision or {}).get("included")):
        return "not_in_realized_revenue"
    return "eligible"


def _build_exporter(project: str, project_settings: Dict[str, Any]) -> Any:
    from export_orders import BizniWebExporter

    api_url = resolve_biznisweb_api_url(project, project_settings)
    api_token = os.getenv("BIZNISWEB_API_TOKEN", "").strip()
    if not api_token:
        raise RuntimeError(f"BIZNISWEB_API_TOKEN missing for project '{project}'")
    return BizniWebExporter(
        api_url=api_url,
        api_token=api_token,
        project_name=project,
        output_tag="creditnote_storno_guard",
        enable_period_bundle=False,
    )


def run_creditnote_storno_guard(
    project_name: str,
    date_from: Union[str, date, None] = None,
    date_to: Union[str, date, None] = None,
    dry_run: bool = False,
    exporter: Optional[Any] = None,
    raw_creditnote_rows: Optional[Sequence[Dict[str, Any]]] = None,
    project_settings: Optional[Dict[str, Any]] = None,
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

    orders, decisions, errors = fetch_creditnote_orders_by_number(exporter, order_nums)
    summary.audit_errors = dict(sorted(errors.items()))
    order_map = {str(order.get("order_num") or "").strip(): order for order in orders if str(order.get("order_num") or "").strip()}

    eligible_orders: List[Dict[str, Any]] = []
    skipped: Dict[str, int] = {}
    for order_num in order_nums:
        order = order_map.get(order_num)
        decision = decisions.get(order_num)
        reason = _eligibility_reason(order, decision, settings)
        if reason == "eligible":
            eligible_orders.append(order or {"order_num": order_num})
        else:
            skipped[reason] = skipped.get(reason, 0) + 1

    summary.checked_orders = len(order_nums)
    summary.eligible_orders = len(eligible_orders)
    summary.skipped_by_reason = dict(sorted(skipped.items()))
    summary.eligible_order_nums = [str(order.get("order_num") or "") for order in eligible_orders]

    logger.info(
        "Creditnote storno guard project=%s creditnoted_orders=%s eligible=%s dry_run=%s",
        project,
        summary.creditnoted_orders,
        summary.eligible_orders,
        dry_run,
    )

    if dry_run:
        return summary

    for order in eligible_orders:
        order_num = str(order.get("order_num") or "").strip()
        if not order_num:
            summary.failed_orders += 1
            summary.failed_order_nums.append("")
            continue
        try:
            change_order_status(exporter.client, order_num, target_status_id)
            summary.updated_orders += 1
            summary.updated_order_nums.append(order_num)
            logger.info("Changed creditnoted order %s to status_id=%s", order_num, target_status_id)
        except Exception as exc:  # pragma: no cover - API failure path
            summary.failed_orders += 1
            summary.failed_order_nums.append(order_num)
            logger.error("Failed to change creditnoted order %s to Storno: %s", order_num, exc)

    return summary
