"""Durable, project-scoped automation lease and invoice outbox in S3.

All writes use compare-and-swap. Losing a lease, an uncertain S3 write, or an
unreadable journal stops mutations; none of those conditions means empty state.
The bucket is private runtime state and must never be copied into the Git repo.
"""

from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
import re
from typing import Any, Callable, Iterator
from uuid import uuid4


class AutomationStateError(RuntimeError):
    """The durable safety state cannot be trusted or updated."""


class AutomationLeaseBusy(AutomationStateError):
    """Another run currently owns this project's mutation lease."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise AutomationStateError("Automation state timestamp lacks a timezone")
    return parsed.astimezone(timezone.utc)


def resolve_automation_state_location(
    project: str, settings: dict[str, Any], environ: Any = None,
) -> tuple[str, str]:
    """Resolve one private, project-specific key for all order automation writers."""
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", project):
        raise AutomationStateError("Invalid project identifier for automation state")
    env = os.environ if environ is None else environ
    invoice = settings.get("invoice_generation") or {}
    bucket = str(
        invoice.get("state_bucket")
        or env.get(f"ORDER_AUTOMATION_STATE_BUCKET_{project.upper()}")
        or env.get("ORDER_AUTOMATION_STATE_BUCKET")
        or env.get(f"REPORT_S3_BUCKET_{project.upper()}")
        or env.get("REPORT_S3_BUCKET")
        or ""
    ).strip()
    prefix = str(
        invoice.get("state_prefix")
        or env.get(f"ORDER_AUTOMATION_STATE_PREFIX_{project.upper()}")
        or env.get("ORDER_AUTOMATION_STATE_PREFIX")
        or f"data/{project}/order-automation"
    ).strip().strip("/")
    prefix = prefix.replace("{project}", project)
    if not bucket or "/" in bucket:
        raise AutomationStateError("A private automation state S3 bucket is required")
    if project not in prefix.split("/") or any(part in {"", ".", ".."} for part in prefix.split("/")):
        raise AutomationStateError("Automation state prefix must contain an exact project segment")
    return bucket, f"{prefix}/state.json"


def _error_code(exc: Exception) -> str:
    return str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))


class S3AutomationStateStore:
    """A single CAS document keeps the lease and its invoice journal atomic."""

    def __init__(
        self, client: Any, bucket: str, key: str, project: str, *,
        now: Callable[[], datetime] = utc_now, lease_seconds: int = 300,
        max_lifetime_seconds: int = 3600,
    ) -> None:
        self.client = client
        self.bucket = bucket
        self.key = key
        self.project = project
        self.now = now
        self.lease_seconds = max(120, lease_seconds)
        self.max_lifetime_seconds = max(self.lease_seconds, max_lifetime_seconds)

    def empty_state(self) -> dict[str, Any]:
        return {"schema_version": 1, "project": self.project, "lease": None, "orders": {}, "last_scan": {}}

    def read(self) -> tuple[dict[str, Any], str]:
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self.key)
        except Exception as exc:
            if _error_code(exc) in {"NoSuchKey", "404"}:
                return self.empty_state(), ""
            raise AutomationStateError("Cannot read the automation safety journal") from exc
        try:
            body = response["Body"].read()
            state = json.loads(body)
            etag = str(response["ETag"])
        except (KeyError, TypeError, ValueError) as exc:
            raise AutomationStateError("Malformed automation safety journal") from exc
        if (
            not isinstance(state, dict)
            or state.get("schema_version") != 1
            or state.get("project") != self.project
            or not isinstance(state.get("orders"), dict)
            or not isinstance(state.get("last_scan"), dict)
            or not etag
        ):
            raise AutomationStateError("Automation journal identity or schema mismatch")
        return state, etag

    def put(self, state: dict[str, Any], etag: str) -> None:
        condition = {"IfMatch": etag} if etag else {"IfNoneMatch": "*"}
        try:
            self.client.put_object(
                Bucket=self.bucket, Key=self.key,
                Body=(json.dumps(state, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8"),
                ContentType="application/json", ServerSideEncryption="AES256", **condition,
            )
        except Exception as exc:
            raise AutomationStateError("Automation journal conditional write failed; stopping mutations") from exc

    @contextmanager
    def lease(self, owner: str) -> Iterator["AutomationJournal"]:
        state, etag = self.read()
        now = self.now()
        existing = state.get("lease")
        if existing:
            try:
                active = parse_utc(existing["expires_at"]) > now
            except (KeyError, TypeError, ValueError) as exc:
                raise AutomationStateError("Invalid existing automation lease") from exc
            if active:
                raise AutomationLeaseBusy("Another run holds this project's automation lease")
        token = uuid4().hex
        state["lease"] = {
            "token": token, "owner": str(owner), "acquired_at": iso_utc(now),
            "expires_at": iso_utc(now + timedelta(seconds=self.lease_seconds)),
            "max_expires_at": iso_utc(now + timedelta(seconds=self.max_lifetime_seconds)),
        }
        self.put(state, etag)
        journal = AutomationJournal(self, token)
        journal.assert_owned()
        try:
            yield journal
        finally:
            journal.release()


class AutomationJournal:
    def __init__(self, store: S3AutomationStateStore, token: str) -> None:
        self.store = store
        self.token = token

    def _owned(self) -> tuple[dict[str, Any], str]:
        state, etag = self.store.read()
        lease = state.get("lease") or {}
        if lease.get("token") != self.token:
            raise AutomationStateError("Automation lease ownership was lost")
        try:
            valid = parse_utc(lease["expires_at"]) > self.store.now()
        except (KeyError, TypeError, ValueError) as exc:
            raise AutomationStateError("Invalid owned automation lease") from exc
        if not valid:
            raise AutomationStateError("Automation lease expired; this run cannot resume mutations")
        return state, etag

    def assert_owned(self) -> None:
        """Renew before a mutation; API request deadlines must be below the lease TTL."""
        state, etag = self._owned()
        now = self.store.now()
        maximum = parse_utc(state["lease"]["max_expires_at"])
        if maximum <= now + timedelta(seconds=60):
            raise AutomationStateError("Automation run exceeded its bounded lease lifetime")
        state["lease"]["expires_at"] = iso_utc(min(maximum, now + timedelta(seconds=self.store.lease_seconds)))
        self.store.put(state, etag)

    def snapshot(self) -> dict[str, Any]:
        return deepcopy(self._owned()[0])

    def get_order(self, order_num: str) -> dict[str, Any]:
        return deepcopy(self._owned()[0]["orders"].get(str(order_num), {}))

    def update_order(self, order_num: str, **fields: Any) -> None:
        if not str(order_num).strip():
            raise AutomationStateError("Invoice journal order identity is required")
        state, etag = self._owned()
        record = state["orders"].setdefault(str(order_num), {"order_num": str(order_num)})
        if "order_num" in fields and str(fields["order_num"]) != str(order_num):
            raise AutomationStateError("Invoice journal order identity cannot change")
        record.update(fields)
        record["updated_at"] = iso_utc(self.store.now())
        self.store.put(state, etag)

    def pending_orders(self) -> list[dict[str, Any]]:
        state, _ = self._owned()
        return [deepcopy(record) for record in state["orders"].values() if record.get("phase") != "complete"]

    def enqueue_orders(self, orders: list[dict[str, Any]]) -> None:
        state, etag = self._owned()
        for order in orders:
            number = str(order["order_num"])
            existing = state["orders"].get(number) or {}
            if existing.get("phase") not in {"creating", "create_ambiguous", "email"}:
                state["orders"][number] = {**existing, "order_num": number, "phase": "pending"}
        self.store.put(state, etag)

    def remember_shipped_orders(self, orders: list[dict[str, Any]], shipped_ids: set[int]) -> None:
        state, etag = self._owned()
        expected = {str(value) for value in shipped_ids}
        changed = False
        for order in orders:
            if order.get("blocked") is not False or str((order.get("status") or {}).get("id")) not in expected:
                continue
            number = str(order["order_num"])
            record = state["orders"].setdefault(number, {"order_num": number, "phase": "complete"})
            record.update(verified_fulfillment_status="Odoslaná", status_observed_at=iso_utc(self.store.now()),
                          status_observation_source="authenticated_order_api")
            changed = True
        if changed:
            self.store.put(state, etag)

    def enqueue_status_reviews(self, orders: list[dict[str, Any]]) -> None:
        """Persist status work before advancing the incremental scan watermark."""
        if not orders:
            return
        state, etag = self._owned()
        for order in orders:
            number = str(order["order_num"])
            record = state["orders"].setdefault(number, {"order_num": number, "phase": "complete"})
            if (record.get("status_review") or {}).get("state") != "open":
                record["status_review"] = {"state": "open", "reason": "pending_evidence_check"}
        self.store.put(state, etag)

    def record_scan(self, **fields: Any) -> None:
        state, etag = self._owned()
        state["last_scan"].update(fields)
        self.store.put(state, etag)

    def release(self) -> None:
        # An expired/old owner must never clear a replacement owner's lease.
        state, etag = self.store.read()
        if (state.get("lease") or {}).get("token") != self.token:
            return
        state["lease"] = None
        self.store.put(state, etag)


def build_automation_state_store(project: str, settings: dict[str, Any]) -> S3AutomationStateStore:
    import boto3
    from botocore.config import Config

    bucket, key = resolve_automation_state_location(project, settings)
    client = boto3.client(
        "s3", region_name=os.getenv("AWS_REGION", "eu-central-1"),
        config=Config(connect_timeout=10, read_timeout=30, retries={"total_max_attempts": 1}),
    )
    return S3AutomationStateStore(client, bucket, key, project)
