"""Project-bound status identities; provider labels remain available for audit.

A reviewed shop rename changes presentation, not a status identity. This module
does not translate arbitrary labels and never resolves a duplicate visible name
without the reviewed ID. It performs no network calls or writes.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
import re
import unicodedata
from urllib.parse import urlparse


CONTRACT_VERSION = "2026-09-13-vevo-reviewed-renames-v1"
# Historical API labels and gateway event/ID bindings are separately proven.
# 31/34 historical evidence is transliterated, so accent normalization is allowed
# only inside this exact project/ID contract. ID1 has no historical proof here.
REVIEWED_RENAMES: dict[str, dict[str, tuple[str, tuple[str, ...]]]] = {"vevo": {
    "4": ("Odoslaná", ("Odoslaná", "Shipped")),
    "17": ("Storno", ("Storno", "Cancelled")),
    "31": ("Platba online - zaplatené", ("Platba online - zaplatene", "Payment online - paid")),
    "33": ("Platba online - platnosť vypršala", ("Platba online - platnosť vypršala", "Payment online - expired")),
    "34": ("Platba online - platba zamietnutá", ("Platba online - platba zamietnuta", "Payment online - expired")),
}}
PROOF_REFERENCES = {
    "current-native-status-catalogue-20260913.json": "879d05d452b7a29842fca578278b841b63f9340a4ffc8d8bcbac195fcaf0952b",
    "status-review-user-restored-readback-20260913.json": "c4621d2d3792a53320706f2bcfee8429b5d443e69a3eabd250ad14f7029c1aea",
    "native-creditnote-invoice-key-contract-20260913.json": "523a2c2d5c7677f4e0889cdffed116e863865e814090c79207c7b904171dc70e",
    "gopay-native-status-guard-inspection-20260913.json": "4c28ff3ace67ae583249ef85a5f01aeef9c5128608b79a6d8e67ea70d00499bc",
    "status-review-two-case-api-20260913.json": "f7b1b0ddd42a7fd038088e94e235fcc2e812affb6283a1362d95ad6b4f9dc5cc",
}
_ATTRIBUTE = "_order_status_identity"


def normalized(value: str) -> str:
    return " ".join("".join(c for c in unicodedata.normalize("NFKD", value)
                            if not unicodedata.combining(c)).casefold().split())


def observed_label(value: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", value) if not unicodedata.combining(c))


def positive_id(value) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int)) or not re.fullmatch(r"[1-9][0-9]*", str(value)):
        raise ValueError("status_identity_invalid_id")
    return str(value)


def valid_label(value) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError("status_identity_invalid_label")
    return value


def validate_catalogue(rows) -> list[dict]:
    if not isinstance(rows, list) or not rows:
        raise ValueError("status_identity_catalogue_missing")
    seen = set()
    result = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("status_identity_catalogue_invalid")
        key = positive_id(row.get("id"))
        valid_label(row.get("name"))
        if key in seen:
            raise ValueError("status_identity_duplicate_id")
        seen.add(key)
        result.append(deepcopy(row))
    return result


@dataclass
class StatusIdentity:
    client: object
    project: str
    transport: object
    url: str | None
    catalogue: dict[str, dict] = field(default_factory=dict)

    @property
    def renamed(self) -> bool:
        return bool(REVIEWED_RENAMES.get(self.project))

    def assert_bound(self, client) -> None:
        transport = getattr(client, "transport", None)
        if client is not self.client or transport is not self.transport or getattr(transport, "url", None) != self.url:
            raise ValueError("status_identity_client_or_project_changed")

    def canonical_label(self, key: str, name: str) -> str:
        entry = REVIEWED_RENAMES.get(self.project, {}).get(key)
        if entry is None:
            return name
        canonical, observed = entry
        if observed_label(name) not in {observed_label(value) for value in observed}:
            raise ValueError("status_identity_unapproved_label")
        return canonical

    def bind_catalogue(self, client, rows) -> list[dict]:
        self.assert_bound(client)
        self.catalogue = {}
        rows = validate_catalogue(rows)
        raw = {positive_id(row["id"]): row for row in rows}
        if not set(REVIEWED_RENAMES.get(self.project, {})).issubset(raw):
            raise ValueError("status_identity_reviewed_id_missing")
        result = []
        for row in rows:
            key = positive_id(row["id"])
            result.append({**row, "name": self.canonical_label(key, row["name"])})
        for canonical, _ in REVIEWED_RENAMES.get(self.project, {}).values():
            if sum(normalized(row["name"]) == normalized(canonical) for row in result) != 1:
                raise ValueError("status_identity_canonical_role_ambiguous")
        self.catalogue = raw
        return result

    def order(self, client, order, *, inventory=False) -> dict:
        self.assert_bound(client)
        if isinstance(order, dict) and (("raw_status" in order) != ("status_identity_contract" in order)):
            raise ValueError("status_identity_canonical_evidence_incomplete")
        if isinstance(order, dict) and "status_identity_contract" in order and order["status_identity_contract"] != {
                "project": self.project, "version": CONTRACT_VERSION}:
            raise ValueError("status_identity_canonical_project_changed")
        if not self.renamed:
            return order
        if not isinstance(order, dict) or not self.catalogue:
            raise ValueError("status_identity_catalogue_not_bound")
        status = order.get("status")
        if status is None:
            return order  # Preserve nullable inventory evidence; it cannot qualify.
        if not isinstance(status, dict):
            raise ValueError("status_identity_order_status_invalid")
        raw = order.get("raw_status", status)
        if not isinstance(raw, dict):
            raise ValueError("status_identity_raw_status_invalid")
        key = positive_id(raw.get("id"))
        name = valid_label(raw.get("name"))
        if key not in self.catalogue:
            if inventory and key not in REVIEWED_RENAMES.get(self.project, {}):
                result = deepcopy(order)
                result["status_identity_unbound"] = True
                return result
            raise ValueError("status_identity_order_status_not_in_catalogue")
        if order.get("status_identity_unbound"):
            raise ValueError("status_identity_unbound_evidence")
        canonical = self.canonical_label(key, name)
        if key not in REVIEWED_RENAMES.get(self.project, {}) and name != self.catalogue[key]["name"]:
            raise ValueError("status_identity_unreviewed_label_drift")
        if "raw_status" in order and (positive_id(status.get("id")) != key or status.get("name") != canonical
                                     or order.get("status_identity_contract") != {"project": self.project, "version": CONTRACT_VERSION}):
            raise ValueError("status_identity_canonical_evidence_changed")
        result = deepcopy(order)
        result["raw_status"] = deepcopy(raw)
        result["status"] = {**status, "name": canonical}
        result["status_identity_contract"] = {"project": self.project, "version": CONTRACT_VERSION}
        return result


def bind_status_identity(client, project: str, settings: dict | None = None) -> StatusIdentity:
    if project not in {"roy", "vevo"}:
        raise ValueError("status_identity_project_unsupported")
    existing = identity_for(client)
    if existing and existing.project != project:
        raise ValueError("status_identity_project_changed")
    transport = getattr(client, "transport", None)
    url = getattr(transport, "url", None)
    if url is not None:
        parsed = urlparse(url)
        if (parsed.scheme != "https" or parsed.hostname not in {f"{project}.flox.sk", f"{project}.sk", f"www.{project}.sk"}
                or parsed.path != "/api/graphql" or parsed.username or parsed.password or parsed.query or parsed.fragment
                or parsed.port not in {None, 443}):
            raise ValueError("status_identity_project_endpoint_changed")
        configured = (settings or {}).get("biznisweb_api_url")
        if configured is not None and url != configured:
            raise ValueError("status_identity_configured_endpoint_changed")
    if existing:
        return existing
    policy = StatusIdentity(client, project, transport, url)
    setattr(client, _ATTRIBUTE, policy)
    return policy


def identity_for(client) -> StatusIdentity | None:
    policy = getattr(client, _ATTRIBUTE, None)
    if policy is not None:
        if not isinstance(policy, StatusIdentity):
            raise ValueError("status_identity_policy_invalid")
        policy.assert_bound(client)
    return policy


def bind_catalogue(client, rows) -> list[dict]:
    policy = identity_for(client)
    return policy.bind_catalogue(client, rows) if policy else validate_catalogue(rows)


def canonical_order(client, order, *, inventory=False) -> dict:
    policy = identity_for(client)
    return policy.order(client, order, inventory=inventory) if policy else order


def invalidate_catalogue(client) -> None:
    policy = identity_for(client)
    if policy is not None:
        policy.catalogue = {}


def unique_target(rows, name: str, configured_id=None) -> int:
    rows = validate_catalogue(rows)
    matches = [row for row in rows if normalized(row["name"]) == normalized(valid_label(name))]
    if len(matches) != 1 or (configured_id is not None and positive_id(matches[0]["id"]) != positive_id(configured_id)):
        raise ValueError("status_identity_target_missing_or_ambiguous")
    return int(matches[0]["id"])


def status_audit_fields(client, order: dict, target_id=None) -> dict:
    policy = identity_for(client)
    if not policy or not policy.renamed:
        return {}
    current = canonical_order(client, order)
    result = {"source_raw_status": deepcopy(current["raw_status"]),
              "status_identity_contract": deepcopy(current["status_identity_contract"])}
    if target_id is not None:
        key = positive_id(target_id)
        if key not in policy.catalogue:
            raise ValueError("status_identity_target_missing")
        result["target_raw_status"] = deepcopy(policy.catalogue[key])
    return result
