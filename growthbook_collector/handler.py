from __future__ import annotations

import base64
import ipaddress
import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple


SCHEMA_VERSION = 1
DEFAULT_COLLECTOR_VERSION = "vevo-growthbook-collector-v1"
DEFAULT_MAX_BODY_BYTES = 4096
DEFAULT_ORIGIN = "https://www.vevo.sk"
DEFAULT_PREFIX = "experiment-events/raw"
DEFAULT_REGISTRY_PATH = Path(__file__).with_name("experiments.json")
RECEIPT_MARKER = "VEVO_GROWTHBOOK_COLLECTOR_RECEIPT"

EVENT_FIELDS = {
    "experiment_exposure": frozenset(),
    "add_to_cart": frozenset({"product_id"}),
    "order_completed": frozenset({"transaction_id"}),
    "performance_vital": frozenset({"page_load_id", "vital_name", "vital_value"}),
    "client_error_observed": frozenset({"page_load_id", "error_kind"}),
}

COMMON_FIELDS = frozenset(
    {
        "schema_version",
        "event_id",
        "event_name",
        "occurred_at",
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
    }
)

PAGE_TYPES = frozenset({"home", "product", "category", "checkout_success"})
CONSENT_STATES = frozenset({"analytics_granted"})
UTM_SOURCES = frozenset({"meta", "facebook", "instagram"})
UTM_MEDIUMS = frozenset({"paid_social", "social", "cpc"})
META_PLACEMENTS = frozenset(
    {
        "audience_network",
        "facebook_feed",
        "facebook_marketplace",
        "facebook_reels",
        "facebook_stories",
        "facebook_video_feeds",
        "instagram_explore",
        "instagram_feed",
        "instagram_profile_feed",
        "instagram_reels",
        "instagram_stories",
        "messenger_inbox",
        "threads_feed",
    }
)
VITAL_LIMITS = {
    "lcp_ms": 60_000,
    "inp_ms": 60_000,
    "cls_milli": 100_000,
}
ERROR_KINDS = frozenset({"runtime_error", "unhandled_rejection"})

SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,79}$")
VARIATION_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
PATH_RE = re.compile(r"^/[A-Za-z0-9/_~!$&'()*+,;=:@.%+-]{0,199}$")
SHORT_TOKEN_RE = re.compile(r"^[a-z0-9_-]{1,50}$")
META_ID_RE = re.compile(r"^[0-9]{1,30}$")
PRODUCT_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,64}$")
TRANSACTION_ID_RE = re.compile(r"^[0-9]{1,20}$")
EMAIL_RE = re.compile(r"(?i)(?:^|[^A-Z0-9._%+-])[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}(?:$|[^A-Z0-9.-])")
IPV4_CANDIDATE_RE = re.compile(r"(?<![0-9])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?![0-9])")
PHONE_RE = re.compile(r"(?<![A-Za-z0-9])\+[0-9][0-9 ().-]{6,}[0-9](?![A-Za-z0-9])")
FORBIDDEN_VALUE_TOKENS = (
    "http://",
    "https://",
    "fbclid",
    "_fbp",
    "_fbc",
    "mailto:",
    "tel:",
    "%40",
)


class RejectedEvent(ValueError):
    def __init__(self, code: str, status_code: int = 400) -> None:
        super().__init__(code)
        self.code = code
        self.status_code = status_code


@dataclass(frozen=True)
class CollectorConfig:
    bucket: str
    prefix: str
    region: str
    environment: str
    allowed_origins: frozenset[str]
    collector_version: str
    registry_path: Path
    max_body_bytes: int = DEFAULT_MAX_BODY_BYTES
    kms_key_arn: Optional[str] = None

    @classmethod
    def from_environment(cls) -> "CollectorConfig":
        bucket = os.environ.get("GROWTHBOOK_EVENT_BUCKET", "").strip()
        if not bucket:
            raise RuntimeError("GROWTHBOOK_EVENT_BUCKET is required")

        prefix = os.environ.get("GROWTHBOOK_EVENT_PREFIX", DEFAULT_PREFIX).strip(" /")
        if not prefix or ".." in prefix:
            raise RuntimeError("GROWTHBOOK_EVENT_PREFIX is invalid")

        environment = os.environ.get("GROWTHBOOK_ENVIRONMENT", "production").strip().lower()
        if environment not in {"preview", "production"}:
            raise RuntimeError("GROWTHBOOK_ENVIRONMENT must be preview or production")

        origins = frozenset(
            value.strip()
            for value in os.environ.get("GROWTHBOOK_ALLOWED_ORIGINS", DEFAULT_ORIGIN).split(",")
            if value.strip()
        )
        if not origins or any(not value.startswith("https://") for value in origins):
            raise RuntimeError("GROWTHBOOK_ALLOWED_ORIGINS must contain HTTPS origins")

        try:
            max_body_bytes = int(
                os.environ.get("GROWTHBOOK_MAX_BODY_BYTES", str(DEFAULT_MAX_BODY_BYTES))
            )
        except ValueError as exc:
            raise RuntimeError("GROWTHBOOK_MAX_BODY_BYTES must be an integer") from exc
        if not 512 <= max_body_bytes <= 16_384:
            raise RuntimeError("GROWTHBOOK_MAX_BODY_BYTES is outside the safe range")

        return cls(
            bucket=bucket,
            prefix=prefix,
            region=os.environ.get("AWS_REGION", "eu-central-1").strip() or "eu-central-1",
            environment=environment,
            allowed_origins=origins,
            collector_version=(
                os.environ.get("GROWTHBOOK_COLLECTOR_VERSION", DEFAULT_COLLECTOR_VERSION).strip()
                or DEFAULT_COLLECTOR_VERSION
            ),
            registry_path=Path(
                os.environ.get("GROWTHBOOK_EXPERIMENT_REGISTRY_PATH", str(DEFAULT_REGISTRY_PATH))
            ),
            max_body_bytes=max_body_bytes,
            kms_key_arn=os.environ.get("GROWTHBOOK_EVENT_KMS_KEY_ARN", "").strip() or None,
        )


def _reject_json_constant(value: str) -> None:
    raise RejectedEvent(f"invalid_json_constant:{value}")


def _object_without_duplicates(pairs: Iterable[Tuple[str, Any]]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise RejectedEvent("duplicate_field")
        result[key] = value
    return result


def _parse_json(raw: bytes) -> Dict[str, Any]:
    try:
        parsed = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_object_without_duplicates,
            parse_constant=_reject_json_constant,
        )
    except RejectedEvent:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RejectedEvent("invalid_json") from exc
    if not isinstance(parsed, dict):
        raise RejectedEvent("body_must_be_object")
    return parsed


def _canonical_uuid4(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise RejectedEvent(f"invalid_{field}")
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError) as exc:
        raise RejectedEvent(f"invalid_{field}") from exc
    if parsed.version != 4 or str(parsed) != value:
        raise RejectedEvent(f"invalid_{field}")
    return value


def _parse_occurred_at(value: Any, now: datetime) -> Tuple[str, datetime]:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise RejectedEvent("invalid_occurred_at")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise RejectedEvent("invalid_occurred_at") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise RejectedEvent("invalid_occurred_at")
    parsed = parsed.astimezone(timezone.utc)
    if parsed > now + timedelta(minutes=5) or parsed < now - timedelta(hours=24):
        raise RejectedEvent("occurred_at_out_of_range")
    canonical = parsed.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return canonical, parsed


def _contains_ipv4(value: str) -> bool:
    for candidate in IPV4_CANDIDATE_RE.findall(value):
        try:
            ipaddress.ip_address(candidate)
            return True
        except ValueError:
            continue
    return False


def _reject_pii_values(payload: Mapping[str, Any]) -> None:
    for value in payload.values():
        if isinstance(value, (dict, list, tuple, set)):
            raise RejectedEvent("nested_value_not_allowed")
        if not isinstance(value, str):
            continue
        lowered = value.lower()
        if any(token in lowered for token in FORBIDDEN_VALUE_TOKENS):
            raise RejectedEvent("forbidden_value")
        if EMAIL_RE.search(value) or PHONE_RE.search(value) or _contains_ipv4(value):
            raise RejectedEvent("pii_value_not_allowed")


def _nullable_token(value: Any, allowed: frozenset[str], field: str) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str) or not SHORT_TOKEN_RE.fullmatch(value) or value not in allowed:
        raise RejectedEvent(f"invalid_{field}")
    return value


def _nullable_meta_id(value: Any, field: str) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str) or not META_ID_RE.fullmatch(value):
        raise RejectedEvent(f"invalid_{field}")
    return value


@lru_cache(maxsize=8)
def load_registry(path: str, environment: str) -> Dict[str, Dict[str, frozenset[str]]]:
    registry_path = Path(path)
    try:
        raw = json.loads(registry_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError("Experiment registry is unavailable or invalid") from exc

    if raw.get("schema_version") != SCHEMA_VERSION:
        raise RuntimeError("Experiment registry schema version mismatch")
    environments = raw.get("environments")
    if not isinstance(environments, dict) or environment not in environments:
        raise RuntimeError("Experiment registry environment is missing")
    environment_registry = environments[environment]
    if not isinstance(environment_registry, dict):
        raise RuntimeError("Experiment registry environment must be an object")

    normalized: Dict[str, Dict[str, frozenset[str]]] = {}
    for experiment_id, definition in environment_registry.items():
        if not isinstance(experiment_id, str) or not SLUG_RE.fullmatch(experiment_id):
            raise RuntimeError("Experiment registry contains an invalid experiment id")
        if not isinstance(definition, dict):
            raise RuntimeError("Experiment registry definition must be an object")
        variations = definition.get("variations")
        page_types = definition.get("exposure_page_types")
        health_page_types = definition.get("health_page_types")
        allowed_events = definition.get("allowed_events")
        if not isinstance(variations, list) or not variations:
            raise RuntimeError("Experiment registry variations are invalid")
        if not isinstance(page_types, list) or not page_types:
            raise RuntimeError("Experiment registry page types are invalid")
        if not isinstance(health_page_types, list) or not health_page_types:
            raise RuntimeError("Experiment registry health page types are invalid")
        if not isinstance(allowed_events, list) or not allowed_events:
            raise RuntimeError("Experiment registry events are invalid")
        if any(not isinstance(value, str) or not VARIATION_RE.fullmatch(value) for value in variations):
            raise RuntimeError("Experiment registry contains an invalid variation")
        if len(set(variations)) != len(variations):
            raise RuntimeError("Experiment registry variations must be unique")
        if any(value not in PAGE_TYPES - {"checkout_success"} for value in page_types):
            raise RuntimeError("Experiment registry contains an invalid exposure page type")
        if any(value not in PAGE_TYPES for value in health_page_types):
            raise RuntimeError("Experiment registry contains an invalid health page type")
        if not set(page_types).issubset(health_page_types):
            raise RuntimeError("Experiment health pages must include all exposure pages")
        if any(value not in EVENT_FIELDS for value in allowed_events):
            raise RuntimeError("Experiment registry contains an invalid event")
        normalized[experiment_id] = {
            "variations": frozenset(variations),
            "exposure_page_types": frozenset(page_types),
            "health_page_types": frozenset(health_page_types),
            "allowed_events": frozenset(allowed_events),
        }
    return normalized


def validate_event(
    payload: Dict[str, Any],
    *,
    now: datetime,
    registry: Mapping[str, Mapping[str, frozenset[str]]],
    collector_version: str,
) -> Dict[str, Any]:
    _reject_pii_values(payload)

    event_name = payload.get("event_name")
    if event_name not in EVENT_FIELDS:
        raise RejectedEvent("invalid_event_name")
    expected_fields = COMMON_FIELDS | EVENT_FIELDS[event_name]
    if set(payload) != expected_fields:
        raise RejectedEvent("field_set_mismatch")

    if type(payload.get("schema_version")) is not int or payload["schema_version"] != SCHEMA_VERSION:
        raise RejectedEvent("invalid_schema_version")

    event_id = _canonical_uuid4(payload.get("event_id"), "event_id")
    device_id = _canonical_uuid4(payload.get("device_id"), "device_id")
    occurred_at, _ = _parse_occurred_at(payload.get("occurred_at"), now)

    page_path = payload.get("page_path")
    if (
        not isinstance(page_path, str)
        or not PATH_RE.fullmatch(page_path)
        or "//" in page_path
        or "/../" in f"{page_path}/"
    ):
        raise RejectedEvent("invalid_page_path")

    page_type = payload.get("page_type")
    if page_type not in PAGE_TYPES:
        raise RejectedEvent("invalid_page_type")
    if payload.get("consent_state") not in CONSENT_STATES:
        raise RejectedEvent("ineligible_consent", status_code=403)

    experiment_id = payload.get("experiment_id")
    if not isinstance(experiment_id, str) or not SLUG_RE.fullmatch(experiment_id):
        raise RejectedEvent("invalid_experiment_id")
    experiment = registry.get(experiment_id)
    if experiment is None:
        raise RejectedEvent("inactive_experiment")
    if event_name not in experiment["allowed_events"]:
        raise RejectedEvent("event_not_allowed_for_experiment")

    variation_id = payload.get("variation_id")
    if not isinstance(variation_id, str) or variation_id not in experiment["variations"]:
        raise RejectedEvent("invalid_variation_id")

    if event_name == "experiment_exposure" and page_type not in experiment["exposure_page_types"]:
        raise RejectedEvent("ineligible_exposure_page")
    if event_name == "add_to_cart" and page_type not in experiment["exposure_page_types"]:
        raise RejectedEvent("event_page_not_exposed")
    if event_name in {"performance_vital", "client_error_observed"}:
        if page_type not in experiment["health_page_types"]:
            raise RejectedEvent("health_page_not_allowed")
    if event_name == "add_to_cart" and page_type != "product":
        raise RejectedEvent("add_to_cart_requires_product_page")
    if event_name == "order_completed" and page_type != "checkout_success":
        raise RejectedEvent("order_completed_requires_checkout_success")

    normalized = {
        "schema_version": SCHEMA_VERSION,
        "event_id": event_id,
        "event_name": event_name,
        "occurred_at": occurred_at,
        "device_id": device_id,
        "page_path": page_path,
        "page_type": page_type,
        "consent_state": "analytics_granted",
        "experiment_id": experiment_id,
        "variation_id": variation_id,
        "utm_source": _nullable_token(payload.get("utm_source"), UTM_SOURCES, "utm_source"),
        "utm_medium": _nullable_token(payload.get("utm_medium"), UTM_MEDIUMS, "utm_medium"),
        "meta_campaign_id": _nullable_meta_id(payload.get("meta_campaign_id"), "meta_campaign_id"),
        "meta_adset_id": _nullable_meta_id(payload.get("meta_adset_id"), "meta_adset_id"),
        "meta_ad_id": _nullable_meta_id(payload.get("meta_ad_id"), "meta_ad_id"),
        "meta_placement": _nullable_token(
            payload.get("meta_placement"), META_PLACEMENTS, "meta_placement"
        ),
    }

    if event_name == "add_to_cart":
        product_id = payload.get("product_id")
        if not isinstance(product_id, str) or not PRODUCT_ID_RE.fullmatch(product_id):
            raise RejectedEvent("invalid_product_id")
        normalized["product_id"] = product_id
    elif event_name == "order_completed":
        transaction_id = payload.get("transaction_id")
        if not isinstance(transaction_id, str) or not TRANSACTION_ID_RE.fullmatch(transaction_id):
            raise RejectedEvent("invalid_transaction_id")
        normalized["transaction_id"] = transaction_id
    elif event_name == "performance_vital":
        normalized["page_load_id"] = _canonical_uuid4(payload.get("page_load_id"), "page_load_id")
        vital_name = payload.get("vital_name")
        vital_value = payload.get("vital_value")
        if vital_name not in VITAL_LIMITS:
            raise RejectedEvent("invalid_vital_name")
        if type(vital_value) is not int or not 0 <= vital_value <= VITAL_LIMITS[vital_name]:
            raise RejectedEvent("invalid_vital_value")
        normalized["vital_name"] = vital_name
        normalized["vital_value"] = vital_value
    elif event_name == "client_error_observed":
        normalized["page_load_id"] = _canonical_uuid4(payload.get("page_load_id"), "page_load_id")
        error_kind = payload.get("error_kind")
        if error_kind not in ERROR_KINDS:
            raise RejectedEvent("invalid_error_kind")
        normalized["error_kind"] = error_kind

    normalized.update(
        {
            "received_at": now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "event_date": now.date().isoformat(),
            "collector_version": collector_version,
            "risk_result": "accepted",
        }
    )
    return normalized


def _emit_receipt_marker(duplicate: bool) -> None:
    """Emit one aggregate PII-free receipt used for A/A duplicate-rate proof."""

    marker = {
        "accepted": True,
        "duplicate": duplicate,
        "marker": RECEIPT_MARKER,
        "schema_version": 1,
    }
    try:
        print(json.dumps(marker, separators=(",", ":"), sort_keys=True), flush=True)
    except Exception:
        # Receipt evidence must never turn a persisted event into a failed cart
        # or checkout-side request. Missing markers fail the later parity gate.
        pass


def _headers(event: Mapping[str, Any]) -> Dict[str, str]:
    raw_headers = event.get("headers") or {}
    if not isinstance(raw_headers, dict):
        return {}
    return {str(key).lower(): str(value) for key, value in raw_headers.items() if value is not None}


def _method(event: Mapping[str, Any]) -> str:
    request_context = event.get("requestContext") or {}
    if isinstance(request_context, dict):
        http = request_context.get("http") or {}
        if isinstance(http, dict) and http.get("method"):
            return str(http["method"]).upper()
    return str(event.get("httpMethod") or "").upper()


def _response(status_code: int, body: Dict[str, Any], origin: Optional[str]) -> Dict[str, Any]:
    headers = {
        "Cache-Control": "no-store",
        "Content-Security-Policy": "default-src 'none'",
        "Content-Type": "application/json; charset=utf-8",
        "Referrer-Policy": "no-referrer",
        "Vary": "Origin",
        "X-Content-Type-Options": "nosniff",
    }
    if origin:
        headers.update(
            {
                "Access-Control-Allow-Headers": "content-type",
                "Access-Control-Allow-Methods": "POST, OPTIONS",
                "Access-Control-Allow-Origin": origin,
                "Access-Control-Max-Age": "600",
            }
        )
    return {
        "statusCode": status_code,
        "headers": headers,
        "body": json.dumps(body, separators=(",", ":"), sort_keys=True),
        "isBase64Encoded": False,
    }


def _raw_body(event: Mapping[str, Any], max_body_bytes: int) -> bytes:
    body = event.get("body")
    if not isinstance(body, str):
        raise RejectedEvent("missing_body")
    try:
        raw = base64.b64decode(body, validate=True) if event.get("isBase64Encoded") else body.encode("utf-8")
    except (ValueError, UnicodeEncodeError) as exc:
        raise RejectedEvent("invalid_body_encoding") from exc
    if not raw or len(raw) > max_body_bytes:
        raise RejectedEvent("invalid_body_size", status_code=413)
    return raw


def _s3_error_code(exc: Exception) -> str:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        error = response.get("Error")
        if isinstance(error, dict):
            return str(error.get("Code") or "")
    return ""


def persist_event(s3: Any, config: CollectorConfig, record: Dict[str, Any]) -> bool:
    key = f"{config.prefix}/event_date={record['event_date']}/{record['event_id']}.json"
    put_args: Dict[str, Any] = {
        "Bucket": config.bucket,
        "Key": key,
        "Body": (
            json.dumps(record, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
        ).encode("utf-8"),
        "CacheControl": "no-store",
        "ContentType": "application/json; charset=utf-8",
        "IfNoneMatch": "*",
        "ServerSideEncryption": "aws:kms" if config.kms_key_arn else "AES256",
    }
    if config.kms_key_arn:
        put_args["SSEKMSKeyId"] = config.kms_key_arn
    # A 409 means a competing operation prevented S3 from deciding the
    # conditional write. It is not evidence that this event already exists.
    # Retry twice with the same immutable key; only a 412 proves a duplicate.
    for attempt in range(3):
        try:
            s3.put_object(**put_args)
            return False
        except Exception as exc:
            error_code = _s3_error_code(exc)
            if error_code in {"PreconditionFailed", "412"}:
                return True
            if error_code in {"ConditionalRequestConflict", "409"} and attempt < 2:
                continue
            raise

    raise RuntimeError("unreachable S3 conditional-write state")


def handle_request(
    event: Mapping[str, Any],
    *,
    config: CollectorConfig,
    registry: Mapping[str, Mapping[str, frozenset[str]]],
    s3: Any,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    headers = _headers(event)
    origin = headers.get("origin")
    allowed_origin = origin if origin in config.allowed_origins else None

    if origin not in config.allowed_origins:
        return _response(403, {"accepted": False, "code": "origin_not_allowed"}, None)

    method = _method(event)
    if method == "OPTIONS":
        return _response(204, {}, allowed_origin)
    if method != "POST":
        return _response(405, {"accepted": False, "code": "method_not_allowed"}, allowed_origin)

    content_type = headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if content_type != "application/json":
        return _response(415, {"accepted": False, "code": "content_type_required"}, allowed_origin)

    try:
        raw = _raw_body(event, config.max_body_bytes)
        payload = _parse_json(raw)
        record = validate_event(
            payload,
            now=now,
            registry=registry,
            collector_version=config.collector_version,
        )
        duplicate = persist_event(s3, config, record)
    except RejectedEvent as exc:
        return _response(
            exc.status_code,
            {"accepted": False, "code": "invalid_event"},
            allowed_origin,
        )
    except Exception:
        return _response(503, {"accepted": False, "code": "storage_unavailable"}, allowed_origin)

    _emit_receipt_marker(duplicate)
    return _response(202, {"accepted": True, "duplicate": duplicate}, allowed_origin)


def lambda_handler(event: Mapping[str, Any], _context: Any) -> Dict[str, Any]:
    try:
        config = CollectorConfig.from_environment()
        registry = load_registry(str(config.registry_path), config.environment)
        import boto3  # type: ignore

        s3 = boto3.client("s3", region_name=config.region)
    except Exception:
        origin = _headers(event).get("origin")
        safe_origin = origin if origin == DEFAULT_ORIGIN else None
        return _response(503, {"accepted": False, "code": "collector_unavailable"}, safe_origin)
    return handle_request(event, config=config, registry=registry, s3=s3)
