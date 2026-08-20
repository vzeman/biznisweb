import base64
import json
import tempfile
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from growthbook_collector.handler import (
    CollectorConfig,
    handle_request,
    load_registry,
)


NOW = datetime(2026, 8, 20, 15, 0, 0, tzinfo=timezone.utc)
ORIGIN = "https://www.vevo.sk"


class FakeS3Error(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.put_calls = []
        self.fail_code = None
        self.fail_codes = []

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)
        if self.fail_codes:
            raise FakeS3Error(self.fail_codes.pop(0))
        if self.fail_code:
            raise FakeS3Error(self.fail_code)
        key = (kwargs["Bucket"], kwargs["Key"])
        if kwargs.get("IfNoneMatch") == "*" and key in self.objects:
            raise FakeS3Error("PreconditionFailed")
        self.objects[key] = kwargs["Body"]
        return {"ETag": '"test"'}


def registry_file(production=None):
    payload = {
        "schema_version": 1,
        "environments": {
            "preview": {
                "vevo-sk-aa-001": {
                    "variations": ["control", "variant"],
                    "exposure_page_types": ["home", "product", "category"],
                    "health_page_types": ["home", "product", "category", "checkout_success"],
                    "allowed_events": [
                        "experiment_exposure",
                        "add_to_cart",
                        "order_completed",
                        "performance_vital",
                        "client_error_observed",
                    ],
                }
            },
            "production": production or {},
        },
    }
    temp = tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json", delete=False)
    json.dump(payload, temp)
    temp.close()
    return Path(temp.name)


def base_payload(event_name="experiment_exposure", page_type="product"):
    return {
        "schema_version": 1,
        "event_id": str(uuid.uuid4()),
        "event_name": event_name,
        "occurred_at": "2026-08-20T14:59:30.000Z",
        "device_id": str(uuid.uuid4()),
        "page_path": "/p-1531/example",
        "page_type": page_type,
        "consent_state": "analytics_granted",
        "experiment_id": "vevo-sk-aa-001",
        "variation_id": "control",
        "utm_source": "meta",
        "utm_medium": "paid_social",
        "meta_campaign_id": "123456789",
        "meta_adset_id": "234567890",
        "meta_ad_id": "345678901",
        "meta_placement": "instagram_feed",
    }


class GrowthBookCollectorTests(unittest.TestCase):
    def setUp(self):
        load_registry.cache_clear()
        self.registry_path = registry_file()
        self.addCleanup(self.registry_path.unlink, missing_ok=True)
        self.config = CollectorConfig(
            bucket="vevo-experiment-test",
            prefix="experiment-events/raw",
            region="eu-central-1",
            environment="preview",
            allowed_origins=frozenset({ORIGIN}),
            collector_version="test-version",
            registry_path=self.registry_path,
        )
        self.registry = load_registry(str(self.registry_path), "preview")
        self.s3 = FakeS3()

    def request(self, payload, **overrides):
        event = {
            "requestContext": {"http": {"method": "POST"}},
            "headers": {"origin": ORIGIN, "content-type": "application/json; charset=utf-8"},
            "body": json.dumps(payload),
            "isBase64Encoded": False,
        }
        event.update(overrides)
        return handle_request(
            event,
            config=self.config,
            registry=self.registry,
            s3=self.s3,
            now=NOW,
        )

    def response_body(self, response):
        return json.loads(response["body"])

    def test_valid_exposure_is_written_once_with_server_fields(self):
        payload = base_payload()
        response = self.request(payload)

        self.assertEqual(202, response["statusCode"])
        self.assertEqual({"accepted": True, "duplicate": False}, self.response_body(response))
        self.assertEqual(ORIGIN, response["headers"]["Access-Control-Allow-Origin"])
        self.assertEqual(1, len(self.s3.objects))

        call = self.s3.put_calls[0]
        self.assertEqual("*", call["IfNoneMatch"])
        self.assertEqual("AES256", call["ServerSideEncryption"])
        self.assertEqual(
            f"experiment-events/raw/event_date=2026-08-20/{payload['event_id']}.json",
            call["Key"],
        )
        stored = json.loads(call["Body"])
        self.assertEqual("2026-08-20T15:00:00.000Z", stored["received_at"])
        self.assertEqual("test-version", stored["collector_version"])
        self.assertEqual("accepted", stored["risk_result"])
        self.assertNotIn("origin", stored)
        self.assertNotIn("headers", stored)

    def test_duplicate_is_idempotent_and_creates_no_second_object(self):
        payload = base_payload()
        first = self.request(payload)
        second = self.request(payload)

        self.assertEqual(202, first["statusCode"])
        self.assertEqual(202, second["statusCode"])
        self.assertEqual({"accepted": True, "duplicate": True}, self.response_body(second))
        self.assertEqual(1, len(self.s3.objects))

    def test_conditional_conflict_retries_and_is_not_called_a_duplicate(self):
        self.s3.fail_codes = ["ConditionalRequestConflict"]

        response = self.request(base_payload())

        self.assertEqual(202, response["statusCode"])
        self.assertEqual({"accepted": True, "duplicate": False}, self.response_body(response))
        self.assertEqual(2, len(self.s3.put_calls))
        self.assertEqual(1, len(self.s3.objects))

    def test_repeated_conditional_conflict_fails_closed(self):
        self.s3.fail_codes = [
            "ConditionalRequestConflict",
            "ConditionalRequestConflict",
            "ConditionalRequestConflict",
        ]

        response = self.request(base_payload())

        self.assertEqual(503, response["statusCode"])
        self.assertEqual(
            {"accepted": False, "code": "storage_unavailable"},
            self.response_body(response),
        )
        self.assertEqual(3, len(self.s3.put_calls))
        self.assertEqual(0, len(self.s3.objects))

    def test_production_registry_starts_empty_and_rejects_preview_experiment(self):
        production = load_registry(str(self.registry_path), "production")
        response = handle_request(
            {
                "requestContext": {"http": {"method": "POST"}},
                "headers": {"origin": ORIGIN, "content-type": "application/json"},
                "body": json.dumps(base_payload()),
            },
            config=self.config,
            registry=production,
            s3=self.s3,
            now=NOW,
        )

        self.assertEqual(400, response["statusCode"])
        self.assertEqual(0, len(self.s3.objects))

    def test_origin_and_preflight_are_fail_closed(self):
        denied = self.request(
            base_payload(),
            headers={"origin": "https://attacker.example", "content-type": "application/json"},
        )
        self.assertEqual(403, denied["statusCode"])
        self.assertNotIn("Access-Control-Allow-Origin", denied["headers"])

        preflight = handle_request(
            {
                "requestContext": {"http": {"method": "OPTIONS"}},
                "headers": {"origin": ORIGIN},
            },
            config=self.config,
            registry=self.registry,
            s3=self.s3,
            now=NOW,
        )
        self.assertEqual(204, preflight["statusCode"])
        self.assertEqual(ORIGIN, preflight["headers"]["Access-Control-Allow-Origin"])

    def test_content_type_method_and_body_size_are_enforced(self):
        wrong_type = self.request(
            base_payload(), headers={"origin": ORIGIN, "content-type": "text/plain"}
        )
        self.assertEqual(415, wrong_type["statusCode"])

        wrong_method = self.request(
            base_payload(), requestContext={"http": {"method": "GET"}}
        )
        self.assertEqual(405, wrong_method["statusCode"])

        oversized = self.request(base_payload(), body="x" * 4097)
        self.assertEqual(413, oversized["statusCode"])
        self.assertEqual(0, len(self.s3.objects))

    def test_base64_body_is_supported_without_relaxing_validation(self):
        payload = base_payload()
        raw = json.dumps(payload).encode("utf-8")
        response = self.request(
            payload,
            body=base64.b64encode(raw).decode("ascii"),
            isBase64Encoded=True,
        )
        self.assertEqual(202, response["statusCode"])

    def test_unknown_missing_duplicate_and_nested_fields_are_rejected(self):
        unknown = base_payload()
        unknown["email"] = "customer@example.com"
        self.assertEqual(400, self.request(unknown)["statusCode"])

        missing = base_payload()
        missing.pop("meta_ad_id")
        self.assertEqual(400, self.request(missing)["statusCode"])

        raw_duplicate = json.dumps(base_payload())[:-1] + ',"event_name":"order_completed"}'
        duplicate_response = self.request(base_payload(), body=raw_duplicate)
        self.assertEqual(400, duplicate_response["statusCode"])

        nested = base_payload()
        nested["utm_source"] = {"value": "meta"}
        self.assertEqual(400, self.request(nested)["statusCode"])
        self.assertEqual(0, len(self.s3.objects))

    def test_pii_urls_and_meta_click_identifiers_are_rejected_in_values(self):
        cases = [
            ("page_path", "/customer/customer@example.com"),
            ("page_path", "/call/+421900123456"),
            ("page_path", "/source/192.168.1.10"),
            ("page_path", "/go/https://example.com"),
            ("page_path", "/go/fbclid-value"),
            ("page_path", "/customer/name%40example.com"),
        ]
        for field, value in cases:
            with self.subTest(value=value):
                payload = base_payload()
                payload[field] = value
                response = self.request(payload)
                self.assertEqual(400, response["statusCode"])
        self.assertEqual(0, len(self.s3.objects))

    def test_uuid_timestamp_consent_experiment_and_variation_are_enforced(self):
        invalid_cases = []
        bad_uuid = base_payload()
        bad_uuid["device_id"] = str(uuid.uuid1())
        invalid_cases.append((bad_uuid, 400))
        old = base_payload()
        old["occurred_at"] = (NOW - timedelta(hours=25)).isoformat().replace("+00:00", "Z")
        invalid_cases.append((old, 400))
        future = base_payload()
        future["occurred_at"] = (NOW + timedelta(minutes=6)).isoformat().replace("+00:00", "Z")
        invalid_cases.append((future, 400))
        no_consent = base_payload()
        no_consent["consent_state"] = "denied"
        invalid_cases.append((no_consent, 403))
        inactive = base_payload()
        inactive["experiment_id"] = "vevo-sk-unknown-001"
        invalid_cases.append((inactive, 400))
        bad_variation = base_payload()
        bad_variation["variation_id"] = "surprise"
        invalid_cases.append((bad_variation, 400))

        for payload, expected_status in invalid_cases:
            with self.subTest(payload=payload):
                self.assertEqual(expected_status, self.request(payload)["statusCode"])
        self.assertEqual(0, len(self.s3.objects))

    def test_event_specific_schemas_and_page_compatibility(self):
        add = base_payload("add_to_cart", "product")
        add["product_id"] = "1531"
        self.assertEqual(202, self.request(add)["statusCode"])

        order = base_payload("order_completed", "checkout_success")
        order["page_path"] = "/e/orders/finish"
        order["transaction_id"] = "2602008438"
        self.assertEqual(202, self.request(order)["statusCode"])
        stored_order = json.loads(self.s3.put_calls[-1]["Body"])
        self.assertEqual("2602008438", stored_order["transaction_id"])
        for forbidden in ("revenue", "price", "email", "customer_id"):
            self.assertNotIn(forbidden, stored_order)

        wrong_page = base_payload("add_to_cart", "home")
        wrong_page["product_id"] = "1531"
        wrong_page["page_path"] = "/"
        self.assertEqual(400, self.request(wrong_page)["statusCode"])

        money = base_payload("order_completed", "checkout_success")
        money["page_path"] = "/e/orders/finish"
        money["transaction_id"] = "2602008438"
        money["revenue"] = 42.0
        self.assertEqual(400, self.request(money)["statusCode"])

    def test_performance_and_error_events_store_no_details(self):
        page_load_id = str(uuid.uuid4())
        vital = base_payload("performance_vital", "product")
        vital.update({"page_load_id": page_load_id, "vital_name": "lcp_ms", "vital_value": 1300})
        self.assertEqual(202, self.request(vital)["statusCode"])

        invalid_vital = dict(vital)
        invalid_vital["event_id"] = str(uuid.uuid4())
        invalid_vital["vital_value"] = 60_001
        self.assertEqual(400, self.request(invalid_vital)["statusCode"])

        error = base_payload("client_error_observed", "product")
        error.update({"page_load_id": page_load_id, "error_kind": "runtime_error"})
        self.assertEqual(202, self.request(error)["statusCode"])
        stored_error = json.loads(self.s3.put_calls[-1]["Body"])
        self.assertEqual("runtime_error", stored_error["error_kind"])
        for forbidden in ("message", "stack", "filename", "url", "line", "column"):
            self.assertNotIn(forbidden, stored_error)

        error_with_message = dict(error)
        error_with_message["event_id"] = str(uuid.uuid4())
        error_with_message["message"] = "secret"
        self.assertEqual(400, self.request(error_with_message)["statusCode"])

        checkout_error = base_payload("client_error_observed", "checkout_success")
        checkout_error.update(
            {
                "page_path": "/e/orders/finish",
                "page_load_id": str(uuid.uuid4()),
                "error_kind": "unhandled_rejection",
            }
        )
        self.assertEqual(202, self.request(checkout_error)["statusCode"])

    def test_storage_failure_is_generic_and_does_not_echo_payload(self):
        payload = base_payload()
        self.s3.fail_code = "AccessDenied"
        response = self.request(payload)

        self.assertEqual(503, response["statusCode"])
        body = response["body"]
        self.assertNotIn(payload["event_id"], body)
        self.assertNotIn(payload["device_id"], body)
        self.assertEqual({"accepted": False, "code": "storage_unavailable"}, json.loads(body))

    def test_kms_configuration_uses_only_the_supplied_key(self):
        config = CollectorConfig(
            **{
                **self.config.__dict__,
                "kms_key_arn": "arn:aws:kms:eu-central-1:123456789012:key/test",
            }
        )
        response = handle_request(
            {
                "requestContext": {"http": {"method": "POST"}},
                "headers": {"origin": ORIGIN, "content-type": "application/json"},
                "body": json.dumps(base_payload()),
            },
            config=config,
            registry=self.registry,
            s3=self.s3,
            now=NOW,
        )
        self.assertEqual(202, response["statusCode"])
        self.assertEqual("aws:kms", self.s3.put_calls[-1]["ServerSideEncryption"])
        self.assertEqual(config.kms_key_arn, self.s3.put_calls[-1]["SSEKMSKeyId"])


if __name__ == "__main__":
    unittest.main()
