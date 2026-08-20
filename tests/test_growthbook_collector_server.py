from __future__ import annotations

import http.client
import json
import threading
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path

from growthbook_collector.handler import CollectorConfig
from growthbook_collector.server import HEALTH_MARKER, HOST_MARKER, create_server


ORIGIN = "https://www.vevo.sk"


class FakeS3:
    def __init__(self) -> None:
        self.put_calls: list[dict[str, object]] = []

    def put_object(self, **kwargs: object) -> None:
        self.put_calls.append(kwargs)


def exposure_payload() -> dict[str, object]:
    return {
        "schema_version": 1,
        "event_id": str(uuid.uuid4()),
        "event_name": "experiment_exposure",
        "occurred_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
            "+00:00", "Z"
        ),
        "device_id": str(uuid.uuid4()),
        "page_path": "/p-1531/example",
        "page_type": "product",
        "consent_state": "analytics_granted",
        "experiment_id": "vevo-sk-aa-001",
        "variation_id": "control",
        "utm_source": "meta",
        "utm_medium": "paid_social",
        "meta_campaign_id": "123",
        "meta_adset_id": "456",
        "meta_ad_id": "789",
        "meta_placement": "instagram_feed",
    }


class GrowthBookCollectorServerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.s3 = FakeS3()
        self.config = CollectorConfig(
            bucket="unit-test-bucket",
            prefix="experiment-events/raw",
            region="eu-central-1",
            environment="preview",
            allowed_origins=frozenset({ORIGIN}),
            collector_version="git-unit-test",
            registry_path=Path("unused.json"),
            max_body_bytes=4096,
        )
        self.registry = {
            "vevo-sk-aa-001": {
                "variations": frozenset({"control", "variant"}),
                "exposure_page_types": frozenset({"home", "product", "category"}),
                "health_page_types": frozenset(
                    {"home", "product", "category", "checkout_success"}
                ),
                "allowed_events": frozenset(
                    {
                        "experiment_exposure",
                        "add_to_cart",
                        "order_completed",
                        "performance_vital",
                        "client_error_observed",
                    }
                ),
            }
        }
        self.server = create_server(
            host="127.0.0.1",
            port=0,
            config=self.config,
            registry=self.registry,
            s3=self.s3,
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.port = int(self.server.server_address[1])

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.assertFalse(self.thread.is_alive())

    def request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, str], dict[str, object]]:
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=2)
        connection.request(method, path, body=body, headers=headers or {})
        response = connection.getresponse()
        raw = response.read()
        result_headers = {key.lower(): value for key, value in response.getheaders()}
        connection.close()
        return response.status, result_headers, json.loads(raw or b"{}")

    def test_local_health_and_host_marker_have_exact_build_identity(self) -> None:
        status, headers, health = self.request("GET", "/health")
        self.assertEqual(200, status)
        self.assertEqual(HEALTH_MARKER, health["marker"])
        self.assertEqual("git-unit-test", health["version"])
        self.assertEqual("no-store", headers["cache-control"])

        status, _, marker = self.request("GET", "/marker.json")
        self.assertEqual(200, status)
        self.assertEqual(HOST_MARKER, marker["marker"])
        self.assertEqual("/app", marker["runtime_path"])
        self.assertEqual("preview", marker["environment"])

    def test_exact_post_route_reuses_validated_append_only_collector(self) -> None:
        body = json.dumps(exposure_payload(), separators=(",", ":")).encode("utf-8")
        status, headers, response = self.request(
            "POST",
            "/v1/events",
            body,
            {"Content-Type": "application/json", "Origin": ORIGIN},
        )
        self.assertEqual(202, status)
        self.assertEqual({"accepted": True, "duplicate": False}, response)
        self.assertEqual(ORIGIN, headers["access-control-allow-origin"])
        self.assertEqual(1, len(self.s3.put_calls))
        self.assertEqual("*", self.s3.put_calls[0]["IfNoneMatch"])

    def test_preflight_is_exact_origin_and_does_not_write(self) -> None:
        status, headers, response = self.request(
            "OPTIONS",
            "/v1/events",
            headers={"Origin": ORIGIN},
        )
        self.assertEqual(204, status)
        self.assertEqual({}, response)
        self.assertEqual(ORIGIN, headers["access-control-allow-origin"])
        self.assertEqual([], self.s3.put_calls)

    def test_unknown_paths_queries_and_origins_fail_closed(self) -> None:
        for path in ("/", "/health?detail=true", "/v1/events?debug=1", "/marker.json?x=1"):
            status, headers, response = self.request("GET", path)
            self.assertEqual(404, status)
            self.assertEqual({"code": "not_found", "ok": False}, response)
            self.assertNotIn("access-control-allow-origin", headers)

        body = json.dumps(exposure_payload()).encode("utf-8")
        status, headers, response = self.request(
            "POST",
            "/v1/events",
            body,
            {"Content-Type": "application/json", "Origin": "https://attacker.example"},
        )
        self.assertEqual(403, status)
        self.assertEqual("origin_not_allowed", response["code"])
        self.assertNotIn("access-control-allow-origin", headers)
        self.assertEqual([], self.s3.put_calls)

    def test_missing_and_oversized_bodies_are_rejected_before_storage(self) -> None:
        status, _, response = self.request(
            "POST",
            "/v1/events",
            headers={"Content-Type": "application/json", "Origin": ORIGIN},
        )
        self.assertEqual(400, status)
        self.assertEqual("invalid_body_size", response["code"])

        status, _, response = self.request(
            "POST",
            "/v1/events",
            b"x" * 4097,
            {"Content-Type": "application/json", "Origin": ORIGIN},
        )
        self.assertEqual(413, status)
        self.assertEqual("invalid_body_size", response["code"])
        self.assertEqual([], self.s3.put_calls)


if __name__ == "__main__":
    unittest.main()
