import io
import json
import unittest
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse

from scripts.diagnose_meta_attribution_shape import (
    PURCHASE_ACTION,
    _api_json,
    collect_shape,
)


class Response:
    def __init__(self, payload):
        self.payload = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, _limit=-1):
        return self.payload


class MetaAttributionShapeDiagnosticTests(unittest.TestCase):
    def test_emits_only_hashed_identity_and_field_names(self):
        calls = []
        payloads = iter([
            {"data": [{"id": "123456789", "name": "Private campaign"}]},
            {"data": [{
                "spend": "99.90",
                "actions": [{"action_type": PURCHASE_ACTION, "value": "9"}],
                "cost_per_action_type": [{"action_type": PURCHASE_ACTION, "value": "11.10"}],
            }]},
        ])

        def opener(request, timeout):
            calls.append((request, timeout))
            return Response(next(payloads))

        result = collect_shape(
            project="roy",
            account_id="987654321",
            token="private-token",
            date_from="2026-10-30",
            date_to="2026-11-05",
            opener=opener,
        )
        serialized = json.dumps(result, sort_keys=True)
        self.assertNotIn("private-token", serialized)
        self.assertNotIn("987654321", serialized)
        self.assertNotIn("123456789", serialized)
        self.assertNotIn("Private campaign", serialized)
        self.assertNotIn("99.90", serialized)
        self.assertNotIn("11.10", serialized)
        self.assertEqual(result["campaigns_seen"], 1)
        self.assertEqual(
            result["campaign_shapes"][0]["action_shapes"],
            [{"class": "purchase", "fields": ["action_type", "value"]}],
        )
        self.assertEqual(
            result["campaign_shapes"][0]["cost_per_action_shapes"],
            [{"class": "purchase", "fields": ["action_type", "value"]}],
        )
        for request, timeout in calls:
            self.assertNotIn("private-token", request.full_url)
            self.assertEqual(request.headers["Authorization"], "Bearer private-token")
            self.assertEqual(timeout, 45)

    def test_binds_exact_requested_window_and_uses_cursor_not_next_url(self):
        requests = []
        payloads = iter([
            {"data": [], "paging": {"next": "https://example.invalid/?access_token=leak", "cursors": {"after": "cursor"}}},
            {"data": [{"id": "123"}]},
            {"data": [{"actions": [{"action_type": PURCHASE_ACTION, "7d_click": "2"}]}]},
        ])

        def opener(request, timeout):
            requests.append(request)
            self.assertEqual(timeout, 45)
            return Response(next(payloads))

        result = collect_shape(
            project="roy",
            account_id="1",
            token="token",
            date_from="2026-10-30",
            date_to="2026-11-05",
            opener=opener,
        )
        second_query = parse_qs(urlparse(requests[1].full_url).query)
        insight_query = parse_qs(urlparse(requests[2].full_url).query)
        self.assertEqual(second_query["after"], ["cursor"])
        self.assertNotIn("access_token", requests[1].full_url)
        self.assertEqual(json.loads(insight_query["action_attribution_windows"][0]), ["7d_click"])
        self.assertEqual(insight_query["action_report_time"], ["impression"])
        self.assertEqual(
            result["campaign_shapes"][0]["action_shapes"],
            [{"class": "purchase", "fields": ["7d_click", "action_type"]}],
        )

    def test_http_failure_does_not_expose_provider_message_or_token(self):
        body = io.BytesIO(json.dumps({"error": {
            "code": 190,
            "error_subcode": 123,
            "type": "OAuthException",
            "message": "secret provider message private-token",
        }}).encode("utf-8"))

        def opener(request, timeout):
            self.assertEqual(timeout, 45)
            raise HTTPError(request.full_url, 400, "private-token", {}, body)

        with self.assertRaises(RuntimeError) as caught:
            _api_json("/act_1/campaigns", {"fields": "id"}, "private-token", opener=opener)
        message = str(caught.exception)
        self.assertIn("status=400 code=190 subcode=123 type=OAuthException", message)
        self.assertNotIn("private-token", message)
        self.assertNotIn("secret provider message", message)

    def test_rejects_unbounded_date_window(self):
        with self.assertRaisesRegex(ValueError, "date-window-invalid"):
            collect_shape(
                project="roy",
                account_id="1",
                token="token",
                date_from="2026-09-01",
                date_to="2026-11-05",
                opener=lambda *_args, **_kwargs: None,
            )


if __name__ == "__main__":
    unittest.main()
