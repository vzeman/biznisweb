from __future__ import annotations

import json
import tempfile
import unittest
import uuid
from pathlib import Path

from scripts.summarize_growthbook_activation_smoke import summarize


FROM = "2026-08-25T05:34:30Z"
THROUGH = "2026-08-25T05:44:30Z"
COLLECTOR_VERSION = "git-57b29c3b166eabbbabee4d3b8e69d1b56e2ae8e2"


def event(*, event_id: str, device_id: str, variation: str, received_at: str) -> dict:
    return {
        "schema_version": 1,
        "event_id": event_id,
        "event_name": "experiment_exposure",
        "occurred_at": received_at,
        "device_id": device_id,
        "page_path": "/p-1531/parfum-do-prania-vevo-no-07-ylang-absolute",
        "page_type": "product",
        "consent_state": "analytics_granted",
        "experiment_id": "vevo-sk-aa-001",
        "variation_id": variation,
        "utm_source": None,
        "utm_medium": None,
        "meta_campaign_id": None,
        "meta_adset_id": None,
        "meta_ad_id": None,
        "meta_placement": None,
        "received_at": received_at,
        "event_date": "2026-08-25",
        "collector_version": COLLECTOR_VERSION,
        "risk_result": "accepted",
    }


class GrowthBookActivationSmokeReducerTests(unittest.TestCase):
    def write_events(self, rows: list[dict]) -> Path:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        for index, row in enumerate(rows):
            (root / f"{index}.json").write_text(json.dumps(row), encoding="utf-8")
        return root

    def test_reduces_repeat_assignment_without_exporting_identity(self) -> None:
        device = str(uuid.uuid4())
        root = self.write_events(
            [
                event(
                    event_id=str(uuid.uuid4()),
                    device_id=device,
                    variation="control",
                    received_at="2026-08-25T05:40:00Z",
                ),
                event(
                    event_id=str(uuid.uuid4()),
                    device_id=device,
                    variation="control",
                    received_at="2026-08-25T05:42:00Z",
                ),
            ]
        )
        result = summarize(root, FROM, THROUGH, COLLECTOR_VERSION)
        self.assertEqual(2, result["target_exposure_count"])
        self.assertEqual(1, result["unique_exposed_device_count"])
        self.assertEqual(1, result["repeat_exposed_device_count"])
        self.assertEqual(1, result["sticky_consistent_repeat_device_count"])
        self.assertEqual(0, result["sticky_inconsistent_device_count"])
        self.assertEqual({"control": 2, "variant": 0}, result["variation_exposure_counts"])
        self.assertFalse(result["contains_event_or_device_ids"])
        self.assertNotIn(device, json.dumps(result))

    def test_detects_cross_variation_sticky_conflict(self) -> None:
        device = str(uuid.uuid4())
        root = self.write_events(
            [
                event(
                    event_id=str(uuid.uuid4()),
                    device_id=device,
                    variation="control",
                    received_at="2026-08-25T05:40:00Z",
                ),
                event(
                    event_id=str(uuid.uuid4()),
                    device_id=device,
                    variation="variant",
                    received_at="2026-08-25T05:42:00Z",
                ),
            ]
        )
        result = summarize(root, FROM, THROUGH, COLLECTOR_VERSION)
        self.assertEqual(1, result["repeat_exposed_device_count"])
        self.assertEqual(1, result["sticky_inconsistent_device_count"])

    def test_rejects_extra_raw_field(self) -> None:
        row = event(
            event_id=str(uuid.uuid4()),
            device_id=str(uuid.uuid4()),
            variation="control",
            received_at="2026-08-25T05:40:00Z",
        )
        row["email"] = "forbidden@example.com"
        root = self.write_events([row])
        with self.assertRaisesRegex(ValueError, "field set drift"):
            summarize(root, FROM, THROUGH, COLLECTOR_VERSION)

    def test_rejects_collector_version_not_bound_to_runtime(self) -> None:
        root = self.write_events(
            [
                event(
                    event_id=str(uuid.uuid4()),
                    device_id=str(uuid.uuid4()),
                    variation="control",
                    received_at="2026-08-25T05:40:00Z",
                )
            ]
        )
        with self.assertRaisesRegex(ValueError, "collector version drift"):
            summarize(root, FROM, THROUGH, "git-" + "0" * 40)

    def test_ignores_valid_records_outside_frozen_window(self) -> None:
        root = self.write_events(
            [
                event(
                    event_id=str(uuid.uuid4()),
                    device_id=str(uuid.uuid4()),
                    variation="variant",
                    received_at="2026-08-25T05:50:00Z",
                )
            ]
        )
        result = summarize(root, FROM, THROUGH, COLLECTOR_VERSION)
        self.assertEqual(0, result["raw_event_count"])
        self.assertEqual(0, result["target_exposure_count"])


if __name__ == "__main__":
    unittest.main()
