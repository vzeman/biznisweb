from __future__ import annotations

import copy
import hashlib
import json
import unittest
from datetime import datetime, timezone

from scripts import build_growthbook_cta_lifecycle_preflight as builder


def _context() -> dict:
    return {
        "schema_version": 1,
        "target_experiment_id": builder.TARGET_EXPERIMENT,
        "source_experiment_id": builder.SOURCE_EXPERIMENT,
        "source_from_utc": "2026-08-25T22:00:00Z",
        "source_through_utc": "2026-09-02T22:00:00Z",
        "minimum_collection_due_utc": "2026-09-23T22:00:00Z",
        "order_window_days": 7,
        "lifecycle_checkpoint_days": 14,
        "minimum_followup_days_after_source_end": 21,
        "manifest_sha256": "a" * 64,
        "source_completion_sha256": "b" * 64,
        "source_aa_snapshot_sha256": "c" * 64,
        "query_template_sha256": "d" * 64,
    }


def _quality() -> tuple[bytes, str]:
    value = {
        "metric_contract_version": builder.METRIC_CONTRACT,
        "experiment_id": builder.SOURCE_EXPERIMENT,
        "facts_generated_at": "2026-09-24T03:45:00.000Z",
    }
    return (
        builder.canonical_json_bytes(value),
        "experiment-events/curated/quality/experiment_id=vevo-sk-aa-001/"
        "facts_generated_at=20260924T034500Z.json",
    )


def _fact(device_id: str = "00000000-0000-4000-8000-000000000001") -> dict:
    return {
        "metric_contract_version": builder.METRIC_CONTRACT,
        "device_id": device_id,
        "first_exposure_at": "2026-08-26T10:00:00.000Z",
        "variation_id": "control",
        "meta_campaign_id": None,
        "meta_adset_id": None,
        "meta_ad_id": None,
        "meta_placement": None,
        "add_to_cart_24h": 1,
        "purchase_converted": 1,
        "joined_order_count": 1,
        "net_revenue_eur": 0.0,
        "cm1_eur": 0.0,
        "cancelled_order_count": 0,
        "refunded_order_count": 1,
        "immature_order_count": 0,
        "client_error_observed": 0,
        "contaminated": 0,
        "eligible": 1,
        "order_attribution_eligible": 1,
        "order_attribution_issue": "",
        "unmatched_transaction_count": 0,
        "ambiguous_transaction_count": 0,
        "exclusion_reason": "",
        "facts_generated_at": "2026-09-24T03:45:00.000Z",
    }


def _athena(*, cm1: str = "0.00", immature: str = "0") -> dict:
    values = ["1", "1", "1", immature, cm1, "0", "1", "1", "2026-09-24T03:45:00.000Z"]
    return {
        "ResultSet": {
            "Rows": [
                {"Data": [{"VarCharValue": name} for name in builder.ATHENA_COLUMNS]},
                {"Data": [{"VarCharValue": value} for value in values]},
            ]
        }
    }


class GrowthBookCtaLifecyclePreflightTests(unittest.TestCase):
    def test_quality_object_is_bound_to_the_retained_direct_cohort_generation(
        self,
    ) -> None:
        selected = builder.select_quality_context(
            [_fact()],
            source_from_utc=_context()["source_from_utc"],
            source_through_utc=_context()["source_through_utc"],
            minimum_due_utc=_context()["minimum_collection_due_utc"],
        )

        self.assertEqual("2026-09-24T03:45:00.000Z", selected["facts_generated_at"])
        self.assertEqual(_quality()[1], selected["quality_key"])

        mixed = _fact("00000000-0000-4000-8000-000000000002")
        mixed["facts_generated_at"] = "2026-09-25T03:45:00.000Z"
        with self.assertRaisesRegex(
            builder.LifecyclePreflightError, "multiple curated facts generations"
        ):
            builder.select_quality_context(
                [_fact(), mixed],
                source_from_utc=_context()["source_from_utc"],
                source_through_utc=_context()["source_through_utc"],
                minimum_due_utc=_context()["minimum_collection_due_utc"],
            )

    def test_builds_source_explicit_identity_free_completed_aa_preflight(self) -> None:
        quality, key = _quality()
        observation = builder.build_observation(
            context=_context(),
            quality_bytes=quality,
            quality_key=key,
            direct_rows=[_fact()],
            athena_result=_athena(),
            workflow_run_id="12345678901",
            main_commit="e" * 40,
        )

        self.assertEqual(builder.SOURCE_EXPERIMENT, observation["source_experiment_id"])
        self.assertEqual(builder.TARGET_EXPERIMENT, observation["target_experiment_id"])
        self.assertFalse(observation["cta_outcome_data_read"])
        self.assertFalse(observation["contains_event_or_device_identity"])
        self.assertNotIn("device_id", observation)
        self.assertEqual(
            hashlib.sha256(quality).hexdigest(),
            observation["reporting_quality_object_sha256"],
        )

    def test_rejects_cm1_or_lifecycle_count_mismatch(self) -> None:
        quality, key = _quality()
        with self.assertRaisesRegex(builder.LifecyclePreflightError, "CM1 parity"):
            builder.build_observation(
                context=_context(),
                quality_bytes=quality,
                quality_key=key,
                direct_rows=[_fact()],
                athena_result=_athena(cm1="1.00"),
                workflow_run_id="12345678901",
                main_commit="e" * 40,
            )

    def test_rejects_immature_orders_or_missing_lifecycle_case(self) -> None:
        quality, key = _quality()
        immature = _fact()
        immature["immature_order_count"] = 1
        with self.assertRaisesRegex(builder.LifecyclePreflightError, "immature"):
            builder.build_observation(
                context=_context(),
                quality_bytes=quality,
                quality_key=key,
                direct_rows=[immature],
                athena_result=_athena(immature="1"),
                workflow_run_id="12345678901",
                main_commit="e" * 40,
            )

        no_case = _fact()
        no_case["refunded_order_count"] = 0
        with self.assertRaisesRegex(
            builder.LifecyclePreflightError, "cancelled/refunded/creditnoted"
        ):
            builder.build_observation(
                context=_context(),
                quality_bytes=quality,
                quality_key=key,
                direct_rows=[no_case],
                athena_result=_athena(),
                workflow_run_id="12345678901",
                main_commit="e" * 40,
            )

    def test_prepare_requires_completed_pass_stop_and_full_21_days(self) -> None:
        manifest = json.loads(builder.DEFAULT_MANIFEST.read_text(encoding="utf-8"))
        completion = {
            "status": "production_aa_stopped_verified_cta_activation_blocked",
            "aa_pass": {"status": "verified_pass", "verdict": "PASS"},
            "stop_readback": {"status": "verified_zero_allocation"},
        }
        snapshot = {
            "measurement_window": {
                "resolution_status": "resolved",
                "from_utc": "2026-08-25T22:00:00Z",
                "resolved_through_utc": "2026-09-02T22:00:00Z",
            }
        }
        query_sha = hashlib.sha256(
            builder.DEFAULT_QUERY_TEMPLATE.read_bytes()
        ).hexdigest()
        context = builder.prepare_context(
            manifest,
            completion,
            snapshot,
            manifest_sha256="a" * 64,
            completion_sha256="b" * 64,
            snapshot_sha256="c" * 64,
            query_template_sha256=query_sha,
            now=datetime(2026, 9, 23, 22, 0, tzinfo=timezone.utc),
        )
        self.assertEqual("2026-09-23T22:00:00Z", context["minimum_collection_due_utc"])

        with self.assertRaisesRegex(builder.LifecyclePreflightError, "follow-up"):
            builder.prepare_context(
                manifest,
                completion,
                snapshot,
                manifest_sha256="a" * 64,
                completion_sha256="b" * 64,
                snapshot_sha256="c" * 64,
                query_template_sha256=query_sha,
                now=datetime(2026, 9, 23, 21, 59, 59, tzinfo=timezone.utc),
            )

        failed = copy.deepcopy(completion)
        failed["aa_pass"]["verdict"] = "FAIL"
        with self.assertRaisesRegex(builder.LifecyclePreflightError, "PASS"):
            builder.prepare_context(
                manifest,
                failed,
                snapshot,
                manifest_sha256="a" * 64,
                completion_sha256="b" * 64,
                snapshot_sha256="c" * 64,
                query_template_sha256=query_sha,
                now=datetime(2026, 9, 24, tzinfo=timezone.utc),
            )

    def test_sql_is_completed_aa_only_and_contains_no_cta_result_source(self) -> None:
        template = builder.DEFAULT_QUERY_TEMPLATE.read_text(encoding="utf-8")
        rendered = builder.render_query(template, _context())
        self.assertIn("experiment_id = 'vevo-sk-aa-001'", rendered)
        self.assertNotIn("vevo-sk-product-cta-color-001", rendered)
        self.assertNotIn("variation_id", rendered.lower())


if __name__ == "__main__":
    unittest.main()
