from __future__ import annotations

import copy
import hashlib
import json
import pathlib
import unittest
from datetime import UTC, datetime

from scripts import build_growthbook_cta_baseline_observation as builder
from scripts.evaluate_growthbook_aa import evaluate, load_config
from scripts.record_growthbook_aa_completion import (
    canonical_json_bytes,
    record_pass,
    record_stop,
)
from tests.test_growthbook_aa_completion_recorder import (
    SNAPSHOT_COMMIT,
    SNAPSHOT_RUN_ID,
    aa_snapshot,
    build_snapshot_manifest,
    stop_observation,
)


ROOT = pathlib.Path(__file__).resolve().parents[1]


def load(name: str) -> dict[str, object]:
    return json.loads((ROOT / "projects" / "vevo" / name).read_text(encoding="utf-8"))


def athena_result(exposed: str = "451", converted: str = "148") -> dict[str, object]:
    columns = ["exposed_devices", "converted_devices"]
    return {
        "ResultSet": {
            "Rows": [
                {"Data": [{"VarCharValue": value} for value in columns]},
                {"Data": [{"VarCharValue": exposed}, {"VarCharValue": converted}]},
            ],
            "ResultSetMetadata": {"ColumnInfo": [{"Name": value} for value in columns]},
        }
    }


class GrowthBookCtaBaselineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.manifest = load("growthbook_cta_baseline.json")
        self.activation = load("growthbook_production_aa_activation.json")
        self.plan = load("growthbook_cta_sample_plan.json")
        self.pending_completion = load("growthbook_production_aa_completion.json")
        self.pending_snapshot_manifest = load("growthbook_aa_snapshot.json")
        self.snapshot_manifest = build_snapshot_manifest()
        snapshot = aa_snapshot()
        decision = evaluate(
            snapshot,
            load_config(ROOT / "projects" / "vevo" / "growthbook_aa_acceptance.json"),
        )
        snapshot_sha = hashlib.sha256(canonical_json_bytes(snapshot)).hexdigest()
        decision_sha = hashlib.sha256(canonical_json_bytes(decision)).hexdigest()
        passed = record_pass(
            self.pending_completion,
            self.activation,
            self.snapshot_manifest,
            snapshot,
            decision,
            workflow_run_id=SNAPSHOT_RUN_ID,
            main_commit=SNAPSHOT_COMMIT,
            snapshot_sha256=snapshot_sha,
            decision_sha256=decision_sha,
        )
        self.stop_observation = stop_observation(
            snapshot_sha256=snapshot_sha,
            decision_sha256=decision_sha,
        )
        self.completion, self.workspace = record_stop(
            passed,
            self.activation,
            self.snapshot_manifest,
            load("growthbook_workspace.json"),
            self.stop_observation,
            observation_sha256=hashlib.sha256(
                canonical_json_bytes(self.stop_observation)
            ).hexdigest(),
        )
        self.workspace["state"] = (
            "production_aa_completed_cta_sample_freeze_pro_quantiles_verified"
        )
        self.ready_at = datetime(2026, 9, 2, 22, 0, tzinfo=UTC)

    def ready_kwargs(self) -> dict[str, object]:
        return {
            "manifest": self.manifest,
            "completion": self.completion,
            "snapshot_manifest": self.snapshot_manifest,
            "activation": self.activation,
            "stop_observation": self.stop_observation,
            "plan": self.plan,
            "workspace": self.workspace,
            "now_utc": self.ready_at,
        }

    def test_checked_in_contract_is_valid_and_has_no_activation_path(self) -> None:
        builder.validate_manifest(self.manifest)
        boundaries = self.manifest["release_boundaries"]
        self.assertTrue(boundaries["main_only"])
        self.assertTrue(boundaries["aws_aggregate_reads_only"])
        self.assertFalse(boundaries["growthbook_mutation_allowed"])
        self.assertFalse(boundaries["meta_ads_mutation_allowed"])
        self.assertFalse(boundaries["biznisweb_mutation_allowed"])
        self.assertFalse(boundaries["price_cart_checkout_order_mutation_allowed"])
        self.assertFalse(boundaries["cta_activation_allowed"])

    def test_collection_fails_before_verified_pass_and_stop(self) -> None:
        with self.assertRaisesRegex(
            builder.CtaBaselineError,
            "verified A/A PASS and stop readback",
        ):
            builder.render_query(
                manifest=self.manifest,
                completion=self.pending_completion,
                snapshot_manifest=self.pending_snapshot_manifest,
                activation=self.activation,
                stop_observation=None,
                plan=self.plan,
                workspace=load("growthbook_workspace.json"),
                now_utc=self.ready_at,
            )

    def test_collection_fails_until_full_24_hour_followup(self) -> None:
        kwargs = self.ready_kwargs()
        kwargs["now_utc"] = datetime(2026, 9, 2, 21, 59, 59, tzinfo=UTC)
        with self.assertRaisesRegex(
            builder.CtaBaselineError, "follow-up is incomplete"
        ):
            builder.render_query(**kwargs)

    def test_renders_only_the_frozen_product_baseline_window(self) -> None:
        query = builder.render_query(**self.ready_kwargs())
        self.assertNotIn("__", query)
        self.assertIn("2026-08-25T22:00:00Z", query)
        self.assertIn("2026-09-01T22:00:00Z", query)
        self.assertIn("2026-09-02T22:00:00Z", query)
        self.assertIn("BETWEEN '2026-08-25' AND '2026-09-01'", query)
        self.assertIn("BETWEEN '2026-08-25' AND '2026-09-02'", query)
        self.assertIn("raw.variation_id = eligible.variation_id", query.lower())
        self.assertIn("cart.variation_id = exposure.variation_id", query.lower())
        self.assertNotIn("'control'", query.lower())
        self.assertNotIn("'variant'", query.lower())
        self.assertNotIn("transaction_id", query.lower())
        self.assertNotIn("select *", query.lower())

    def test_builds_exact_hash_bound_identity_free_observation(self) -> None:
        observation = builder.build_observation(
            **self.ready_kwargs(),
            athena_result=athena_result(),
        )
        self.assertEqual(451, observation["exposed_devices"])
        self.assertEqual(148, observation["converted_devices"])
        self.assertEqual(
            self.completion["aa_pass"]["snapshot_sha256"],
            observation["aa_snapshot_sha256"],
        )
        self.assertEqual(
            "2026-08-25T22:00:00Z", observation["aa_window_started_at_utc"]
        )
        self.assertEqual("2026-09-01T22:00:00Z", observation["aa_window_ended_at_utc"])
        self.assertFalse(observation["contains_device_or_event_identity"])
        self.assertFalse(observation["contains_customer_or_order_data"])
        self.assertEqual(
            builder.canonical_json_bytes(observation),
            (
                json.dumps(
                    observation,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                + "\n"
            ).encode("utf-8"),
        )

    def test_rejects_small_invalid_or_multirow_aggregate(self) -> None:
        with self.assertRaisesRegex(builder.CtaBaselineError, "observation is invalid"):
            builder.build_observation(
                **self.ready_kwargs(),
                athena_result=athena_result("199", "50"),
            )
        invalid = athena_result()
        invalid["NextToken"] = "more-rows"
        with self.assertRaisesRegex(builder.CtaBaselineError, "more than one row"):
            builder.build_observation(
                **self.ready_kwargs(),
                athena_result=invalid,
            )
        invalid = athena_result()
        invalid["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"] = "451.0"
        with self.assertRaisesRegex(builder.CtaBaselineError, "nonnegative integers"):
            builder.build_observation(
                **self.ready_kwargs(),
                athena_result=invalid,
            )

    def test_rejects_query_hash_or_workspace_allocation_drift(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["query_contract"]["template_sha256"] = "0" * 64
        with self.assertRaisesRegex(builder.CtaBaselineError, "SHA-256 drift"):
            builder.validate_manifest(manifest)

        kwargs = self.ready_kwargs()
        workspace = copy.deepcopy(self.workspace)
        workspace["experiments"][1]["production_allocation_percent"] = 100
        kwargs["workspace"] = workspace
        with self.assertRaisesRegex(
            builder.CtaBaselineError, "CTA allocation is nonzero"
        ):
            builder.render_query(**kwargs)


if __name__ == "__main__":
    unittest.main()
