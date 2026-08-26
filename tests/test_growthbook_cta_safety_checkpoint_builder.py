from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from scripts import build_growthbook_cta_safety_checkpoint as builder
from scripts import record_growthbook_cta_safety_checkpoint as recorder
from scripts.evaluate_growthbook_cta_safety import canonical_json_bytes
from tests import test_growthbook_cta_window_checkpoint as window_fixtures


ROOT = Path(__file__).resolve().parents[1]


def load(relative: str) -> dict:
    return json.loads((ROOT / relative).read_text(encoding="utf-8"))


def athena_payload(*, duplicate_rows: int = 0) -> dict:
    def cell(value: object) -> dict[str, str]:
        return {"VarCharValue": str(value)}

    columns = [cell(value) for value in builder.RESULT_COLUMNS]
    rows = [
        {
            "Data": [
                cell("control"),
                cell(400),
                cell(250),
                cell(4),
                cell(1300),
                cell(150),
                cell(5),
                cell(duplicate_rows),
                cell(0),
            ]
        },
        {
            "Data": [
                cell("brand_contrast"),
                cell(400),
                cell(250),
                cell(4),
                cell(1350),
                cell(155),
                cell(6),
                cell(duplicate_rows),
                cell(0),
            ]
        },
    ]
    return {"ResultSet": {"Rows": [{"Data": columns}, *rows]}}


PRODUCT_HTML = """
<html><body>
<span>Kód produktu: <strong>07500</strong></span>
<p class="price"><span class="priceTaxValueNumber">25,90 €</span></p>
<button class="button s1-submitCart">Pridať do košíka</button>
</body></html>
"""


class GrowthBookCtaSafetyCheckpointBuilderTests(unittest.TestCase):
    def setUp(self) -> None:
        fixture = window_fixtures.GrowthBookCtaWindowCheckpointTests(
            methodName="runTest"
        )
        fixture.setUp()
        initial = load("projects/vevo/growthbook_cta_safety_monitoring.json")
        decision_bytes = (
            ROOT / "projects/vevo/growthbook_cta_decision_contract.json"
        ).read_bytes()
        self.manifest = recorder.initialize_monitoring(
            initial,
            fixture.activation,
            fixture.start_observation,
            source_hashes=recorder.source_hashes(
                fixture.activation,
                fixture.start_observation,
                decision_bytes,
            ),
        )

    def test_gate_is_exact_hourly_and_independent_of_missed_indexes(self) -> None:
        with self.assertRaisesRegex(builder.SafetyCheckpointSkip, "before-first-due"):
            builder.checkpoint_gate(
                self.manifest,
                datetime(2026, 9, 5, 6, 59, 59, tzinfo=UTC),
            )
        first = builder.checkpoint_gate(
            self.manifest,
            datetime(2026, 9, 5, 7, 5, tzinfo=UTC),
        )
        self.assertEqual(1, first["checkpoint_index"])
        self.assertEqual("2026-09-05T07:00:00Z", first["checkpoint_through_utc"])
        with self.assertRaisesRegex(
            builder.SafetyCheckpointSkip, "outside-exact-due-window"
        ):
            builder.checkpoint_gate(
                self.manifest,
                datetime(2026, 9, 5, 8, 0, 1, tzinfo=UTC),
            )
        third = builder.checkpoint_gate(
            self.manifest,
            datetime(2026, 9, 7, 7, 5, tzinfo=UTC),
        )
        self.assertEqual(3, third["checkpoint_index"])

    def test_gate_skips_closed_and_already_recorded_without_aws(self) -> None:
        with self.assertRaisesRegex(builder.SafetyCheckpointSkip, "monitoring-not-open"):
            builder.checkpoint_gate(
                load("projects/vevo/growthbook_cta_safety_monitoring.json"),
                datetime(2026, 9, 5, 7, 5, tzinfo=UTC),
            )
        recorded = copy.deepcopy(self.manifest)
        recorded["latest_checkpoint"].update(
            {
                "status": "recorded",
                "checkpoint_index": 1,
                "observed_at_utc": "2026-09-05T07:05:00Z",
                "eligible_devices_seen": 800,
                "evidence_sha256": "a" * 64,
                "decision_sha256": "b" * 64,
                "provenance_sha256": "c" * 64,
                "workflow_run_id": "60000000001",
                "main_commit": "d" * 40,
                "verdict": "CONTINUE",
                "stop_reasons": [],
            }
        )
        with self.assertRaisesRegex(builder.SafetyCheckpointSkip, "already-recorded"):
            builder.checkpoint_gate(
                recorded,
                datetime(2026, 9, 5, 7, 5, tzinfo=UTC),
            )

    def test_rendered_query_is_hash_bound_and_has_no_outcomes(self) -> None:
        query = builder.render_query(
            self.manifest,
            started_at_utc="2026-09-04T07:00:00Z",
            through_utc="2026-09-05T07:00:00Z",
        )
        self.assertIn("experiment_device_facts", query)
        self.assertIn("experiment_performance_facts", query)
        self.assertNotIn("__CTA_", query)
        self.assertNotIn("__CHECKPOINT_", query)
        lowered = query.lower()
        for forbidden in (
            "add_to_cart",
            "purchase",
            "conversion",
            "revenue",
            "cm1_eur",
            "meta_campaign",
            "winner",
        ):
            self.assertNotIn(forbidden, lowered)
        self.assertEqual(
            hashlib.sha256(builder.SQL_PATH.read_bytes()).hexdigest(),
            self.manifest["source_bindings"]["safety_query"]["sha256"],
        )

    def test_safe_bundle_is_canonical_identity_free_and_reproducible(self) -> None:
        bundle = builder.build_bundle(
            self.manifest,
            athena_payload(),
            checkpoint_index=1,
            assignment_started_at_utc="2026-09-04T07:00:00Z",
            observed_at_utc="2026-09-05T07:05:00Z",
            product_html=PRODUCT_HTML,
            cart_html="<html><body>Košík</body></html>",
            product_fetch_succeeded=True,
            cart_fetch_succeeded=True,
            repository="vzeman/biznisweb",
            workflow_run_id="60000000001",
            main_commit="a" * 40,
        )
        evidence, decision, provenance = bundle
        self.assertEqual("CONTINUE", decision["verdict"])
        self.assertTrue(all(evidence["commerce_readback"][key] for key in ("add_to_cart_text_unchanged", "price_unchanged")))
        self.assertEqual(builder.WORKFLOW, provenance["workflow"])
        self.assertEqual(
            hashlib.sha256(canonical_json_bytes(evidence)).hexdigest(),
            provenance["files"]["evidence_sha256"],
        )
        encoded = b"".join(canonical_json_bytes(value) for value in bundle).lower()
        for forbidden in (
            b'"device_id":',
            b'"event_id":',
            b'"customer_id":',
            b'"order_id":',
        ):
            self.assertNotIn(forbidden, encoded)

    def test_query_quality_or_commerce_failure_becomes_stop_required(self) -> None:
        cases = (
            ({"query_failed": True}, PRODUCT_HTML, True, True, "query_incomplete"),
            (athena_payload(duplicate_rows=1), PRODUCT_HTML, True, True, "duplicate_or_conflicting_assignment"),
            (athena_payload(), "", False, True, "reproducible_cart_or_checkout_runtime_error"),
            (athena_payload(), PRODUCT_HTML.replace("25,90 €", "26,90 €"), True, True, "price_changed"),
        )
        for payload, product_html, product_ok, cart_ok, reason in cases:
            with self.subTest(reason=reason):
                _, decision, _ = builder.build_bundle(
                    self.manifest,
                    payload,
                    checkpoint_index=1,
                    assignment_started_at_utc="2026-09-04T07:00:00Z",
                    observed_at_utc="2026-09-05T07:05:00Z",
                    product_html=product_html,
                    cart_html="<html><body>Košík</body></html>",
                    product_fetch_succeeded=product_ok,
                    cart_fetch_succeeded=cart_ok,
                    repository="vzeman/biznisweb",
                    workflow_run_id="60000000001",
                    main_commit="a" * 40,
                )
                self.assertEqual("STOP_REQUIRED", decision["verdict"])
                self.assertIn(reason, decision["stop_reasons"])

        _, decision, _ = builder.build_bundle(
            self.manifest,
            athena_payload(),
            checkpoint_index=1,
            assignment_started_at_utc="2026-09-04T07:00:00Z",
            observed_at_utc="2026-09-05T07:05:00Z",
            product_html=PRODUCT_HTML,
            cart_html="<html><body>unexpected page</body></html>",
            product_fetch_succeeded=True,
            cart_fetch_succeeded=True,
            repository="vzeman/biznisweb",
            workflow_run_id="60000000001",
            main_commit="a" * 40,
        )
        self.assertEqual("STOP_REQUIRED", decision["verdict"])
        self.assertIn(
            "reproducible_cart_or_checkout_runtime_error",
            decision["stop_reasons"],
        )

    def test_prepare_cli_skips_before_aws_and_assemble_writes_exact_three_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            github_env = root / "github.env"
            github_env.write_text("", encoding="utf-8")
            waiting = root / "waiting.json"
            waiting.write_text(
                json.dumps(load("projects/vevo/growthbook_cta_safety_monitoring.json")),
                encoding="utf-8",
            )
            self.assertEqual(
                0,
                builder.main(
                    [
                        "prepare",
                        "--manifest",
                        str(waiting),
                        "--event-name",
                        "schedule",
                        "--github-env",
                        str(github_env),
                        "--runner-temp",
                        str(root),
                        "--run-id",
                        "60000000001",
                        "--now-utc",
                        "2026-09-05T07:05:00Z",
                    ]
                ),
            )
            self.assertEqual("RUN_CHECKPOINT=false\n", github_env.read_text(encoding="utf-8"))

            manifest = root / "manifest.json"
            manifest.write_text(json.dumps(self.manifest), encoding="utf-8")
            athena = root / "athena.json"
            athena.write_text(json.dumps(athena_payload()), encoding="utf-8")
            product = root / "product.html"
            product.write_text(PRODUCT_HTML, encoding="utf-8")
            cart = root / "cart.html"
            cart.write_text("<html><body>Košík</body></html>", encoding="utf-8")
            output = root / "artifact"
            self.assertEqual(
                0,
                builder.main(
                    [
                        "assemble",
                        "--manifest",
                        str(manifest),
                        "--athena-result",
                        str(athena),
                        "--product-html",
                        str(product),
                        "--cart-html",
                        str(cart),
                        "--product-fetch-succeeded",
                        "true",
                        "--cart-fetch-succeeded",
                        "true",
                        "--checkpoint-index",
                        "1",
                        "--assignment-started-at-utc",
                        "2026-09-04T07:00:00Z",
                        "--observed-at-utc",
                        "2026-09-05T07:05:00Z",
                        "--repository",
                        "vzeman/biznisweb",
                        "--workflow-run-id",
                        "60000000001",
                        "--main-commit",
                        "a" * 40,
                        "--output-directory",
                        str(output),
                    ]
                ),
            )
            self.assertEqual(
                {builder.EVIDENCE_FILE, builder.DECISION_FILE, builder.PROVENANCE_FILE},
                {path.name for path in output.iterdir()},
            )
            for path in output.iterdir():
                payload = json.loads(path.read_text(encoding="utf-8"))
                self.assertEqual(path.read_bytes(), canonical_json_bytes(payload))


if __name__ == "__main__":
    unittest.main()
