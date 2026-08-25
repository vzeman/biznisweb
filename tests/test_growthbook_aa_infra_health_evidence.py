from __future__ import annotations

import hashlib
import json
import pathlib
import tempfile
import unittest

from scripts.record_growthbook_natural_evidence import canonical_evidence_bytes
from scripts.validate_growthbook_aa_infra_health_evidence import (
    InfraHealthEvidenceError,
    main,
    validate_health_evidence,
)


ROOT = pathlib.Path(__file__).resolve().parents[1]
DEPLOY_PATH = (
    ROOT
    / "projects"
    / "vevo"
    / "growthbook_production_reconciliation_deploy_evidence.json"
)
DEPLOY_BYTES = DEPLOY_PATH.read_bytes()
DEPLOY = json.loads(DEPLOY_BYTES)


def health_evidence(*, post_run: bool) -> dict:
    task_definition = DEPLOY["reconciliation"]["task_definition"].rsplit("/", 1)[-1]
    source_task_definition = DEPLOY["source_runtime"]["task_definition"].rsplit("/", 1)[-1]
    return {
        "schema_version": 1,
        "evidence_type": "vevo_growthbook_production_aa_infra_health",
        "environment": "production",
        "observed_at_utc": "2026-08-26T02:15:00Z",
        "provenance": {
            "workflow": ".github/workflows/monitor-vevo-growthbook-production-aa-infra.yml",
            "workflow_run_id": "32850000000",
            "main_commit": "a" * 40,
            "deploy_evidence_sha256": hashlib.sha256(DEPLOY_BYTES).hexdigest(),
        },
        "aws": DEPLOY["aws"],
        "phase": {
            "status": (
                "natural_reconciliation_verified"
                if post_run
                else "waiting_for_first_natural_run"
            ),
            "first_natural_run_due_local": "2026-08-26T03:45:00+02:00",
            "checked_due_local": "2026-08-26T03:45:00+02:00" if post_run else None,
        },
        "runtime": {
            "instance_id": "N/A:Fargate",
            "task_id": "1" * 32 if post_run else None,
            "private_ip": "172.31.1.25" if post_run else None,
            "service": "vevo-growthbook-reconcile-production",
            "runtime_path": "/app",
            "task_definition": task_definition,
            "image_digest": DEPLOY["reconciliation"]["image_digest"],
            "localhost_marker_source": "hash_bound_deploy_evidence_host_gate",
        },
        "control": {
            "schedule_name": "vevo-growthbook-reconcile-production",
            "schedule_state": "ENABLED",
            "schedule_expression": "cron(45 3 * * ? *)",
            "schedule_timezone": "Europe/Bratislava",
            "schedule_succeeded": post_run,
            "success_marker_sha256": "b" * 64 if post_run else None,
            "publish_summary_sha256": "c" * 64 if post_run else None,
            "generated_published_parity_verified": True if post_run else None,
            "alarm_states": {
                "vevo-growthbook-reconcile-production-dlq": "OK",
                "vevo-growthbook-reconcile-production-failure": "OK",
                "vevo-growthbook-reconcile-production-missing-success": "OK",
            },
            "alarms_clear": True,
            "dlq_empty": True,
            "source_schedule_name": "vevo-daily-report-email",
            "source_schedule_state": "ENABLED",
            "source_task_definition": source_task_definition,
            "source_schedule_unchanged": True,
        },
        "privacy": {
            "experimental_population_read": False,
            "arm_assignment_read": False,
            "outcome_metrics_read": False,
            "meta_dimensions_read": False,
            "performance_values_read": False,
            "reporting_row_counts_emitted": False,
            "contains_raw_aws_payloads": False,
            "contains_cloudwatch_messages": False,
            "contains_credentials": False,
            "contains_event_device_customer_or_order_ids": False,
        },
        "boundaries": {
            "aws_resource_mutated": False,
            "collector_or_reporting_mutated": False,
            "growthbook_mutated": False,
            "gtm_mutated": False,
            "meta_ads_mutated": False,
            "biznisweb_mutated": False,
            "price_product_stock_cart_checkout_payment_or_order_mutated": False,
            "workflow_or_experiment_gate_changed": False,
        },
    }


class GrowthBookAaInfraHealthEvidenceTests(unittest.TestCase):
    def test_accepts_pre_first_run_structure_only_evidence(self) -> None:
        value = health_evidence(post_run=False)
        value["control"]["alarm_states"][
            "vevo-growthbook-reconcile-production-missing-success"
        ] = "INSUFFICIENT_DATA"
        validate_health_evidence(value, DEPLOY, deploy_evidence_bytes=DEPLOY_BYTES)

    def test_accepts_natural_run_health_without_experiment_results(self) -> None:
        value = health_evidence(post_run=True)
        validate_health_evidence(value, DEPLOY, deploy_evidence_bytes=DEPLOY_BYTES)
        serialized = json.dumps(value, sort_keys=True).lower()
        for forbidden in (
            '"eligible_devices"',
            '"variation_id"',
            '"conversion"',
            '"revenue"',
            '"cm1_eur"',
            '"raw_events"',
            '"device_facts"',
            '"performance_facts"',
            '"quality_reports"',
        ):
            self.assertNotIn(forbidden, serialized)

    def test_rejects_result_or_identity_field_injection(self) -> None:
        value = health_evidence(post_run=True)
        value["eligible_devices"] = 1000
        with self.assertRaisesRegex(InfraHealthEvidenceError, "evidence keys drift"):
            validate_health_evidence(value, DEPLOY, deploy_evidence_bytes=DEPLOY_BYTES)

    def test_rejects_runtime_alarm_or_mutation_drift(self) -> None:
        mutations = (
            ("runtime", "image_digest", "sha256:" + "0" * 64),
            ("control", "dlq_empty", False),
            ("boundaries", "growthbook_mutated", True),
        )
        for section, key, replacement in mutations:
            with self.subTest(section=section, key=key):
                value = health_evidence(post_run=True)
                value[section][key] = replacement
                with self.assertRaises(InfraHealthEvidenceError):
                    validate_health_evidence(
                        value,
                        DEPLOY,
                        deploy_evidence_bytes=DEPLOY_BYTES,
                    )

    def test_cli_requires_canonical_bytes(self) -> None:
        value = health_evidence(post_run=False)
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "health.json"
            path.write_bytes(canonical_evidence_bytes(value))
            self.assertEqual(
                0,
                main(
                    [
                        "--evidence",
                        str(path),
                        "--deploy-evidence",
                        str(DEPLOY_PATH),
                    ]
                ),
            )
            path.write_text(json.dumps(value), encoding="utf-8")
            self.assertEqual(
                1,
                main(
                    [
                        "--evidence",
                        str(path),
                        "--deploy-evidence",
                        str(DEPLOY_PATH),
                    ]
                ),
            )


if __name__ == "__main__":
    unittest.main()
