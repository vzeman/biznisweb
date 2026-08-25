from __future__ import annotations

import copy
import json
import unittest
from unittest import mock

from scripts import vevo_growthbook_cli as cli


def _valid_experiment() -> dict[str, object]:
    return {
        "id": cli.EXPERIMENT_ID,
        "trackingKey": cli.TRACKING_KEY,
        "status": "draft",
        "type": "standard",
        "hashAttribute": "id",
        "disableStickyBucketing": False,
        "variations": [
            {"variationId": "v0", "key": "control", "name": "control"},
            {"variationId": "v1", "key": "variant", "name": "variant"},
        ],
        "phases": [
            {
                "coverage": 1.0,
                "trafficSplit": [
                    {"variationId": "v0", "weight": 0.5},
                    {"variationId": "v1", "weight": 0.5},
                ],
            }
        ],
    }


def _valid_feature() -> dict[str, object]:
    return {
        "id": cli.FEATURE_KEY,
        "revision": {"version": cli.FEATURE_LIVE_REVISION},
        "environments": {
            "production": {"enabled": False},
            "staging": {"enabled": True},
        },
        "rules": [
            {
                "id": "preview-rule",
                "type": "experiment-ref",
                "experimentId": "exp_preview",
                "enabled": True,
                "environments": ["staging"],
            }
        ],
    }


def _valid_revision() -> dict[str, object]:
    return {
        "featureId": cli.FEATURE_KEY,
        "baseVersion": cli.FEATURE_LIVE_REVISION,
        "version": cli.FEATURE_DRAFT_REVISION,
        "status": "draft",
        "environmentsEnabled": {"production": True, "staging": True},
        "rules": [
            {
                "id": "preview-rule",
                "type": "experiment-ref",
                "experimentId": "exp_preview",
                "enabled": True,
                "environments": ["staging"],
            },
            {
                "id": "production-rule",
                "type": "experiment-ref",
                "experimentId": cli.EXPERIMENT_ID,
                "enabled": True,
                "environments": ["production"],
            },
        ],
    }


class VevoGrowthBookCliTests(unittest.TestCase):
    def test_checked_in_gate_is_exact(self) -> None:
        activation, digest = cli._validate_local_gate()
        self.assertEqual(9, activation["schema_version"])
        self.assertEqual(64, len(digest))

    def test_remote_preflight_accepts_only_reviewed_state(self) -> None:
        result = cli._validate_remote(
            _valid_experiment(),
            _valid_feature(),
            _valid_revision(),
            {"rebaseRequired": False, "conflicts": []},
        )
        self.assertEqual("draft", result["experiment_status"])
        self.assertEqual(100, result["coverage_percent"])
        self.assertEqual(1, result["feature_draft_production_rule_count"])
        self.assertFalse(result["rebase_required"])

    def test_remote_preflight_rejects_running_experiment(self) -> None:
        experiment = _valid_experiment()
        experiment["status"] = "running"
        with self.assertRaisesRegex(cli.GateError, "not draft"):
            cli._validate_remote(
                experiment,
                _valid_feature(),
                _valid_revision(),
                {"rebaseRequired": False, "conflicts": []},
            )

    def test_remote_preflight_rejects_non_5050_split(self) -> None:
        experiment = _valid_experiment()
        phases = experiment["phases"]
        self.assertIsInstance(phases, list)
        phases[0]["trafficSplit"][0]["weight"] = 0.6
        with self.assertRaisesRegex(cli.GateError, "weights drifted"):
            cli._validate_remote(
                experiment,
                _valid_feature(),
                _valid_revision(),
                {"rebaseRequired": False, "conflicts": []},
            )

    def test_remote_preflight_rejects_live_production_rule(self) -> None:
        feature = _valid_feature()
        feature["rules"].append(
            {
                "id": "unexpected-live-production-rule",
                "type": "force",
                "enabled": True,
                "environments": ["production"],
            }
        )
        with self.assertRaisesRegex(cli.GateError, "already has a Production rule"):
            cli._validate_remote(
                _valid_experiment(),
                feature,
                _valid_revision(),
                {"rebaseRequired": False, "conflicts": []},
            )

    def test_remote_preflight_rejects_second_production_experiment_rule(self) -> None:
        revision = _valid_revision()
        duplicate = copy.deepcopy(revision["rules"][-1])
        duplicate["id"] = "duplicate-production-rule"
        revision["rules"].append(duplicate)
        with self.assertRaisesRegex(cli.GateError, "exactly one Production rule"):
            cli._validate_remote(
                _valid_experiment(),
                _valid_feature(),
                revision,
                {"rebaseRequired": False, "conflicts": []},
            )

    def test_remote_preflight_rejects_rebase_or_conflict(self) -> None:
        for merge in (
            {"rebaseRequired": True, "conflicts": []},
            {"rebaseRequired": False, "conflicts": {"rules.order": {}}},
        ):
            with self.subTest(merge=merge):
                with self.assertRaises(cli.GateError):
                    cli._validate_remote(
                        _valid_experiment(),
                        _valid_feature(),
                        _valid_revision(),
                        merge,
                    )

    def test_plan_is_exact_and_never_mutates(self) -> None:
        start = cli.subprocess.CompletedProcess(
            args=[], returncode=0, stdout="", stderr=f"Would send: {cli.START_ENDPOINT}\n"
        )
        publish = cli.subprocess.CompletedProcess(
            args=[], returncode=0, stdout="", stderr=f"Would send: {cli.PUBLISH_ENDPOINT}\n"
        )
        with mock.patch.object(cli, "_run_cli", side_effect=[start, publish]):
            result = cli.plan()
        self.assertEqual("mutation_dry_run_passed", result["status"])
        self.assertFalse(result["mutation_executed"])
        self.assertEqual(
            [cli.START_ENDPOINT, cli.PUBLISH_ENDPOINT],
            [item["endpoint"] for item in result["ordered_operations"]],
        )

    def test_secret_redaction_covers_environment_and_token_shape(self) -> None:
        with mock.patch.dict(cli.os.environ, {"GBCLI_BEARER_AUTH": "plain-secret-value"}):
            value = cli._redact("plain-secret-value secret_abcdefghijklmnop")
        self.assertNotIn("plain-secret-value", value)
        self.assertNotIn("secret_abcdefghijklmnop", value)
        self.assertEqual(2, value.count("<redacted>"))

    def test_preflight_output_contains_hashes_not_raw_remote_payloads(self) -> None:
        payloads = [
            _valid_experiment(),
            _valid_feature(),
            _valid_revision(),
            {"rebaseRequired": False, "conflicts": []},
            {"from": {"version": 2}, "to": {"version": 3}, "changes": {}},
        ]
        with mock.patch.object(cli, "_run_cli_json", side_effect=payloads):
            result = cli.preflight()
        encoded = json.dumps(result)
        self.assertEqual("authenticated_preflight_passed", result["status"])
        self.assertFalse(result["mutation_executed"])
        self.assertNotIn("preview-rule", encoded)
        self.assertEqual(5, len(result["remote_payload_sha256"]))


if __name__ == "__main__":
    unittest.main()
