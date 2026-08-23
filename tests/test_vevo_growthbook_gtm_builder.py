import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from scripts.build_vevo_growthbook_gtm_tag import build_tag, validate_config


class VevoGrowthBookGtmBuilderTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.config = {
            "schemaVersion": 1,
            "environment": "preview",
            "clientKey": "sdk-abcdefgh12345678",
            "apiHost": "https://cdn.growthbook.io",
            "collectorUrl": "https://abc123.execute-api.eu-central-1.amazonaws.com/v1/events",
            "allowedHost": "www.vevo.sk",
            "gtmContainerId": "GTM-5ZB5LFGB",
            "enableDevMode": True,
        }

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_builds_reproducible_preview_only_custom_html(self):
        config_path = self.root / "preview.json"
        output_path = self.root / "tag.html"
        config_path.write_text(json.dumps(self.config), encoding="utf-8")

        digest = build_tag(config_path, output_path)
        artifact = output_path.read_text(encoding="utf-8")

        self.assertEqual(hashlib.sha256(artifact.encode("utf-8")).hexdigest(), digest)
        self.assertIn("window.VEVO_GROWTHBOOK_CONFIG=", artifact)
        self.assertIn("var PRODUCTION_ACTIVATION = false;", artifact)
        self.assertIn("@growthbook/growthbook@1.7.0/dist/bundles/index.min.js", artifact)
        self.assertIn("web-vitals@6.0.1/dist/web-vitals.iife.js", artifact)
        self.assertIn('credentials: "omit"', artifact)
        self.assertNotIn("auto.min.js", artifact)

    def test_builds_production_only_with_reviewed_collector_host(self):
        production = dict(self.config)
        production.update(
            {
                "environment": "production",
                "clientKey": "sdk-production12345678",
                "collectorUrl": (
                    "https://prod123.execute-api.eu-central-1.amazonaws.com/v1/events"
                ),
                "enableDevMode": False,
            }
        )
        config_path = self.root / "production.json"
        output_path = self.root / "production-tag.html"
        config_path.write_text(json.dumps(production), encoding="utf-8")
        expected_host_digest = hashlib.sha256(
            b"prod123.execute-api.eu-central-1.amazonaws.com"
        ).hexdigest()

        digest = build_tag(
            config_path,
            output_path,
            expected_production_collector_host_sha256=expected_host_digest,
        )
        artifact = output_path.read_text(encoding="utf-8")

        self.assertEqual(hashlib.sha256(artifact.encode("utf-8")).hexdigest(), digest)
        self.assertIn('"environment":"production"', artifact)
        self.assertIn('"enableDevMode":false', artifact)
        self.assertIn("var PRODUCTION_ACTIVATION = true;", artifact)
        self.assertNotIn("var PRODUCTION_ACTIVATION = false;", artifact)

        with self.assertRaisesRegex(ValueError, "reviewed evidence"):
            build_tag(
                config_path,
                output_path,
                expected_production_collector_host_sha256="0" * 64,
            )

    def test_rejects_unknown_environment_or_non_allowlisted_configuration(self):
        invalid_overrides = [
            {"environment": "unknown"},
            {"collectorUrl": "https://example.com/v1/events"},
            {"collectorUrl": "https://abc123.execute-api.eu-west-1.amazonaws.com/v1/events"},
            {"collectorUrl": "https://abc123.execute-api.eu-central-1.amazonaws.com:444/v1/events"},
            {"collectorUrl": "https://abc123.execute-api.eu-central-1.amazonaws.com/v1/events?email=x"},
            {"allowedHost": "vevo.sk"},
            {"gtmContainerId": "GTM-OTHER"},
            {"enableDevMode": "true"},
            {"enableDevMode": False},
            {"clientKey": "sdk-REPLACE_WITH_PREVIEW_CLIENT_KEY"},
        ]
        for overrides in invalid_overrides:
            with self.subTest(overrides=overrides):
                payload = dict(self.config)
                payload.update(overrides)
                with self.assertRaises(ValueError):
                    validate_config(payload)

        production_dev_mode = dict(self.config)
        production_dev_mode.update(
            {
                "environment": "production",
                "enableDevMode": True,
            }
        )
        with self.assertRaises(ValueError):
            validate_config(production_dev_mode)

        extra = dict(self.config)
        extra["unexpected"] = "field"
        with self.assertRaises(ValueError):
            validate_config(extra)

    def test_accepts_client_key_from_environment_without_local_config_copy(self):
        config_path = self.root / "preview.example.json"
        output_path = self.root / "tag.html"
        config = dict(self.config)
        config["clientKey"] = "sdk-REPLACE_WITH_PREVIEW_CLIENT_KEY"
        config_path.write_text(json.dumps(config), encoding="utf-8")

        digest = build_tag(
            config_path,
            output_path,
            client_key_override="sdk-secureenvironment123",
            collector_url_override=(
                "https://preview123.execute-api.eu-central-1.amazonaws.com/v1/events"
            ),
        )
        artifact = output_path.read_text(encoding="utf-8")

        self.assertEqual(hashlib.sha256(artifact.encode("utf-8")).hexdigest(), digest)
        self.assertIn('"clientKey":"sdk-secureenvironment123"', artifact)
        self.assertIn(
            '"collectorUrl":"https://preview123.execute-api.eu-central-1.amazonaws.com/v1/events"',
            artifact,
        )
        self.assertNotIn("REPLACE_WITH_PREVIEW_CLIENT_KEY", artifact)

    def test_rejects_invalid_environment_client_key_override(self):
        config_path = self.root / "preview.example.json"
        output_path = self.root / "tag.html"
        config = dict(self.config)
        config["clientKey"] = "sdk-REPLACE_WITH_PREVIEW_CLIENT_KEY"
        config_path.write_text(json.dumps(config), encoding="utf-8")

        with self.assertRaises(ValueError):
            build_tag(
                config_path,
                output_path,
                client_key_override="not-a-client-key",
            )

        with self.assertRaises(ValueError):
            build_tag(
                config_path,
                output_path,
                client_key_override="sdk-secureenvironment123",
                collector_url_override="https://example.com/v1/events",
            )


if __name__ == "__main__":
    unittest.main()
