import importlib.util
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "tools"
    / "biznisweb_vevo_content_mcp.py"
)
SPEC = importlib.util.spec_from_file_location("biznisweb_vevo_content_mcp", MODULE_PATH)
MCP = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MCP)


class AdminResponseParserTests(unittest.TestCase):
    def test_parser_preserves_tokens_inside_strings(self):
        parsed = MCP.parse_admin_object(
            "{rows:[{contents:'https:\\/\\/www.vevo.sk/n/example true false null',active:true}],total:1}"
        )

        self.assertEqual(parsed["total"], 1)
        self.assertTrue(parsed["rows"][0]["active"])
        self.assertEqual(
            parsed["rows"][0]["contents"],
            "https://www.vevo.sk/n/example true false null",
        )


class EnvironmentTests(unittest.TestCase):
    def test_content_env_can_run_admin_without_graphql_token(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_file = Path(temp_dir) / ".env"
            env_file.write_text(
                "BIZNISWEB_ADMIN_BASE_URL=https://vevo.flox.sk\n"
                "BIZNISWEB_USERNAME=test-user\n"
                "BIZNISWEB_PASSWORD=test-password\n",
                encoding="utf-8",
            )
            with mock.patch.dict(
                os.environ,
                {"VEVO_CONTENT_ENV_FILE": str(env_file)},
                clear=True,
            ):
                values = MCP.ensure_vevo_env()

        self.assertEqual(values["admin_base_url"], "https://vevo.flox.sk")
        self.assertEqual(values["api_url"], "")
        self.assertEqual(values["token"], "")
        self.assertEqual(values["username_present"], "true")

    def test_explicit_missing_env_file_fails_without_fallback(self):
        with mock.patch.dict(
            os.environ,
            {"VEVO_CONTENT_ENV_FILE": "missing-vevo-content.env"},
            clear=True,
        ):
            with self.assertRaises(FileNotFoundError):
                MCP.resolve_env_file()


class SlugGuardTests(unittest.TestCase):
    def test_clean_slug_is_accepted(self):
        self.assertEqual(MCP.validate_slug("clean-vevo-slug-2026"), "clean-vevo-slug-2026")

    def test_invalid_or_placeholder_slugs_are_rejected(self):
        for slug in ("", "VEVO-Slug", "vevo_slug", "vôňa", "111111111111"):
            with self.subTest(slug=slug), self.assertRaises(ValueError):
                MCP.validate_slug(slug)


class NewsPayloadGuardTests(unittest.TestCase):
    def setUp(self):
        self.minimum = {
            "block_id": "765",
            "title": "API slug guard test",
            "short": "<p>Short content with enough text for an admin readback.</p>",
            "long": "<p>Long content with enough text for an admin readback.</p>",
            "link": "api-slug-guard-test",
        }

    def test_new_payload_defaults_to_hidden(self):
        payload = MCP.news_payload(self.minimum)

        self.assertEqual(payload["active"], "0")
        self.assertEqual(payload["link"], "api-slug-guard-test")

    def test_payload_normalizes_utf16_surrogate_pairs_from_legacy_admin(self):
        payload = MCP.news_payload(
            {
                **self.minimum,
                "long": "<p>Legacy light \ud83d\udca1 in admin content.</p>",
            }
        )

        self.assertIn("\U0001f4a1", payload["long"])
        self.assertNotIn("\ud83d", payload["long"])

    def test_visible_payload_requires_explicit_confirmation(self):
        with self.assertRaisesRegex(ValueError, "confirm_visible"):
            MCP.news_payload({**self.minimum, "visible": True})

        payload = MCP.news_payload(
            {**self.minimum, "visible": True, "confirm_visible": True}
        )
        self.assertEqual(payload["active"], "1")

    def test_readback_rejects_truncated_html(self):
        payload = MCP.news_payload(self.minimum)
        post = {
            "title": payload["title"],
            "link": payload["link"],
            "short": payload["short"],
            "long": "too short",
        }

        with self.assertRaisesRegex(RuntimeError, "admin readback"):
            MCP.assert_post_readback(post, payload)

    def test_readback_treats_utf16_surrogate_pair_as_same_unicode_character(self):
        payload = MCP.news_payload(
            {
                **self.minimum,
                "title": "\U0001f338 Čo je parfum do prania?",
                "long": "<p>Normal light \U0001f4a1 in content with enough text.</p>",
            }
        )
        post = {
            **payload,
            "title": "\ud83c\udf38 Čo je parfum do prania?",
            "long": "<p>Normal light \ud83d\udca1 in content with enough text.</p>",
        }

        MCP.assert_post_readback(post, payload)

    def test_legacy_repeated_one_slug_can_only_be_retained_while_hiding(self):
        existing = {
            "news_id": "1520",
            "block_id": "774",
            "active": "1",
            "title": "Legacy duplicate",
            "short": "Short content with enough text for readback.",
            "long": "<p>Long content with enough text for readback.</p>",
            "link": "111111111111111111",
        }

        hidden = MCP.news_payload({"active": False}, existing=existing)
        self.assertEqual(hidden["active"], "0")
        self.assertEqual(hidden["link"], "111111111111111111")

        with self.assertRaisesRegex(ValueError, "repeated-1"):
            MCP.news_payload(
                {"active": True, "confirm_visible": True}, existing=existing
            )

        with self.assertRaisesRegex(ValueError, "repeated-1"):
            MCP.news_payload(
                {"active": False, "link": "111111111111111111"},
                existing=existing,
            )


class ToolSchemaTests(unittest.TestCase):
    def test_create_requires_explicit_slug_and_rich_html_fields(self):
        required = set(
            MCP.TOOLS["vevo_add_news_post"]["inputSchema"]["required"]
        )

        self.assertEqual(required, {"block_id", "title", "short", "long", "link"})
        self.assertIn("confirm_visible", MCP.NEWS_FIELDS_SCHEMA)

    def test_list_schema_supports_compact_full_catalog_scan(self):
        properties = MCP.TOOLS["vevo_list_news_posts"]["inputSchema"]["properties"]

        self.assertIn("summary_only", properties)
        self.assertGreaterEqual(MCP.DUPLICATE_SCAN_LIMIT, 1000)

    def test_duplicate_matching_normalizes_title_case(self):
        rows = [{"news_id": "1", "title": "Čistý Domov", "link": "iny-slug"}]

        matches = MCP.unique_post_matches(
            rows,
            {"title": "čistý domov", "link": "novy-slug"},
        )

        self.assertEqual(matches, rows)


class ToolMutationGuardTests(unittest.TestCase):
    def test_active_update_ignores_hidden_title_clone_but_blocks_slug_collision(self):
        payload = {
            "active": "1",
            "title": "Canonical title",
            "link": "canonical-clean-slug",
        }
        current = {
            "news_id": "10",
            "active": "1",
            "title": "Canonical title",
            "link": "canonical-clean-slug",
        }
        hidden_title_clone = {
            "news_id": "11",
            "active": "0",
            "title": "Canonical title",
            "link": "111111111111",
        }
        hidden_slug_collision = {
            "news_id": "12",
            "active": "0",
            "title": "Different title",
            "link": "canonical-clean-slug",
        }
        active_title_clone = {
            "news_id": "13",
            "active": "1",
            "title": "Canonical title",
            "link": "another-clean-slug",
        }

        conflicts = MCP.blocking_update_matches(
            [
                current,
                hidden_title_clone,
                hidden_slug_collision,
                active_title_clone,
            ],
            payload,
            "10",
        )

        self.assertEqual(
            [row["news_id"] for row in conflicts],
            ["12", "13"],
        )
        self.assertEqual(
            MCP.blocking_update_matches(
                [hidden_slug_collision],
                {**payload, "active": "0"},
                "10",
            ),
            [],
        )

    def test_hidden_create_still_rejects_an_existing_duplicate(self):
        payload = {
            "block_id": "765",
            "active": "0",
            "title": "Existing title",
            "short": "Short content",
            "long": "<p>Long content</p>",
            "link": "new-clean-slug",
        }
        duplicate = {
            "news_id": "42",
            "title": "Existing title",
            "link": "existing-clean-slug",
        }
        with (
            mock.patch.object(MCP, "news_payload", return_value=payload),
            mock.patch.object(
                MCP,
                "public_status_for_slug",
                return_value={"status_code": 404},
            ),
            mock.patch.object(MCP, "admin_list_news_posts", return_value=[duplicate]),
            mock.patch.object(MCP, "admin_post") as admin_post,
        ):
            with self.assertRaisesRegex(RuntimeError, "Duplicate candidate"):
                MCP.tool_add_news_post({})

        admin_post.assert_not_called()

    def test_existing_legacy_duplicate_can_be_hidden(self):
        existing = {
            "news_id": "1520",
            "block_id": "774",
            "active": "1",
            "title": "Legacy duplicate",
            "short": "Short content with enough text for readback.",
            "long": "<p>Long content with enough text for readback.</p>",
            "link": "111111111111111111",
        }
        hidden = {**existing, "active": "0"}
        canonical = {
            "news_id": "1682",
            "block_id": "774",
            "active": "1",
            "title": "Legacy duplicate",
            "link": "canonical-clean-slug",
        }
        with (
            mock.patch.object(
                MCP,
                "admin_get_news_post",
                side_effect=[existing, hidden],
            ),
            mock.patch.object(
                MCP,
                "admin_list_news_posts",
                return_value=[existing, canonical],
            ),
            mock.patch.object(MCP, "admin_post", return_value={"success": True}),
            mock.patch.object(
                MCP,
                "wait_for_public_status",
                return_value={"status_code": 404},
            ),
        ):
            result = MCP.tool_update_news_post(
                {"post_id": "1520", "active": False}
            )

        self.assertEqual(result["post_id"], "1520")
        self.assertEqual(result["news_post"]["active"], "0")


if __name__ == "__main__":
    unittest.main()
