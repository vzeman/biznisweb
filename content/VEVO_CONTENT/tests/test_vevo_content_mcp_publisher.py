import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "imports"
    / "publish_vevo_batch_via_content_mcp.py"
)
SPEC = importlib.util.spec_from_file_location("publish_vevo_batch_via_content_mcp", MODULE_PATH)
PUBLISHER = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(PUBLISHER)


def valid_article():
    return {
        "title": "Ako bezpečne overiť VEVO MCP",
        "short": "<p>Krátka odpoveď s dostatočným obsahom.</p>",
        "long": (
            "<p>Praktický úvod s dostatočným obsahom.</p>"
            "<h2>Postup</h2>"
            '<table style="width: 100%"><tbody><tr><td>Kontrola</td></tr></tbody></table>'
            '<p><a href="/p-1/test-produkt">Produkt</a></p>'
            '<p><a href="/c/test-kategoria">Kategória</a></p>'
        ),
        "link": "ako-bezpecne-overit-vevo-mcp",
        "title_tag": "Ako bezpečne overiť VEVO MCP",
        "description": "Praktický test bezpečného publikovania VEVO článku cez slug-aware MCP.",
        "commenting": False,
    }


class ArticleValidationTests(unittest.TestCase):
    def test_rich_article_passes(self):
        PUBLISHER.validate_article(valid_article())

    def test_repeated_one_slug_is_rejected(self):
        article = valid_article()
        article["link"] = "111111"

        with self.assertRaises(ValueError):
            PUBLISHER.validate_article(article)

    def test_one_character_paragraph_damage_is_rejected(self):
        article = valid_article()
        article["long"] += "<p>s</p><p>a</p><p>v</p>"

        with self.assertRaisesRegex(ValueError, "One-character"):
            PUBLISHER.validate_article(article)


class ToolPayloadTests(unittest.TestCase):
    def test_hidden_create_has_no_publish_confirmation(self):
        payload = PUBLISHER.article_tool_args(valid_article(), "765", visible=False)

        self.assertFalse(payload["visible"])
        self.assertNotIn("confirm_visible", payload)
        self.assertEqual(payload["commenting"], "none")

    def test_public_update_requires_confirmation(self):
        payload = PUBLISHER.article_tool_args(valid_article(), "765", visible=True)

        self.assertTrue(payload["visible"])
        self.assertTrue(payload["confirm_visible"])


class CatalogTests(unittest.TestCase):
    def test_exact_match_uses_casefolded_title_or_slug(self):
        rows = [
            {"news_id": "10", "title": "Čistý domov", "link": "iny-slug"},
            {"news_id": "11", "title": "Iný článok", "link": "presny-slug"},
        ]

        by_title = PUBLISHER.exact_catalog_matches(rows, "čistý DOMOV", "novy-slug")
        by_slug = PUBLISHER.exact_catalog_matches(rows, "Nový článok", "presny-slug")

        self.assertEqual([row["news_id"] for row in by_title], ["10"])
        self.assertEqual([row["news_id"] for row in by_slug], ["11"])


class PublicationReportTests(unittest.TestCase):
    def test_read_only_preflight_preserves_completed_status(self):
        self.assertEqual(PUBLISHER.status_after_preflight({"all_ok": True}), "complete")

    def test_read_only_preflight_marks_new_report_as_preflight_only(self):
        self.assertEqual(
            PUBLISHER.status_after_preflight({"all_ok": False}),
            "preflight_passed",
        )


if __name__ == "__main__":
    unittest.main()
