import json
import sys
import tempfile
import unittest
from pathlib import Path


TOOLS_DIR = Path(__file__).resolve().parents[1] / "tools"
sys.path.insert(0, str(TOOLS_DIR))

import vevo_article_depth_guard as depth_guard
import vevo_cross_section_duplicate_audit as cross_section_audit
import vevo_duplicate_guard as duplicate_guard
import vevo_html_safety_guard as html_guard
import vevo_project_audit as project_audit
import vevo_rich_expansion_renderer as rich_renderer


def valid_article(**overrides):
    article = {
        "title": "Ako vyčistiť skúšobný povrch bezpečne",
        "short": "Stručná a praktická odpoveď pre domácnosť.",
        "link": "ako-vycistit-skusobny-povrch-bezpecne",
        "long": (
            '<p><strong>Rýchla odpoveď:</strong> Najprv povrch otestujte.</p>'
            '<div style="border:1px solid #ddd"><h2>Postup</h2><p>Prvý krok.</p></div>'
            '<div style="border:1px solid #ddd"><p>Druhý krok.</p></div>'
            '<div style="border:1px solid #ddd"><p>Tretí krok.</p></div>'
            '<div style="border:1px solid #ddd"><p>Štvrtý krok.</p></div>'
            '<p><a href="/p-1/test-produkt">Produkt</a></p>'
            '<p><a href="/c/test-kategoria">Kategória</a></p>'
        ),
    }
    article.update(overrides)
    return article


class RichExpansionRendererTests(unittest.TestCase):
    def test_renderer_normalizes_surrogates_and_emits_two_tables(self):
        config = {
            "title": "Testovacia téma",
            "quick": "Krátka odpoveď.",
            "intro": "Úvod.",
            "focus": "presný problém",
            "boundary": "iné návody",
            "points": ["bod jeden", "bod dva", "bod tri", "bod štyri"],
            "sections": [("Sekcia", ["Prvý odsek.", "Druhý odsek."])],
            "deep_dive": [
                ("Vlastná analýza", ["Jedinečný prvý odsek.", "Jedinečný druhý odsek."])
            ],
            "table": {
                "headers": ["A", "B"],
                "rows": [["a", "b"], ["c", "d"]],
            },
            "steps": ["Prvý krok.", "Druhý krok.", "Tretí krok."],
            "checks": [
                ["Prvá", "Prvá kontrola?"],
                ["Druhá", "Druhá kontrola?"],
                ["Tretia", "Tretia kontrola?"],
            ],
            "expert": ["Odborný odsek."],
            "sources": [["Zdroj", "https://example.com/source"]],
            "commerce": {
                "category_title": "Kategória",
                "category_body": "Opis kategórie.",
                "category_href": "/c/test",
                "product_title": "Produkt",
                "product_body": "Opis produktu.",
                "product_href": "/p-1/test",
            },
            "faq": [["Otázka?", "Odpoveď."]],
        }

        rendered = rich_renderer.render_expansion(config, "TEST-MARKER")

        self.assertIn("TEST-MARKER", rendered)
        self.assertIn("Vlastná analýza", rendered)
        self.assertNotIn("Ako si vytvoriť spoľahlivý domáci postup", rendered)
        self.assertEqual(rendered.count("<table"), 2)
        self.assertEqual(
            rich_renderer.normalize_admin_unicode("\ud83d\udca1"),
            "\U0001f4a1",
        )
        self.assertEqual(
            rich_renderer.sanitize_legacy("<p>s</p><p>Platný odsek.</p>"),
            "<p>Platný odsek.</p>",
        )


class DuplicateGuardTests(unittest.TestCase):
    def test_slovak_normalization_and_slug(self):
        value = "Čo je ľan a ako ho prať?"
        self.assertEqual(duplicate_guard.norm(value), "co je lan a ako ho prat")
        self.assertEqual(duplicate_guard.slugify(value), "co-je-lan-a-ako-ho-prat")

    def test_same_action_and_subject_is_detected(self):
        overlap = duplicate_guard.intent_overlap(
            "Ako vyčistiť radiátor od prachu",
            "Ako vyčistiť radiátor od mastnoty",
        )
        self.assertTrue(overlap["same_head"])
        self.assertIn("clean", overlap["same_actions"])
        self.assertIn("radiator", overlap["shared_anchors"])

    def test_reworded_encyclopedic_material_title_is_blocked(self):
        existing = [
            duplicate_guard.row_from_title_link(
                "Čo je ripstop: mriežkovaná tkanina, odolnosť a pranie outdoorového oblečenia",
                "https://www.vevo.sk/n/co-je-ripstop-mriezkovana-tkanina-odolnost-a-pranie-outdooroveho-oblecenia",
                "rss",
            )
        ]

        results = duplicate_guard.analyze(
            ["Čo je ripstop: spevnená mriežková tkanina, trhliny a starostlivosť"],
            existing,
            0.28,
        )

        self.assertEqual(results[0]["status"], "block")
        self.assertIn(
            "canonical_definition_head",
            {issue["type"] for issue in results[0]["issues"]},
        )

    def test_distinct_how_to_article_is_not_a_definition_head_duplicate(self):
        existing = [
            duplicate_guard.row_from_title_link(
                "Čo je ripstop: mriežkovaná tkanina, odolnosť a pranie outdoorového oblečenia",
                "https://www.vevo.sk/n/co-je-ripstop-mriezkovana-tkanina-odolnost-a-pranie-outdooroveho-oblecenia",
                "rss",
            )
        ]

        results = duplicate_guard.analyze(
            ["Ako opraviť malú trhlinu v ripstopovej bunde"],
            existing,
            0.28,
        )

        self.assertNotIn(
            "canonical_definition_head",
            {issue["type"] for issue in results[0]["issues"]},
        )

    def test_chenille_alias_is_blocked_by_zenilka_definition(self):
        existing = [
            duplicate_guard.row_from_title_link(
                "Čo je ženilka: vlasová priadza, uvoľňovanie chĺpkov a čistenie",
                "https://www.vevo.sk/n/co-je-zenilka-vlasova-priadza-uvolnovanie-chlpkov-a-cistenie",
                "rss",
            )
        ]

        results = duplicate_guard.analyze(
            ["Čo je chenille: mäkký vlas, čalúnenie a starostlivosť"],
            existing,
            0.28,
        )

        self.assertEqual(results[0]["status"], "block")
        self.assertIn(
            "canonical_definition_head",
            {issue["type"] for issue in results[0]["issues"]},
        )

    def test_hopsack_alias_is_blocked_by_panama_weave_definition(self):
        existing = [
            duplicate_guard.row_from_title_link(
                "Čo je panamová väzba: košíková tkanina, posun nití a pranie",
                "https://www.vevo.sk/n/co-je-panamova-vazba-kosikova-tkanina-posun-niti-a-pranie",
                "rss",
            )
        ]

        results = duplicate_guard.analyze(
            ["Čo je hopsack: otvorená väzba, zachytávanie a čistenie"],
            existing,
            0.28,
        )

        self.assertEqual(results[0]["status"], "block")
        self.assertIn(
            "canonical_definition_head",
            {issue["type"] for issue in results[0]["issues"]},
        )

    def test_oxford_cloth_alias_is_blocked_by_slovak_definition(self):
        existing = [
            duplicate_guard.row_from_title_link(
                "Čo je oxfordská tkanina: košeľová väzba, golier a správne pranie",
                "https://www.vevo.sk/n/co-je-oxfordska-tkanina-koselova-vazba-golier-a-spravne-pranie",
                "rss",
            )
        ]

        results = duplicate_guard.analyze(
            ["Čo je Oxford cloth: košeľovina, väzba a údržba"],
            existing,
            0.28,
        )

        self.assertEqual(results[0]["status"], "block")
        self.assertIn(
            "canonical_definition_head",
            {issue["type"] for issue in results[0]["issues"]},
        )

    def test_inlet_alias_is_blocked_by_sypkovina_definition(self):
        existing = [
            duplicate_guard.row_from_title_link(
                "Čo je sypkovina: hustá tkanina na perie, škvrny a pranie",
                "https://www.vevo.sk/n/co-je-sypkovina-husta-tkanina-na-perie-skvrny-a-pranie",
                "rss",
            )
        ]

        results = duplicate_guard.analyze(
            ["Čo je inlet: poťah na perie, únik páperia a čistenie"],
            existing,
            0.28,
        )

        self.assertEqual(results[0]["status"], "block")
        self.assertIn(
            "canonical_definition_head",
            {issue["type"] for issue in results[0]["issues"]},
        )

    def test_manila_hemp_alias_is_blocked_by_abaka_definition(self):
        existing = [
            duplicate_guard.row_from_title_link(
                "Čo je abaka: pevné listové vlákno, vlhkosť a starostlivosť",
                "https://www.vevo.sk/n/co-je-abaka-pevne-listove-vlakno-vlhkost-a-starostlivost",
                "rss",
            )
        ]

        results = duplicate_guard.analyze(
            ["Čo je Manila hemp: prírodné vlákno, košíky a údržba"],
            existing,
            0.28,
        )

        self.assertEqual(results[0]["status"], "block")
        self.assertIn(
            "canonical_definition_head",
            {issue["type"] for issue in results[0]["issues"]},
        )

    def test_mollskin_alias_is_blocked_by_moleskin_definition(self):
        existing = [
            duplicate_guard.row_from_title_link(
                "Čo je moleskin: hustá brúsená bavlna, lesk a správne pranie",
                "https://www.vevo.sk/n/co-je-moleskin-husta-brusena-bavlna-lesk-a-spravne-pranie",
                "rss",
            )
        ]

        results = duplicate_guard.analyze(
            ["Čo je mollskin: mäkký povrch, nohavice a čistenie"],
            existing,
            0.28,
        )

        self.assertEqual(results[0]["status"], "block")
        self.assertIn(
            "canonical_definition_head",
            {issue["type"] for issue in results[0]["issues"]},
        )

    def test_herringbone_alias_is_blocked_by_rybia_kost_definition(self):
        existing = [
            duplicate_guard.row_from_title_link(
                "Čo je rybia kosť: lámaný keper, smer vzoru a správne pranie",
                "https://www.vevo.sk/n/co-je-rybia-kost-lamany-keper-smer-vzoru-a-spravne-pranie",
                "rss",
            )
        ]

        results = duplicate_guard.analyze(
            ["Čo je herringbone weave: lomený vzor a údržba"],
            existing,
            0.28,
        )

        self.assertEqual(results[0]["status"], "block")
        self.assertIn(
            "canonical_definition_head",
            {issue["type"] for issue in results[0]["issues"]},
        )

    def test_voile_alias_is_blocked_by_voalova_zaclona_definition(self):
        existing = [
            duplicate_guard.row_from_title_link(
                "Čo je voálová záclona: priesvitná tkanina, prach a pranie bez pokrčenia",
                "https://www.vevo.sk/n/co-je-voalova-zaclona-priesvitna-tkanina-prach-a-pranie-bez-pokrčenia",
                "rss",
            )
        ]

        results = duplicate_guard.analyze(
            ["Čo je voile: jemná priesvitná látka a pranie"],
            existing,
            0.28,
        )

        self.assertEqual(results[0]["status"], "block")
        self.assertIn(
            "canonical_definition_head",
            {issue["type"] for issue in results[0]["issues"]},
        )

    def test_taffeta_alias_is_blocked_by_taft_definition(self):
        existing = [
            duplicate_guard.row_from_title_link(
                "Čo je taft: šušťavá tkanina, vodné mapy a bezpečné čistenie",
                "https://www.vevo.sk/n/co-je-taft-sustava-tkanina-vodne-mapy-a-bezpecne-cistenie",
                "rss",
            )
        ]

        results = duplicate_guard.analyze(
            ["Čo je taffeta: lesklá spoločenská látka a údržba"],
            existing,
            0.28,
        )

        self.assertEqual(results[0]["status"], "block")
        self.assertIn(
            "canonical_definition_head",
            {issue["type"] for issue in results[0]["issues"]},
        )

    def test_boiled_wool_and_felt_aliases_share_loden_definition(self):
        existing = [
            duplicate_guard.row_from_title_link(
                "Čo je loden a varená vlna: valchovanie, plsť a bezpečné čistenie",
                "https://www.vevo.sk/n/co-je-loden-a-varena-vlna-valchovanie-plst-a-bezpecne-cistenie",
                "rss",
            )
        ]

        for candidate in (
            "Čo je boiled wool: zrazená vlna a pranie",
            "Čo je plsť: valchované vlákna a starostlivosť",
        ):
            with self.subTest(candidate=candidate):
                results = duplicate_guard.analyze([candidate], existing, 0.28)
                self.assertEqual(results[0]["status"], "block")
                self.assertIn(
                    "canonical_definition_head",
                    {issue["type"] for issue in results[0]["issues"]},
                )

    def test_batch_54_trade_aliases_are_blocked_by_canonical_definitions(self):
        families = (
            (
                "Čo je buckram: vystužená tkanina, tvarovanie, prach a bezpečné čistenie",
                "co-je-buckram-vystuzena-tkanina-tvarovanie-prach-a-bezpecne-cistenie",
                "Čo je bougran: tuhá kníhviazačská tkanina a čistenie",
            ),
            (
                "Čo je madras: károvaná košeľovina, púšťanie farby a správne pranie",
                "co-je-madras-karovana-koselovina-pustanie-farby-a-spravne-pranie",
                "Čo je bleeding madras: farebné káro a údržba",
            ),
            (
                "Čo je šantán: nepravidelný povrch, hodvábne a zmesové varianty",
                "co-je-santan-nepravidelny-povrch-hodvabne-a-zmesove-varianty",
                "Čo je shantung silk: nopkovaný hodváb a čistenie",
            ),
            (
                "Čo je challis: mäkká splývavá tkanina, zrážanie a pranie",
                "co-je-challis-makka-splyvava-tkanina-zrazanie-a-pranie",
                "Čo je chally: ľahká šatová látka a pranie",
            ),
        )

        for title, slug, candidate in families:
            with self.subTest(candidate=candidate):
                existing = [
                    duplicate_guard.row_from_title_link(
                        title,
                        f"https://www.vevo.sk/n/{slug}",
                        "rss",
                    )
                ]
                results = duplicate_guard.analyze([candidate], existing, 0.28)
                self.assertEqual(results[0]["status"], "block")
                self.assertIn(
                    "canonical_definition_head",
                    {issue["type"] for issue in results[0]["issues"]},
                )

    def test_cross_section_title_tokens_remove_template_words(self):
        left = cross_section_audit.title_tokens(
            "Ako umyt okna bez smuh - Kompletny sprievodca"
        )
        right = cross_section_audit.title_tokens(
            "Ako umyt okna bez smuh - Casto kladene otazky"
        )
        self.assertEqual(left, {"okna", "smuh", "umyt"})
        self.assertEqual(left, right)

    def test_cross_section_report_separates_public_and_hidden_duplicates(self):
        section = {
            "role": "test",
            "title": "Test",
            "page_id": "1",
            "block_id": "2",
            "public_url": "https://example.test",
        }
        rows = [
            {"news_id": "1", "block_id": "2", "active": "1", "title": "Rovnaky clanok", "link": "a", "long": "<p>Text A</p>"},
            {"news_id": "2", "block_id": "2", "active": "1", "title": "Rovnaky clanok", "link": "b", "long": "<p>Text B</p>"},
            {"news_id": "3", "block_id": "2", "active": "0", "title": "Skryty clanok", "link": "c", "long": "<p>Text C</p>"},
            {"news_id": "4", "block_id": "2", "active": "0", "title": "Skryty clanok", "link": "d", "long": "<p>Text D</p>"},
        ]
        report = cross_section_audit.build_report([(section, rows)])
        self.assertEqual(report["totals"]["exact_title_group_count"], 2)
        self.assertEqual(report["totals"]["public_exact_title_group_count"], 1)


class DepthGuardTests(unittest.TestCase):
    def test_unicode_words_are_counted(self):
        markup = "<p>Ľan, žmolkovanie, čistenie a údržba.</p>"
        self.assertEqual(depth_guard.word_count(markup), 5)

    def test_one_character_paragraph_run_is_detected(self):
        run, total = depth_guard.max_short_paragraph_run(
            "<p>s</p><p>á</p><p>v</p><p>Normálny odsek.</p>"
        )
        self.assertEqual(run, 3)
        self.assertEqual(total, 3)


class HtmlSafetyGuardTests(unittest.TestCase):
    def test_valid_rich_article_passes(self):
        result = html_guard.analyze(valid_article())
        self.assertTrue(result["ok"], result["failures"])

    def test_repeated_one_slug_is_blocked(self):
        result = html_guard.analyze(valid_article(link="111111"))
        self.assertFalse(result["ok"])
        self.assertIn("slug is a repeated-1 placeholder", result["failures"])

    def test_price_and_escaped_html_are_blocked(self):
        article = valid_article()
        article["long"] += "<p>Cena: 9,90 €</p>&lt;div&gt;poškodený blok&lt;/div&gt;"
        result = html_guard.analyze(article)
        self.assertFalse(result["ok"])
        self.assertIn("fixed product price detected", result["failures"])
        self.assertIn("escaped HTML detected", result["failures"])


class WorkflowGuardTests(unittest.TestCase):
    def test_project_audit_accepts_content_workflow_branches(self):
        self.assertTrue(project_audit.is_supported_branch("opan-claw"))
        self.assertTrue(project_audit.is_supported_branch("codex/vevo-content-batch-43"))
        self.assertFalse(project_audit.is_supported_branch("main"))
        self.assertFalse(project_audit.is_supported_branch("codex/roy-content-batch-1"))

    def test_powershell_check_enforces_native_exit_codes(self):
        check_script = Path(__file__).resolve().parents[1] / "scripts" / "check.ps1"
        text = check_script.read_text(encoding="utf-8-sig")

        self.assertIn("Invoke-PythonChecked", text)
        self.assertIn("$LASTEXITCODE", text)

    def test_project_audit_reads_mcp_public_ok_count(self):
        original_exports = project_audit.EXPORTS
        try:
            with tempfile.TemporaryDirectory(dir=project_audit.ROOT) as temp_dir:
                project_audit.EXPORTS = Path(temp_dir)
                report = project_audit.EXPORTS / "batch-999-2026-07-14-mcp-publication.json"
                report.write_text(
                    json.dumps(
                        {
                            "posts": [{"post_id": "1"}, {"post_id": "2"}],
                            "public_ok_count": 2,
                            "all_ok": True,
                        }
                    ),
                    encoding="utf-8",
                )
                summary = project_audit.analyze_publication_verify(999)
                self.assertEqual(summary["files"][0]["article_count"], 2)
                self.assertEqual(summary["files"][0]["ok_count"], 2)
                self.assertTrue(summary["files"][0]["all_ok"])
        finally:
            project_audit.EXPORTS = original_exports


if __name__ == "__main__":
    unittest.main()
