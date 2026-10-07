"""Synthetic coverage for reviewed VEVO catalogue translations and cost precedence."""

import os
import unittest
from unittest.mock import patch

from export_orders import BizniWebExporter


class VevoLocalizedProductCostTests(unittest.TestCase):
    def setUp(self):
        self.environment_patch = patch.dict(os.environ)
        self.environment_patch.start()
        self.addCleanup(self.environment_patch.stop)
        self.mode_patch = patch("export_orders.EXPENSE_MATCH_MODE", "title_first")
        self.mode_patch.start()
        self.addCleanup(self.mode_patch.stop)
        self.exporter = self.make_exporter("vevo")

    @staticmethod
    def make_exporter(project):
        return BizniWebExporter(
            api_url="https://example.com/api/graphql",
            api_token="synthetic-token",
            project_name=project,
            enable_period_bundle=False,
            order_facts_only=True,
        )

    def test_verified_localized_variants_use_the_matching_canonical_unit(self):
        cases = [
            ("Vevo Natural No.07 Ylang Absolute mosóparfüm (10 ml-es minta)", "Parfum do prania Vevo No.07 Ylang Absolute (Vzorka 10ml)", "07001"),
            ("Vevo Natural No.08 Cotton Dream mosóparfüm (10 ml-es minta)", "Parfum do prania Vevo No.08 Cotton Dream (Vzorka 10ml)", "08001"),
            ("Vevo Natural No.09 Pure Garden mosóparfüm (10 ml-es minta)", "Parfum do prania Vevo No.09 Pure Garden (Vzorka 10ml)", "09001"),
            ("Parfém na praní Vevo Natural No.06 Royal Cotton (Vzorek 10ml)", "Parfum do prania Vevo No.06 Royal Cotton (Vzorka 10ml)", "06001"),
            ("Parfém na praní Vevo Natural No.08 Cotton Dream (Vzorek 10ml)", "Parfum do prania Vevo No.08 Cotton Dream (Vzorka 10ml)", "08001"),
            ("Parfém na praní Vevo Natural No.09 Pure Garden (Vzorek 10ml)", "Parfum do prania Vevo No.09 Pure Garden (Vzorka 10ml)", "09001"),
            ("Parfém na praní Vevo Premium No.07 Ylang Absolute (200ml)", "Parfum do prania Vevo Premium No.07 Ylang Absolute (200ml)", "07200p"),
            ("Parfém na praní Vevo Premium No.07 Ylang Absolute (500ml)", "Parfum do prania Vevo Premium No.07 Ylang Absolute (500ml)", "07500p"),
            ("Parfém na praní Vevo Premium No.07 Ylang Absolute (Vzorek 10ml)", "Parfum do prania Vevo Premium No.07 Ylang Absolute (Vzorka 10ml)", "07001p"),
            ("Parfém na praní Vevo Premium No.08 Cotton Dream (200ml)", "Parfum do prania Vevo Premium No.08 Cotton Dream (200ml)", "08200p"),
            ("Parfém na praní Vevo Premium No.09 Pure Garden (200ml)", "Parfum do prania Vevo Premium No.09 Pure Garden (200ml)", "09200p"),
            ("Vevo Premium No.07 Ylang Absolute mosóparfüm (500ml)", "Parfum do prania Vevo Premium No.07 Ylang Absolute (500ml)", "07500p"),
            ("Vevo Premium mosóparfüm-minták 3 × 10 ml", "Vzorky parfumov do prania Vevo Premium 3 x 10ml", "01053c"),
            ("Vzorky parfémů do praní Vevo Premium 3 x 10ml", "Vzorky parfumov do prania Vevo Premium 3 x 10ml", "01053c"),
            ("Vevo Shot - koncentrát na čištění pračky 100ml", "Vevo Shot - koncentrát na čistenie práčky 100ml", "VSHT01"),
            ("Vevo 7 ml-es fa mérőkanál mosóparfümhöz", "Odmerka Vevo 7ml drevená na parfum do prania", "ODV01"),
            ("Vevo Natural dřevěná vůně do auta Santal Inspiration List", "Vevo Natural drevená vôňa do auta Santal Inspiration list", "55003"),
        ]
        self.assertIs(self.exporter.project_settings.get("expense_alias_fallback"), True)
        costs = {label: float(index + 1) for index, label in enumerate(dict.fromkeys(c[1] for c in cases))}
        self.exporter._rebuild_product_expense_indexes(costs)
        for localized, canonical, warehouse in cases:
            with self.subTest(label=localized):
                self.assertEqual(canonical, self.exporter.canonicalize_reporting_product_label(localized))
                sku = self.exporter.get_reporting_product_sku("", localized)
                cost, source = self.exporter._resolve_product_expense(sku, localized, warehouse_number=warehouse)
                self.assertEqual(costs[canonical], cost)
                self.assertEqual("canonical_alias:mapped_item_label", source)

    def test_alias_keeps_original_compound_identifier_specificity(self):
        localized = "Parfém na praní Vevo Premium No.08 Cotton Dream (200ml)"
        canonical = self.exporter.canonicalize_reporting_product_label(localized)
        self.exporter._rebuild_product_expense_indexes({canonical: 9.0, f"{canonical}||08200p": 7.0})
        self.assertEqual(
            (7.0, "canonical_alias:mapped_compound_key"),
            self.exporter._resolve_product_expense("", localized, warehouse_number="08200p"),
        )

    def test_raw_exact_compound_identifier_and_zero_costs_keep_precedence(self):
        localized = "Parfém na praní Vevo Premium No.08 Cotton Dream (200ml)"
        canonical = self.exporter.canonicalize_reporting_product_label(localized)
        cases = [
            ({localized: 0.0}, 0.0, "mapped_item_label"),
            ({f"{localized}||SYNTHETIC": 2.0}, 2.0, "mapped_compound_key"),
            ({"SYNTHETIC": 3.0}, 3.0, "mapped_product_identifier"),
        ]
        for raw, expected, source in cases:
            with self.subTest(source=source):
                self.exporter._rebuild_product_expense_indexes({canonical: 9.0, **raw})
                self.assertEqual(
                    (expected, source),
                    self.exporter._resolve_product_expense("", localized, warehouse_number="SYNTHETIC"),
                )

    def test_localized_mixed_bundles_use_existing_composition(self):
        self.exporter._rebuild_product_expense_indexes({
            "Parfum do prania Vevo Natural No.07 Ylang Absolute (500ml)": 2.0,
            "Parfum do prania Vevo Natural No.09 Pure Garden (500ml)": 3.0,
            "Vevo Shot - koncentrát na čistenie práčky 100ml": 4.0,
            "Odmerka Vevo 7ml drevená na parfum do prania": 0.5,
        })
        cases = [
            ("Dvě nejoblíbenější vůně + čistá pračka", "VEVO-DUO-CISTA-PRACKA-2026", 10.0, "ylang_pure_garden_shot_2_cups"),
            ("Vevo Natural Pure Garden + Ylang Absolute mosóparfüm-szett 2×500 ml", "", 5.0, "natural_bestsellers_500ml_ylang_pure_garden"),
        ]
        for label, import_code, expected, rule_id in cases:
            with self.subTest(label=label):
                cost, source = self.exporter._resolve_product_expense(import_code, label, import_code=import_code)
                self.assertEqual(expected, cost)
                self.assertEqual(f"canonical_alias:bundle_components_configured:{rule_id}", source)

    def test_localized_homogeneous_bundle_preserves_multiplier_and_order_quantity(self):
        self.exporter._rebuild_product_expense_indexes({
            "Parfum do prania Vevo Natural No.07 Ylang Absolute (500ml)": 2.0,
        })
        rows = self.exporter.flatten_order({
            "id": "synthetic-1",
            "order_num": "SYNTHETIC-LOCALIZED-BUNDLE",
            "pur_date": "2026-09-01 10:00:00",
            "sum": {"value": 72.0, "currency": {"code": "EUR"}},
            "items": [{
                "item_label": "2x Parfém na praní Vevo Natural No.07 Ylang Absolute 500 ml",
                "quantity": 3,
                "tax_rate": 20,
                "price": {"value": 20.0, "currency": {"code": "EUR"}},
                "sum": {"value": 60.0, "currency": {"code": "EUR"}},
                "sum_with_tax": {"value": 72.0, "currency": {"code": "EUR"}},
            }],
        })
        self.assertEqual(4.0, rows[0]["expense_per_item"])
        self.assertEqual(12.0, rows[0]["total_expense"])
        self.assertEqual("canonical_alias:bundle_components_inferred:x2:mapped_item_label", rows[0]["expense_source"])

    def test_aliases_do_not_recurse_or_infer_unknown_products(self):
        self.exporter.product_name_aliases_exact.update({"Alias A": "Alias B", "Alias B": "Alias A"})
        self.exporter._rebuild_product_expense_indexes({"Different product": 2.0})
        for label in ("Alias A", "Alias B", "Vevo Premium No.07 Ylang Absolute mosóparfüm (900ml)", "Univerzální voňavý čistič Vevo Pure Harmony 500 ml"):
            with self.subTest(label=label):
                self.assertEqual((None, None), self.exporter._resolve_product_expense("", label))

    def test_other_projects_keep_existing_resolution_without_explicit_opt_in(self):
        exporter = self.make_exporter("roy")
        self.assertFalse(exporter.project_settings.get("expense_alias_fallback", False))
        exporter.product_name_aliases_exact["Synthetic localized"] = "Synthetic canonical"
        exporter._rebuild_product_expense_indexes({"Synthetic canonical": 2.0})
        self.assertEqual((None, None), exporter._resolve_product_expense("", "Synthetic localized"))


if __name__ == "__main__":
    unittest.main()
