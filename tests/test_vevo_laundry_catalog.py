"""PII-free regression cases for the actual catalogue identity collisions."""

import unittest

from projects.vevo.laundry_series_catalog import classify


class VevoLaundryCatalogTests(unittest.TestCase):
    def test_legacy_and_localized_natural(self):
        for label in (
            "Parfum do prania Vevo No.07 Ylang Absolute (200ml)",
            "Parfém na praní Vevo Natural No.07 Ylang Absolute (200ml)",
            "Vevo No.07 Ylang Absolute mosóparfüm (200ml)",
        ):
            result = classify({"item_label": label, "item_warehouse_number": "07200"})
            self.assertEqual(("Natural", 200, 1), (result["series"], result["size_ml"], result["bottles_per_item"]))

    def test_shared_ean_does_not_override_premium_name(self):
        result = classify({"item_label": "Parfum do prania Vevo Premium No.07 Ylang Absolute (500ml)",
                           "item_ean": "8586024430457", "product_sku": "8586024430457"})
        self.assertEqual("Premium", result["series"])

    def test_warehouse_p_overrides_historical_localized_label_with_note(self):
        result = classify({"item_label": "Vevo No.09 Pure Garden mosóparfüm (200ml)",
                           "item_warehouse_number": "09200p"})
        self.assertEqual("Premium", result["series"])
        self.assertIn("overrides", result["identity_note"])

    def test_explicit_series_or_volume_conflicts_fail(self):
        for code in ("07200p", "07500"):
            with self.subTest(code=code), self.assertRaises(ValueError):
                classify({"item_label": "Parfum do prania Vevo Natural No.07 Ylang Absolute (200ml)",
                          "item_warehouse_number": code})

    def test_scent_conflict_is_visible_without_moving_series(self):
        result = classify({"item_label": "Parfum do prania Vevo No.08 Cotton Dream (500ml)",
                           "item_warehouse_number": "09500"})
        self.assertEqual("Natural", result["series"])
        self.assertIn("scent differs", result["identity_note"])

    def test_samples_other_products_and_mixed_bundles_are_excluded(self):
        for label in (
            "Parfum do prania Vevo Natural No.07 Ylang Absolute (Vzorka 10ml)",
            "Parfum na podlahy Vevo Premium No.07 Ylang Absolute (500ml)",
            "Interiérový sprej Vevo Cotton Dream 500ml",
            "Vevo Natural Bestsellers 3×200 ml + Shot a odmerka ZADARMO",
            "Sada Vevo Ylang Absolute 200ml + vzorky 6x10ml",
            "Combo Parfum do prania Ylang Absolute 500ml + Prací gél Ylang Absolute 1L",
            "Vevo Essence Sample Set", "Vevo Premium Discovery Set",
        ):
            with self.subTest(label=label):
                self.assertIsNone(classify({"item_label": label}))

    def test_pure_sets_have_exact_bottle_counts(self):
        for label, size, count in (
            ("Vevo Natural Bestsellers Set", 200, 3),
            ("Vevo Natural Complete Fragrance Set", 200, 6),
            ("Vevo Natural Pure Garden + Ylang Absolute mosóparfüm-szett 2×500 ml", 500, 2),
            ("2x Parfém na praní Vevo Natural No.07 Ylang Absolute 500 ml", 500, 2),
        ):
            with self.subTest(label=label):
                row = {"item_label": label}
                result = classify(row)
                self.assertEqual(("Natural", size, count), (result["series"], result["size_ml"], result["bottles_per_item"]))
                self.assertIsNone(classify(row, include_pure_sets=False))

    def test_historical_three_pack_requires_export_composition(self):
        row = {"item_label": "3x Parfum do prania Vevo Natural No.07 Ylang Absolute 500ml"}
        self.assertIsNone(classify(row))
        row["expense_source"] = "bundle_components_inferred:x3:mapped_item_label_normalized"
        self.assertEqual(3, classify(row)["bottles_per_item"])
        row["expense_source"] = "bundle_components_configured:ylang_absolute_3x500_shot_2_cups"
        self.assertIsNone(classify(row))

    def test_unrelated_title_cannot_inherit_pure_set_cost_source(self):
        self.assertIsNone(classify({"item_label": "Different item", "expense_source":
                                  "bundle_components_configured:natural_complete_fragrance_6x200ml"}))


if __name__ == "__main__":
    unittest.main()
