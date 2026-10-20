"""The approved VEVO prices replace only the selected product margin policies."""

import json
import os
import unittest
from pathlib import Path
from unittest.mock import patch

import export_orders
from export_orders import BizniWebExporter
from reporting_core import apply_project_runtime, load_project_runtime, load_project_settings


UPDATED_UNIT_COSTS = {
    "Parfum do sušičky Vevo Premium No.01 Cotton Paradise 10ml": 0.4,
    "Parfum do sušičky Vevo Premium No.02 Sweet Paradise 10ml": 0.4,
    "Parfum do sušičky Vevo Premium No.06 Royal Cotton 10ml": 0.4,
    "Parfum do sušičky Vevo Premium No.07 Ylang Absolute 10ml": 0.4,
    "Parfum do sušičky Vevo Premium No.08 Cotton Dream 10ml": 0.4,
    "Parfum do sušičky Vevo Premium No.09 Pure Garden 10ml": 0.4,
    "Parfum na podlahy Vevo Premium No.01 Cotton Paradise (10ml)": 0.4,
    "Parfum na podlahy Vevo Premium No.02 Sweet Paradise (30ml)": 1.1,
    "Parfum na podlahy Vevo Premium No.07 Ylang Absolute (10ml)": 0.4,
    "Parfum na podlahy Vevo Premium No.07 Ylang Absolute (30ml)": 1.1,
    "Parfum na podlahy Vevo Premium No.08 Cotton Dream (10ml)": 0.4,
    "Parfum na podlahy Vevo Premium No.08 Cotton Dream (30ml)": 1.1,
    "Parfum na podlahy Vevo Premium No.09 Pure Garden (10ml)": 0.4,
    "Parfum na podlahy Vevo Premium No.09 Pure Garden (30ml)": 1.1,
}


class VevoPurchaseCostUpdateTests(unittest.TestCase):
    def setUp(self):
        environment = patch.dict(os.environ)
        environment.start()
        self.addCleanup(environment.stop)
        self.exporter = self.make_exporter("vevo")

    def make_exporter(self, project):
        runtime = load_project_runtime(
            project,
            settings=load_project_settings(project),
            default_packaging_cost_per_order=0.0,
            default_shipping_subsidy_per_order=0.0,
            default_fixed_monthly_cost=0.0,
            default_fixed_daily_cost=0.0,
        )
        runtime_globals = {}
        apply_project_runtime(runtime, runtime_globals)
        globals_patch = patch.dict(export_orders.__dict__, runtime_globals)
        globals_patch.start()
        self.addCleanup(globals_patch.stop)
        exporter = BizniWebExporter(
            api_url="https://example.test/graphql",
            api_token="synthetic-token",
            project_name=project,
            enable_period_bundle=False,
            order_facts_only=True,
        )
        # These synthetic orders isolate applied purchase cost from reconciliation.
        exporter.project_settings["order_revenue_reconciliation_enabled"] = False
        return exporter

    @staticmethod
    def order(label, **identifiers):
        return {
            "order_num": "SYNTHETIC-VEVO-PURCHASE-COST",
            "pur_date": "2026-10-20 10:00:00",
            "status": {"name": "Payment online - paid"},
            "sum": {"value": 24.6, "currency": {"code": "EUR"}},
            "items": [{
                "item_label": label,
                "quantity": 2,
                "tax_rate": 23,
                "price": {"value": 10, "currency": {"code": "EUR"}},
                "sum": {"value": 20, "currency": {"code": "EUR"}},
                "sum_with_tax": {"value": 24.6, "currency": {"code": "EUR"}},
                **identifiers,
            }],
        }

    def assert_applied_purchase_cost(self, row, unit_cost):
        self.assertEqual(unit_cost, row["purchase_cost_reference_per_item"])
        self.assertEqual(unit_cost, row["expense_per_item"])
        self.assertEqual(round(unit_cost * 2, 2), row["total_expense"])
        self.assertEqual(round(20 - unit_cost * 2, 2), row["profit_before_ads"])
        self.assertNotIn("margin", row["expense_source"])

    def test_all_fourteen_paid_products_use_purchase_cost_instead_of_ninety_percent_margin(self):
        for label, unit_cost in UPDATED_UNIT_COSTS.items():
            with self.subTest(label=label):
                row = self.exporter.flatten_order(self.order(label))[0]
                self.assert_applied_purchase_cost(row, unit_cost)

    def test_reviewed_czech_hungarian_and_native_identities_use_the_same_purchase_cost(self):
        project_path = Path(__file__).resolve().parents[1] / "projects" / "vevo"
        bindings = json.loads(
            (project_path / "product_cost_identity_bindings.json").read_text(encoding="utf-8")
        )["bindings"]
        tested_targets = set()
        for binding in bindings:
            target = binding.get("canonical_expense_label")
            if target not in UPDATED_UNIT_COSTS:
                continue
            self.assertNotIn("canonical_policy_sku", binding)
            tested_targets.add(target)
            for label in binding["source_labels"]:
                with self.subTest(binding=binding["id"], label=label):
                    row = self.exporter.flatten_order(self.order(label))[0]
                    self.assert_applied_purchase_cost(row, UPDATED_UNIT_COSTS[target])
                    self.assertTrue(row["expense_source"].startswith("cost_identity_binding:"))
        self.assertEqual(set(UPDATED_UNIT_COSTS), tested_targets)

    def test_unmodified_margin_products_and_all_zero_cost_services_keep_their_policy(self):
        for label in (
            "Odmerka Vevo 7ml drevená na parfum do prania",
            "Parfum na podlahy Vevo Premium No.01 Cotton Paradise (30ml)",
            "Parfém na podlahy Vevo Premium No.06 Royal Cotton (10ml)",
            "Univerzální voňavý čistič Vevo Pure Harmony 500 ml",
        ):
            with self.subTest(label=label):
                row = self.exporter.flatten_order(self.order(label))[0]
                self.assertEqual(2.0, row["total_expense"])
                self.assertEqual("authoritative_margin_90_override", row["expense_source"])
        for label in self.exporter.project_settings["zero_cost_service_labels"]:
            with self.subTest(service=label):
                row = self.exporter.flatten_order(self.order(label))[0]
                self.assertEqual(0.0, row["total_expense"])
                self.assertEqual("zero_cost_service_override", row["expense_source"])

    def test_roy_retains_its_separate_authoritative_margin_policy(self):
        roy = self.make_exporter("roy")
        row = roy.flatten_order(self.order("Synthetic ROY Santal", ean="H-94491FEF"))[0]
        self.assertEqual(2.0, row["total_expense"])
        self.assertEqual("authoritative_margin_90_override", row["expense_source"])
        self.assertEqual((None, {}), roy._load_product_cost_identity_bindings())

    def test_supplied_one_cent_label_differences_are_not_inferred_away(self):
        for label, unit_cost in (
            ("Parfum do prania Vevo Natural No.01 Cotton Paradise (200ml)", 2.33),
            ("Parfum do prania Vevo No.01 Cotton Paradise (200ml)", 2.32),
            ("Parfum do prania Vevo Natural No.08 Cotton Dream (200ml)", 2.31),
            ("Parfum do prania Vevo No.08 Cotton Dream (200ml)", 2.3),
        ):
            with self.subTest(label=label):
                self.assert_applied_purchase_cost(
                    self.exporter.flatten_order(self.order(label))[0], unit_cost
                )


if __name__ == "__main__":
    unittest.main()
