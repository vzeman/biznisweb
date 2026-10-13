"""Reviewed dated cost bindings preserve accounting policy and historical results."""

import copy
import json
import os
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from export_orders import BizniWebExporter
from reporting_core.cost_estimate_policy import assess_configured_cost_estimates


CANONICAL = "Parfum do prania Vevo Premium No.08 Cotton Dream (200ml)"
CZ_LABEL = "Parfém na praní Vevo Premium No.08 Cotton Dream (200ml)"
HU_LABEL = "Vevo Premium No.08 Cotton Dream mosóparfüm (200ml)"
POLICY_SKU = "H-3FECDEE3"
CZ_POLICY_LABEL = "Univerzální voňavý čistič Vevo Pure Harmony 500 ml"
HU_POLICY_LABEL = "Synthetic reviewed Hungarian Pure Harmony 500 ml"


class ProductCostIdentityBindingTests(unittest.TestCase):
    def setUp(self):
        environment = patch.dict(os.environ)
        environment.start()
        self.addCleanup(environment.stop)
        mode = patch("export_orders.EXPENSE_MATCH_MODE", "title_first")
        mode.start()
        self.addCleanup(mode.stop)
        self.exporter = self.make_exporter()
        self.exporter.project_settings["order_revenue_reconciliation_enabled"] = False

    @staticmethod
    def make_exporter(project="vevo"):
        # Synthetic cases do not depend on a deployment's live reviewed catalogue.
        with patch.object(BizniWebExporter, "_load_product_cost_identity_bindings", return_value=(None, {})):
            return BizniWebExporter(
                api_url="https://example.test/graphql", api_token="synthetic-token",
                project_name=project, enable_period_bundle=False, order_facts_only=True,
            )

    @staticmethod
    def config(labels=None, target=CANONICAL, **identity):
        return {
            "schema_version": 1, "effective_from": "2026-01-01",
            "bindings": [{
                "id": "reviewed-cotton-200", "source_labels": labels or [CZ_LABEL, HU_LABEL],
                "canonical_expense_label": target, "warehouse_numbers": ["08200p"],
                "evidence_product_ids": ["synthetic-native-product"], **identity,
            }],
        }

    def configure(self, expenses, config=None, exporter=None):
        exporter = exporter or self.exporter
        effective, by_label = exporter._validate_product_cost_identity_bindings(
            config or self.config(), expenses,
            approved_policy_margins=exporter.project_settings.get("authoritative_margin_override_skus") or {},
            approved_bundle_labels=exporter.product_cost_bundle_rules_by_label,
        )
        exporter._rebuild_product_expense_indexes(expenses)
        exporter.product_cost_identity_effective_from = effective
        exporter.product_cost_identity_bindings_by_label = by_label
        return exporter

    @classmethod
    def policy_config(cls, target=None, labels=None):
        config = cls.config(labels=labels or [CZ_POLICY_LABEL, HU_POLICY_LABEL],
                            warehouse_numbers=["POLICY-UNIT"], canonical_policy_sku=POLICY_SKU)
        if target is None:
            config["bindings"][0].pop("canonical_expense_label")
        else:
            config["bindings"][0]["canonical_expense_label"] = target
        return config

    @staticmethod
    def order(label, purchase_day="2026-09-01", quantity=2, **identifiers):
        return {
            "order_num": "SYNTHETIC-DATED-COST", "pur_date": f"{purchase_day} 10:00:00",
            "sum": {"value": 24.6, "currency": {"code": "EUR"}},
            "items": [{
                "item_label": label, "quantity": quantity, "tax_rate": 23,
                "price": {"value": 10, "currency": {"code": "EUR"}},
                "sum": {"value": 20, "currency": {"code": "EUR"}},
                "sum_with_tax": {"value": 24.6, "currency": {"code": "EUR"}},
                **identifiers,
            }],
        }

    def test_same_verified_cz_hu_units_use_existing_canonical_identifier_cost(self):
        self.configure({CANONICAL: 6.96, f"{CANONICAL}||08200p": 6.69, CZ_LABEL: 1, HU_LABEL: 1})
        for label in (CZ_LABEL, HU_LABEL):
            with self.subTest(label=label):
                cost, source = self.exporter._resolve_product_expense(
                    "", label, warehouse_number="08200p", purchase_date="2026-01-01",
                )
                self.assertEqual(6.69, cost)
                self.assertEqual("cost_identity_binding:reviewed-cotton-200:mapped_compound_key", source)
        # A distinct identifier-specific price is not collapsed into the warehouse price.
        self.configure(
            {CANONICAL: 6.96, f"{CANONICAL}||8586024430464": 6.96, CZ_LABEL: 1},
            self.config(eans=["8586024430464"]),
        )
        self.assertEqual(6.96, self.exporter._resolve_product_expense(
            "", CZ_LABEL, ean="8586024430464", purchase_date="2026-01-01",
        )[0])

    def test_date_boundary_missing_invalid_and_old_dates_keep_legacy_price(self):
        self.configure({CANONICAL: 9, CZ_LABEL: 1})
        for purchase_day in (None, "", "not-a-date", "2026-01-01-invalid", "2025-12-31 23:59:59",
                             "2025-12-31T23:59:59-12:00", pd.NaT):
            with self.subTest(purchase_day=purchase_day):
                self.assertEqual((1.0, "mapped_item_label"), self.exporter._resolve_product_expense(
                    "", CZ_LABEL, warehouse_number="08200p", purchase_date=purchase_day,
                ))
        for purchase_day in (date(2026, 1, 1), datetime(2026, 1, 1, 0, 0), "2026-01-01T00:00:00Z",
                             "2026-01-01T00:00:00+14:00"):
            with self.subTest(purchase_day=purchase_day):
                self.assertEqual(9, self.exporter._resolve_product_expense(
                    "", CZ_LABEL, warehouse_number="08200p", purchase_date=purchase_day,
                )[0])

    def test_wrong_warehouse_size_line_and_unknown_titles_never_use_binding(self):
        self.configure({CANONICAL: 9, HU_LABEL: 1})
        for label, warehouse in (
            (HU_LABEL, "08500p"), (HU_LABEL.replace("200ml", "500ml"), "08200p"),
            (HU_LABEL.replace("Premium", "Natural"), "08200p"), ("Unknown product", "08200p"),
        ):
            with self.subTest(label=label, warehouse=warehouse):
                cost, source = self.exporter._resolve_product_expense(
                    "", label, warehouse_number=warehouse, purchase_date="2026-09-01",
                )
                self.assertNotIn("cost_identity_binding", source or "")
                self.assertEqual(1 if label == HU_LABEL else None, cost)

    def test_identity_only_does_not_match_a_custom_compensation_title(self):
        self.configure({CANONICAL: 9, "Custom compensation": 1})
        self.assertEqual((1, "mapped_item_label"), self.exporter._resolve_product_expense(
            "", "Custom compensation", warehouse_number="08200p", purchase_date="2026-09-01",
        ))

    def test_label_only_requires_missing_identifiers_and_explicit_review(self):
        self.configure({CANONICAL: 9, HU_LABEL: 1}, self.config(allow_label_only=True))
        self.assertEqual(9, self.exporter._resolve_product_expense("", HU_LABEL, purchase_date="2026-09-01")[0])
        for identifiers in ({"warehouse_number": "OTHER"}, {"ean": "UNKNOWN"}, {"import_code": "OTHER"}):
            with self.subTest(identifiers=identifiers):
                self.assertEqual((1, "mapped_item_label"), self.exporter._resolve_product_expense(
                    "", HU_LABEL, purchase_date="2026-09-01", **identifiers,
                ))

    def test_conflicting_ean_rejects_otherwise_matching_warehouse(self):
        self.configure({CANONICAL: 9, HU_LABEL: 1}, self.config(eans=["VERIFIED-EAN"]))
        self.assertEqual((1, "mapped_item_label"), self.exporter._resolve_product_expense(
            "", HU_LABEL, warehouse_number="08200p", ean="OTHER-EAN", purchase_date="2026-09-01",
        ))
        self.assertEqual(9, self.exporter._resolve_product_expense(
            "", HU_LABEL, import_code="08200p", ean="VERIFIED-EAN", purchase_date="2026-09-01",
        )[0])

    def test_raw_compound_overrides_preserve_specificity_even_with_stale_raw_label(self):
        for raw_key in (f"{HU_LABEL}||08200p", f"{HU_LABEL.upper()}||08200P"):
            with self.subTest(raw_key=raw_key):
                self.configure({CANONICAL: 9, HU_LABEL: 1, raw_key: 4})
                cost, source = self.exporter._resolve_product_expense(
                    "", HU_LABEL, warehouse_number="08200p", purchase_date="2026-09-01",
                )
                self.assertEqual(4, cost)
                self.assertTrue(source.startswith("mapped_compound_key"))

    def test_ambiguous_normalized_raw_compound_does_not_guess(self):
        self.configure({CANONICAL: 9, HU_LABEL: 1,
                        f"{HU_LABEL.upper()}||08200P": 4, f"{HU_LABEL.lower()}||08200P": 5})
        with self.assertRaisesRegex(ValueError, "ambiguous raw compound"):
            self.exporter._resolve_product_expense("", HU_LABEL, warehouse_number="08200p", purchase_date="2026-09-01")

    def test_percarbonate_canonical_compounds_preserve_cz_hu_identifier_specific_prices(self):
        target = "Perkarbonát sodný PLUS 1kg"
        labels = ["Synthetic reviewed Czech percarbonate PLUS 1kg", "Synthetic reviewed Hungarian percarbonate PLUS 1kg"]
        canonical_hash = self.exporter.get_product_sku("", target)
        self.assertEqual("H-231AAF25", canonical_hash)
        config = self.config(labels=labels, target=target, warehouse_numbers=["55016"],
                             eans=["8595727700347"], allow_label_only=True)
        expenses = {
            canonical_hash: 2.43, f"{target}||{canonical_hash}": 2.43,
            f"{target}||55016": 2.83, f"{target}||8595727700347": 2.83,
            "55016": 1, "8595727700347": 1,
        }
        for label in labels:
            expenses[label] = 1
            expenses[self.exporter.get_product_sku("", label)] = 1
        self.configure(expenses, config)
        for label in labels:
            for identifiers, expected in (({"warehouse_number": "55016"}, 2.83),
                                          ({"ean": "8595727700347"}, 2.83), ({}, 2.43)):
                with self.subTest(label=label, identifiers=identifiers):
                    cost, source = self.exporter._resolve_product_expense(
                        self.exporter.get_product_sku("", label), label,
                        purchase_date="2026-09-01", **identifiers,
                    )
                    self.assertEqual(expected, cost)
                    self.assertTrue(source.startswith("cost_identity_binding:"))
        # A direct native compound can also be the only registered target cost.
        self.configure({f"{target}||55016": 2.83, labels[0]: 1}, config)
        self.assertEqual(2.83, self.exporter._resolve_product_expense(
            "", labels[0], warehouse_number="55016", purchase_date="2026-09-01",
        )[0])

    def test_guarded_target_validation_rejects_unrelated_keys_and_invalid_derived_costs(self):
        target = "Perkarbonát sodný PLUS 1kg"
        canonical_hash = self.exporter.get_product_sku("", target)
        config = self.config(target=target, warehouse_numbers=["55016"], eans=["8595727700347"])
        valid = {canonical_hash: 2.43, f"{target}||55016": 2.83}
        for key in (canonical_hash, f"{target}||55016", f"{target}||8595727700347", f"{target}||{canonical_hash}"):
            for cost in (-1, float("nan"), float("inf"), True):
                with self.subTest(key=key, cost=cost), self.assertRaises(ValueError):
                    self.exporter._validate_product_cost_identity_bindings(config, {**valid, key: cost})
        for expenses in ({"55016": 2.83}, {f"{target}||UNREVIEWED-ID": 2.83},
                         {self.exporter.get_product_sku("", HU_LABEL): 2.83}, {"Different product": 2.83}):
            with self.subTest(expenses=expenses), self.assertRaises(ValueError):
                self.exporter._validate_product_cost_identity_bindings(config, expenses)

    def test_verified_cz_hu_bundles_use_existing_components_and_ignore_shared_gel_ean(self):
        cases = (
            ("Combo Parfum do prania Ylang Absolute 500ml + Prací gél Ylang Absolute 1L", 8.57,
             "ylang_absolute_perfume_gel_1_1"),
            ("Ylang Absolute 500 ml + 3× prací gél 1 l + Shot a odmerka ZADARMO", 14.39,
             "ylang_absolute_perfume_gel_1_3_shot_cup"),
        )
        expenses = {
            "Parfum do prania Vevo Natural No.07 Ylang Absolute (500ml)": 6.14,
            "8594201618000": 2.43,
            "Vevo Shot - koncentrát na čistenie práčky 100ml": 0.65,
            "Odmerka Vevo 7ml drevená na parfum do prania": 0.31,
        }
        for target, expected, rule_id in cases:
            labels = [f"Synthetic reviewed Czech {rule_id}", f"Synthetic reviewed Hungarian {rule_id}"]
            config = self.config(labels=labels, target=target, warehouse_numbers=[], eans=["8594201618000"],
                                 allow_label_only=True)
            self.configure({**expenses, **{label: 1 for label in labels}}, config)
            for label in labels:
                with self.subTest(target=target, label=label):
                    cost, source = self.exporter._resolve_product_expense(
                        "8594201618000", label, ean="8594201618000", purchase_date="2026-09-01",
                    )
                    self.assertEqual(expected, cost)
                    self.assertEqual(f"cost_identity_binding:reviewed-cotton-200:bundle_components_configured:{rule_id}", source)
                    row = self.exporter.flatten_order(self.order(label, quantity=3, ean="8594201618000"))[0]
                    self.assertEqual(round(expected * 3, 2), row["total_expense"])
                    self.assertEqual(expected, row["purchase_cost_reference_per_item"])
            # An explicitly keyed raw whole-bundle price remains an exception.
            self.configure({**expenses, f"{labels[0]}||8594201618000": 20}, config)
            self.assertEqual((20, "mapped_compound_key"), self.exporter._resolve_product_expense(
                "8594201618000", labels[0], ean="8594201618000", purchase_date="2026-09-01",
            ))
            # The canonical label+EAN key also identifies an explicit whole-bundle price.
            self.configure({**expenses, f"{target}||8594201618000": 21}, config)
            self.assertEqual(21, self.exporter._resolve_product_expense(
                "8594201618000", labels[0], ean="8594201618000", purchase_date="2026-09-01",
            )[0])

    def test_only_prevalidated_exact_configured_bundle_targets_are_accepted(self):
        target = "Combo Parfum do prania Ylang Absolute 500ml + Prací gél Ylang Absolute 1L"
        config = self.config(target=target)
        with self.assertRaises(ValueError):
            self.exporter._validate_product_cost_identity_bindings(config, {})
        self.exporter._validate_product_cost_identity_bindings(config, {}, approved_bundle_labels=[target])
        invalid = self.config(target="Unreviewed bundle")
        with self.assertRaises(ValueError):
            self.exporter._validate_product_cost_identity_bindings(invalid, {}, approved_bundle_labels=[target])
        for approved in ("arbitrary", [""], [None], [f" {target}"]):
            with self.subTest(approved=approved), self.assertRaises(ValueError):
                self.exporter._validate_product_cost_identity_bindings(config, {}, approved_bundle_labels=approved)

    def test_reviewed_homogeneous_multipacks_use_exact_registered_base_and_order_quantity(self):
        perfume = "Parfum do prania Vevo Natural No.07 Ylang Absolute (500ml)"
        floor = "Vevo Ylang Absolute parfumovaný čistič podláh pre všetky vysávače s umývaním - 500ml"
        cases = ((f"2x {perfume}", 6.14, 12.28), (f"2x {floor}", 2.35, 4.70),
                 (f"3x {floor}", 2.35, 7.05))
        for target, base_cost, expected in cases:
            labels = [f"Synthetic reviewed Czech {target}", f"Synthetic reviewed Hungarian {target}"]
            base_label = self.exporter._parse_homogeneous_bundle_label(target)[1]
            config = self.config(labels=labels, target=target, warehouse_numbers=[], eans=["SHARED-BASE-EAN"],
                                 allow_label_only=True)
            self.configure({base_label: base_cost, "SHARED-BASE-EAN": base_cost, **{label: 1 for label in labels}}, config)
            for label in labels:
                with self.subTest(target=target, label=label):
                    sku_before = self.exporter.get_reporting_product_sku("SHARED-BASE-EAN", label)
                    row = self.exporter.flatten_order(self.order(label, quantity=3, ean="SHARED-BASE-EAN"))[0]
                    self.assertEqual(expected, row["expense_per_item"])
                    self.assertEqual(round(expected * 3, 2), row["total_expense"])
                    self.assertEqual(label, row["item_label"])
                    self.assertEqual(sku_before, row["product_sku"])
                    self.assertIn("bundle_components_inferred:x", row["expense_source"])
                    old = self.exporter.flatten_order(self.order(label, "2025-12-31", quantity=3, ean="SHARED-BASE-EAN"))[0]
                    self.assertEqual(3, old["total_expense"])
            self.configure({base_label: base_cost, f"{target}||SHARED-BASE-EAN": 17}, config)
            self.assertEqual(17, self.exporter._resolve_product_expense(
                "SHARED-BASE-EAN", labels[0], ean="SHARED-BASE-EAN", purchase_date="2026-09-01",
            )[0])
            self.configure({base_label: base_cost, f"{labels[0]}||SHARED-BASE-EAN": 18}, config)
            self.assertEqual((18, "mapped_compound_key"), self.exporter._resolve_product_expense(
                "SHARED-BASE-EAN", labels[0], ean="SHARED-BASE-EAN", purchase_date="2026-09-01",
            ))

    def test_unregistered_homogeneous_base_and_invalid_base_prices_are_rejected(self):
        target = "2x Exact reviewed base unit"
        config = self.config(target=target)
        for expenses in ({}, {"Unrelated unit": 3}, {"08200p": 3}, {"exact reviewed base unit": 3}):
            with self.subTest(expenses=expenses), self.assertRaises(ValueError):
                self.exporter._validate_product_cost_identity_bindings(config, expenses)
        for cost in (-1, float("nan"), float("inf"), True):
            with self.subTest(cost=cost), self.assertRaises(ValueError):
                self.exporter._validate_product_cost_identity_bindings(config, {"Exact reviewed base unit": cost})
        for unparsed in ("2x Unknown unit + extra", "2x 3x Exact reviewed base unit", "2x Exact reviewed base unit gift"):
            with self.subTest(unparsed=unparsed), self.assertRaises(ValueError):
                self.exporter._validate_product_cost_identity_bindings(self.config(target=unparsed), {"Exact reviewed base unit": 3})

    def test_binding_does_not_change_reporting_sku_label_or_apply_other_projects(self):
        before = self.exporter.get_reporting_product_sku("", HU_LABEL)
        canonical_before = self.exporter.canonicalize_reporting_product_label(HU_LABEL)
        self.configure({CANONICAL: 9, HU_LABEL: 1})
        self.assertEqual(before, self.exporter.get_reporting_product_sku("", HU_LABEL))
        self.assertEqual(canonical_before, self.exporter.canonicalize_reporting_product_label(HU_LABEL))
        roy = self.make_exporter("roy")
        roy._rebuild_product_expense_indexes({CANONICAL: 9, HU_LABEL: 1})
        self.assertEqual((1, "mapped_item_label"), roy._resolve_product_expense(
            "", HU_LABEL, warehouse_number="08200p", purchase_date="2026-09-01",
        ))

    def test_flatten_propagates_order_date_and_preserves_quantities_and_revenue(self):
        self.configure({CANONICAL: 6.96, f"{CANONICAL}||08200p": 6.69, HU_LABEL: 1})
        old = self.exporter.flatten_order(self.order(HU_LABEL, "2025-12-31", warehouse_number="08200p"))[0]
        new = self.exporter.flatten_order(self.order(HU_LABEL, "2026-01-01", warehouse_number="08200p"))[0]
        self.assertEqual(2, old["total_expense"])
        self.assertEqual(13.38, new["total_expense"])
        self.assertEqual(6.69, new["purchase_cost_reference_per_item"])
        for key in ("item_label", "product_sku", "raw_product_sku", "item_quantity", "item_total_without_tax", "item_total_with_tax"):
            self.assertEqual(old[key], new[key])
        self.assertEqual(6.62, new["profit_before_ads"])

    def test_publication_qa_recognizes_bound_cost_and_preserves_actual_fallback_count(self):
        self.configure({CANONICAL: 6.96, f"{CANONICAL}||08200p": 6.69, HU_LABEL: 1})
        with patch("export_orders.MISSING_COST_MARGIN_PCT", 35):
            bound = self.exporter.flatten_order(self.order(HU_LABEL, warehouse_number="08200p"))[0]
            unknown = self.exporter.flatten_order(self.order("Synthetic unknown item"))[0]
            qa = self.exporter._build_product_expense_coverage_qa(pd.DataFrame([bound, unknown]))
        self.assertEqual(0, qa["failure_count"], qa["failures"])
        self.assertEqual(1, qa["fallback_rows"])
        self.assertEqual(1, qa["estimate_validation"]["fallback_rows"])
        self.assertEqual(0, qa["estimate_validation"]["invalid_rows"])

    def test_publication_classifier_rejects_unknown_malformed_and_repeated_bindings(self):
        self.configure({CANONICAL: 9, HU_LABEL: 1})
        item = self.exporter.flatten_order(self.order(HU_LABEL, warehouse_number="08200p"))[0]
        settings = {"cost_estimate_publication": {"allow_configured_margin_fallback": True}}
        valid = (
            "cost_identity_binding:reviewed-cotton-200:mapped_item_label",
            "cost_identity_binding:reviewed_200:mapped_compound_key_normalized",
            "cost_identity_binding:r:canonical_alias:bundle_components_configured:sample_set",
            "bundle_component:cost_identity_binding:r:mapped_product_identifier",
        )
        invalid = (
            "cost_identity_binding::mapped_item_label", "cost_identity_binding:invalid id:mapped_item_label",
            "cost_identity_binding:r:mapped_unknown", "cost_identity_binding:r:unknown_cost",
            "cost_identity_binding:r:cost_identity_binding:r:mapped_item_label",
            "cost_identity_binding:r:canonical_alias:cost_identity_binding:r:mapped_item_label",
            "cost_identity_binding:r:canonical_alias:canonical_alias:mapped_item_label",
            "cost_identity_binding:r:missing_cost_margin_35_fallback",
            "cost_identity_binding:r:zero_cost_service_override", "cost_identity_binding:r:mapped_item_label:extra",
        )
        for source in valid:
            with self.subTest(source=source):
                result = assess_configured_cost_estimates([{**item, "expense_source": source}], settings=settings, margin_pct=35)
                self.assertEqual(0, result["invalid_rows"])
        for source in invalid:
            with self.subTest(source=source):
                result = assess_configured_cost_estimates([{**item, "expense_source": source}], settings=settings, margin_pct=35)
                self.assertEqual(1, result["invalid_rows"])
                with patch("export_orders.MISSING_COST_MARGIN_PCT", 35):
                    qa = self.exporter._build_product_expense_coverage_qa(pd.DataFrame([{**item, "expense_source": source}]))
                self.assertEqual("critical", qa["status"])

    def test_zero_services_and_authoritative_margin_win_over_bound_reference(self):
        for label, policy in (("Spropitné", "service"), (HU_LABEL, "margin")):
            with self.subTest(policy=policy):
                self.configure({CANONICAL: 9, label: 1}, self.config(labels=[label], allow_label_only=True))
                sku = self.exporter.get_reporting_product_sku("", label)
                with patch("export_orders.AUTHORITATIVE_MARGIN_OVERRIDE_SKUS", {sku: 90}):
                    row = self.exporter.flatten_order(self.order(label))[0]
                self.assertEqual(9, row["purchase_cost_reference_per_item"])
                self.assertEqual(0 if policy == "service" else 2, row["total_expense"])
                self.assertEqual("zero_cost_service_override" if policy == "service" else "authoritative_margin_90_override", row["expense_source"])

    def test_policy_only_foreign_product_uses_existing_model_without_invented_purchase_cost(self):
        self.configure({}, self.policy_config())
        for label in (CZ_POLICY_LABEL, HU_POLICY_LABEL):
            with self.subTest(label=label), patch("export_orders.AUTHORITATIVE_MARGIN_OVERRIDE_SKUS", {}):
                sku_before = self.exporter.get_reporting_product_sku("", label)
                row = self.exporter.flatten_order(self.order(label, warehouse_number="POLICY-UNIT"))[0]
                self.assertEqual(sku_before, row["product_sku"])
                self.assertEqual(label, row["item_label"])
                self.assertEqual(2, row["total_expense"])
                self.assertEqual(18, row["profit_before_ads"])
                self.assertEqual("authoritative_margin_90_override", row["expense_source"])
                self.assertIsNone(row["purchase_cost_reference_per_item"])
                self.assertIsNone(row["purchase_cost_reference_source"])
                self.assertEqual((None, None), self.exporter._resolve_product_expense(
                    sku_before, label, warehouse_number="POLICY-UNIT", purchase_date="2026-09-01",
                ))

    def test_policy_binding_keeps_pre2026_missing_date_and_mismatching_identity_legacy(self):
        self.configure({}, self.policy_config())
        orders = [
            self.order(HU_POLICY_LABEL, "2025-12-31", warehouse_number="POLICY-UNIT"),
            self.order(HU_POLICY_LABEL, warehouse_number="OTHER-UNIT"),
            self.order("Unverified foreign cleaner", warehouse_number="POLICY-UNIT"),
            self.order(HU_POLICY_LABEL, warehouse_number="POLICY-UNIT"),
        ]
        orders[-1]["pur_date"] = None
        for order in orders:
            with self.subTest(order=order), patch("export_orders.MISSING_COST_MARGIN_PCT", 35), \
                    patch("export_orders.AUTHORITATIVE_MARGIN_OVERRIDE_SKUS", {}):
                row = self.exporter.flatten_order(order)[0]
                self.assertEqual(13, row["total_expense"])
                self.assertEqual("missing_cost_margin_35_fallback", row["expense_source"])

    def test_policy_does_not_override_nonpositive_revenue_or_reported_quantity(self):
        self.configure({}, self.policy_config())
        for revenue, quantity in ((0, 2), (-20, 2), (20, 0), (20, -2)):
            with self.subTest(revenue=revenue, quantity=quantity), \
                    patch("export_orders.MISSING_COST_MARGIN_PCT", 35), \
                    patch("export_orders.AUTHORITATIVE_MARGIN_OVERRIDE_SKUS", {}):
                order = self.order(HU_POLICY_LABEL, quantity=quantity, warehouse_number="POLICY-UNIT")
                order["items"][0]["sum"]["value"] = revenue
                order["items"][0]["sum_with_tax"]["value"] = round(revenue * 1.23, 2)
                row = self.exporter.flatten_order(order)[0]
                self.assertEqual(round(revenue * 0.65, 2), row["total_expense"])
                self.assertEqual("missing_cost_margin_35_fallback", row["expense_source"])
                self.assertIsNone(row["purchase_cost_reference_per_item"])

    def test_combined_cost_and_policy_binding_keeps_reference_and_service_priority(self):
        self.configure({CANONICAL: 9, HU_POLICY_LABEL: 1}, self.policy_config(target=CANONICAL))
        row = self.exporter.flatten_order(self.order(HU_POLICY_LABEL, warehouse_number="POLICY-UNIT"))[0]
        self.assertEqual(2, row["total_expense"])
        self.assertEqual(9, row["purchase_cost_reference_per_item"])
        self.assertTrue(row["purchase_cost_reference_source"].startswith("cost_identity_binding:"))
        service_config = self.policy_config(target=CANONICAL, labels=["Spropitné"])
        service_config["bindings"][0]["allow_label_only"] = True
        self.configure({CANONICAL: 9, "Spropitné": 1}, service_config)
        service = self.order("Spropitné")
        row = self.exporter.flatten_order(service)[0]
        self.assertEqual(0, row["total_expense"])
        self.assertEqual("zero_cost_service_override", row["expense_source"])
        self.assertEqual(9, row["purchase_cost_reference_per_item"])

    def test_policy_target_must_already_be_approved_and_cannot_supply_a_new_margin(self):
        config = self.policy_config()
        self.assertEqual(90, self.exporter._validate_product_cost_identity_bindings(
            config, {}, {POLICY_SKU: 90},
        )[1][self.exporter._normalize_match_text(HU_POLICY_LABEL)]["canonical_policy_margin_pct"])
        for approved in ({}, {POLICY_SKU: True}, {POLICY_SKU: -1}, {POLICY_SKU: 101},
                         {POLICY_SKU: float("nan")}, {POLICY_SKU: "invalid"}):
            with self.subTest(approved=approved), self.assertRaises(ValueError):
                self.exporter._validate_product_cost_identity_bindings(config, {}, approved)
        for update in ({"canonical_policy_sku": "UNKNOWN"}, {"canonical_policy_sku": None},
                       {"margin_pct": 90}, {"canonical_expense_label": "Missing price"}):
            invalid = copy.deepcopy(config)
            invalid["bindings"][0].update(update)
            with self.subTest(update=update), self.assertRaises(ValueError):
                self.exporter._validate_product_cost_identity_bindings(invalid, {}, {POLICY_SKU: 90})
        missing_targets = copy.deepcopy(config)
        missing_targets["bindings"][0].pop("canonical_policy_sku")
        with self.assertRaises(ValueError):
            self.exporter._validate_product_cost_identity_bindings(missing_targets, {}, {POLICY_SKU: 90})

    def test_bundle_parent_date_reaches_component_cost_resolution(self):
        self.configure({CANONICAL: 3, HU_LABEL: 1})
        self.exporter.project_settings["product_component_expansion_rules"] = [{
            "key": "synthetic-two-units", "bundle_patterns": ["Synthetic bundle"],
            "components": [{"item_label": HU_LABEL, "item_warehouse_number": "08200p", "quantity": 2}],
        }]
        item = {"order_num": "SYNTHETIC-BUNDLE", "item_label": "Synthetic bundle", "product_sku": "BUNDLE",
                "purchase_date": "2026-09-01", "item_quantity": 3, "item_total_without_tax": 60,
                "total_expense": 3, "expense_per_item": 1}
        current = self.exporter.add_reporting_product_identity_columns(pd.DataFrame([item])).iloc[0]
        historical = self.exporter.add_reporting_product_identity_columns(pd.DataFrame([{**item, "purchase_date": "2025-12-31"}])).iloc[0]
        self.assertEqual(6, current["item_quantity"])
        self.assertEqual(18, current["total_expense"])
        self.assertEqual(6, historical["total_expense"])
        self.assertEqual(60, current["item_total_without_tax"])

    def test_optional_loader_absence_and_project_local_file(self):
        with tempfile.TemporaryDirectory() as directory:
            project_path = Path(directory)
            with patch("export_orders.project_dir", return_value=project_path):
                self.assertEqual((None, {}), self.exporter._load_product_cost_identity_bindings())
                (project_path / "product_expenses.json").write_text(json.dumps({CANONICAL: 9}), encoding="utf-8")
                (project_path / "product_cost_identity_bindings.json").write_text(json.dumps(self.config()), encoding="utf-8")
                effective, bindings = self.exporter._load_product_cost_identity_bindings()
                self.assertEqual(date(2026, 1, 1), effective)
                self.assertIn(self.exporter._normalize_match_text(HU_LABEL), bindings)

    def test_actual_vevo_loader_prices_models_bundles_and_roy_stays_without_opt_in(self):
        # Exercise the conventional opt-in file against the real project registry/settings.
        # All orders/credentials are synthetic and no provider request is made.
        project_path = Path(__file__).resolve().parents[1] / "projects" / "vevo"
        self.assertTrue((project_path / "product_cost_identity_bindings.json").is_file())
        actual = BizniWebExporter(
            api_url="https://example.test/graphql", api_token="synthetic-token",
            project_name="vevo", enable_period_bundle=False, order_facts_only=True,
        )
        actual._rebuild_product_expense_indexes(json.loads((project_path / "product_expenses.json").read_text(encoding="utf-8")))
        actual.project_settings["order_revenue_reconciliation_enabled"] = False
        self.assertEqual(date(2026, 1, 1), actual.product_cost_identity_effective_from)
        self.assertEqual(6.69, actual._resolve_product_expense(
            "", CZ_LABEL, warehouse_number="08200p", purchase_date="2026-09-01",
        )[0])
        cases = (
            ("Combo Parfém do praní Ylang Absolute 500 ml + Prací gel Ylang Absolute 1 l", "8594201618000", 8.57),
            ("Combo 1× Parfém do praní Ylang Absolute 500 ml + 3× Prací gel Ylang Absolute 1 l", "8594201618000", 14.39),
            ("2x Parfém na praní Vevo Natural No.07 Ylang Absolute 500 ml", "", 12.28),
            ("2× Vevo Ylang Absolute čistič podlah pro všechny vysavače s mytím – 500 ml", "", 4.70),
            ("3× Vevo Ylang Absolute čistič podlah pro všechny vysavače s mytím – 500 ml", "", 7.05),
        )
        for label, ean, expected in cases:
            with self.subTest(label=label):
                self.assertEqual(expected, actual._resolve_product_expense(
                    ean, label, ean=ean, purchase_date="2026-09-01",
                )[0])
        with patch("export_orders.AUTHORITATIVE_MARGIN_OVERRIDE_SKUS", {}):
            model = actual.flatten_order(self.order(CZ_POLICY_LABEL))[0]
        self.assertEqual("authoritative_margin_90_override", model["expense_source"])
        self.assertEqual(2, model["total_expense"])
        self.assertIsNone(model["purchase_cost_reference_per_item"])
        service = actual.flatten_order(self.order("Spropitné"))[0]
        self.assertEqual(0, service["total_expense"])
        roy = BizniWebExporter(
            api_url="https://example.test/graphql", api_token="synthetic-token",
            project_name="roy", enable_period_bundle=False, order_facts_only=True,
        )
        self.assertEqual((None, {}), roy._load_product_cost_identity_bindings())

    def test_invalid_schema_ambiguities_identity_and_missing_targets_fail_at_load(self):
        invalid = []
        for update in ({"schema_version": True}, {"schema_version": 2}, {"effective_from": "2026-02-30"},
                       {"effective_from": "2026-01-01T00:00:00"}, {"extra": "unknown"}, {"bindings": {}}):
            config = self.config()
            config.update(update)
            invalid.append(config)
        for update in ({"source_labels": [HU_LABEL, HU_LABEL.upper()]}, {"source_labels": []},
                       {"source_labels": [42]}, {"canonical_expense_label": "Missing target"},
                       {"canonical_expense_label": f"{CANONICAL}||08200p"}, {"allow_label_only": "true"},
                       {"evidence_product_ids": []}, {"warehouse_numbers": ["08200p", "08200P"]},
                       {"warehouse_numbers": [], "allow_label_only": False}, {"unknown_field": []}):
            config = self.config()
            config["bindings"][0].update(update)
            invalid.append(config)
        duplicate = self.config()
        duplicate["bindings"].append(copy.deepcopy(duplicate["bindings"][0]))
        invalid.append(duplicate)
        for config in invalid:
            with self.subTest(config=config):
                with self.assertRaises(ValueError):
                    self.exporter._validate_product_cost_identity_bindings(config, {CANONICAL: 9})
        for cost in (-1, float("nan"), float("inf"), True, None, "invalid"):
            with self.subTest(cost=cost):
                with self.assertRaises(ValueError):
                    self.exporter._validate_product_cost_identity_bindings(self.config(), {CANONICAL: cost})


if __name__ == "__main__":
    unittest.main()
