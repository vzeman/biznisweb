"""Synthetic coverage for the opt-in shipped-payment publication guard."""

import copy
import json
import os
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from export_orders import (
    ORDER_CACHE_SCHEMA_VERSION,
    BizniWebExporter,
    PaymentMetadataEnrichmentError,
    UnsupportedShippedPaymentError,
)


def item(amount="10"):
    return {
        "item_label": "Synthetic merchandise",
        "quantity": 1,
        "price": {"value": amount, "raw_value": amount},
        "sum": {"value": amount, "raw_value": amount},
        "sum_with_tax": {"value": amount, "raw_value": amount},
    }


def order(payment="UNREVIEWED", title="Synthetic unrecognized method", amount="10"):
    return {
        "order_num": "SYNTHETIC-COVERAGE",
        "pur_date": "2026-09-01 10:00:00",
        "status": {"id": "4", "name": "Odoslaná"},
        "delivery_address": {"country": "PL"},
        "invoice_address": {"country": "SK"},
        "price_elements": [{"type": "payment", "reference_id": payment, "title": title}],
        "items": [item(amount)],
    }


class RoyForeignPaymentCoverageTests(unittest.TestCase):
    def setUp(self):
        environment = patch.dict(os.environ)
        environment.start()
        self.addCleanup(environment.stop)
        self.exporter = self.make_exporter()
        self.date = datetime(2026, 9, 1)

    @staticmethod
    def make_exporter(project="roy", policy="error"):
        exporter = BizniWebExporter(
            api_url="https://example.com/api/graphql", api_token="synthetic",
            project_name=project, enable_period_bundle=False, order_facts_only=True,
        )
        exporter.project_settings = copy.deepcopy(exporter.project_settings)
        exporter.project_settings["realized_revenue"]["unsupported_shipped_merchandise_policy"] = policy
        exporter.realized_revenue_settings = exporter._resolve_realized_revenue_settings()
        exporter.excluded_status_orders = []
        exporter.excluded_orders = []
        return exporter

    def assert_coverage_error(self, source, reason="unsupported_shipped_payment"):
        before = copy.deepcopy(source)
        with self.assertRaises(UnsupportedShippedPaymentError) as raised:
            self.exporter._filter_by_status([source])
        self.assertEqual(reason, raised.exception.issues[0]["reason"])
        self.assertEqual(("SYNTHETIC-COVERAGE",), raised.exception.order_nums)
        self.assertEqual(before, source)
        self.assertEqual([], self.exporter.excluded_status_orders)
        return raised.exception

    def test_configuration_is_default_off_and_strictly_validated(self):
        settings = self.exporter.project_settings["realized_revenue"]
        settings.pop("unsupported_shipped_merchandise_policy")
        self.assertEqual("off", self.exporter._resolve_realized_revenue_settings()["unsupported_shipped_merchandise_policy"])
        for value in (None, True, 1, [], {}, "warn", "ERROR", ""):
            with self.subTest(value=value):
                settings["unsupported_shipped_merchandise_policy"] = value
                with self.assertRaisesRegex(ValueError, "unsupported_shipped_merchandise_policy"):
                    self.exporter._resolve_realized_revenue_settings()

    def test_older_resolved_settings_without_opt_in_preserve_default_behavior(self):
        self.exporter.realized_revenue_settings.pop("unsupported_shipped_merchandise_policy")
        self.assertEqual([], self.exporter._filter_by_status([order()], track_excluded=False))

    def test_known_payment_ids_do_not_depend_on_country_or_translated_title(self):
        known = self.exporter.realized_revenue_settings
        for method in known["cod_payment_ids"] | known["prepaid_payment_ids"]:
            for country in ("SK", "CZ", "HU", "SI", "HR", "PL", "GB", "Unknown"):
                source = order(method, "Synthetic locale-independent method")
                source["delivery_address"]["country"] = country
                with self.subTest(method=method, country=country):
                    self.assertEqual([source], self.exporter._filter_by_status([source], track_excluded=False))

    def test_verified_polish_identity_can_be_added_without_broad_online_pattern(self):
        # Payment identity is configuration; no real order/customer data is used.
        self.exporter.realized_revenue_settings["prepaid_payment_ids"].add("21")
        source = order("21", "Synthetic localized prepaid method")
        self.assertEqual([source], self.exporter._filter_by_status([source]))
        self.assert_coverage_error(order(title="Synthetic online method"))

    def test_existing_english_patterns_work_without_a_country_allowlist(self):
        for title in ("Card", "Cash on delivery"):
            with self.subTest(title=title):
                source = order(title=title)
                source["delivery_address"]["country"] = "GB"
                self.assertEqual([source], self.exporter._filter_by_status([source], track_excluded=False))

    def test_unknown_shipped_nonzero_positive_and_negative_amounts_block(self):
        for amount in ("10", "-10", "0.000001"):
            with self.subTest(amount=amount):
                self.assert_coverage_error(order(amount=amount))

    def test_zero_order_total_or_netting_lines_does_not_exempt_merchandise(self):
        source = order()
        source["sum"] = {"value": 0}
        source["items"].append(item("-10"))
        self.assert_coverage_error(source)

    def test_zero_cost_service_label_does_not_exempt_positive_revenue(self):
        source = order()
        source["items"][0]["item_label"] = "Tringelt"
        source["items"][0]["total_expense"] = 0
        self.assert_coverage_error(source)

    def test_all_native_zero_values_retain_old_exclusion_without_inclusion(self):
        for elements in (order()["price_elements"], [], [{"type": "shipping"}]):
            source = order(amount="0.000")
            source["price_elements"] = elements
            with self.subTest(elements=elements):
                self.assertEqual([], self.exporter._filter_by_status([source], track_excluded=False))
                self.assertEqual((False, "cod_status_without_cod_payment"), self.exporter._realized_revenue_decision(source))

    def test_raw_nonzero_or_unit_nonzero_prevents_rounded_zero_exemption(self):
        for field in ("price", "sum", "sum_with_tax"):
            for key in ("value", "raw_value"):
                source = order(amount="0")
                source["items"][0][field][key] = "0.000001"
                with self.subTest(field=field, key=key):
                    self.assert_coverage_error(source)

    def test_malformed_or_missing_item_values_are_not_zero(self):
        for value in (None, True, "", "NaN", "Infinity", "-Infinity", {}, []):
            for key in ("value", "raw_value"):
                source = order(amount="0")
                source["items"][0]["sum"][key] = value
                with self.subTest(value=value, key=key):
                    self.assert_coverage_error(source, "unsupported_shipped_payment_invalid_item_amount")
        for items in (None, [], {}, [None], [{}], [{"sum": {"value": 0}}]):
            source = order(amount="0")
            source["items"] = items
            with self.subTest(items=items):
                self.assert_coverage_error(source, "unsupported_shipped_payment_invalid_item_amount")

    def test_malformed_payment_array_is_explicit_even_with_known_first_method(self):
        malformed = (
            {}, "payment", [None], ["payment"], [{}], [{"type": 1}],
            [{"type": "payment", "reference_id": []}],
            [{"type": "payment", "reference_id": True}],
            [{"type": "payment", "title": {}}],
            [order("7")["price_elements"][0], None],
        )
        for elements in malformed:
            source = order()
            source["price_elements"] = elements
            with self.subTest(elements=elements):
                self.assert_coverage_error(source, "malformed_shipped_payment_metadata")

    def test_multiple_payment_elements_are_not_resolved_by_first_match(self):
        source = order("7")
        source["price_elements"].append(order()["price_elements"][0])
        self.assert_coverage_error(source, "ambiguous_shipped_payment_metadata")

    def test_missing_metadata_uses_existing_guard_and_exact_exception(self):
        source = order()
        source.pop("price_elements")
        with self.assertRaises(PaymentMetadataEnrichmentError):
            self.exporter._filter_by_status([source])
        self.exporter.realized_revenue_settings["missing_payment_metadata_non_realized_order_nums"].add(source["order_num"])
        self.assertEqual([], self.exporter._filter_by_status([source], track_excluded=False))

    def test_exact_realized_missing_metadata_override_is_preserved(self):
        source = order()
        source.pop("price_elements")
        self.exporter.realized_revenue_settings["missing_payment_metadata_realized_order_nums"].add(source["order_num"])
        self.assertEqual([source], self.exporter._filter_by_status([source], track_excluded=False))
        self.assertEqual(
            (True, "configured_missing_payment_metadata_realized"),
            self.exporter._realized_revenue_decision(source),
        )

    def test_paid_pending_cancelled_and_legacy_decisions_are_unchanged(self):
        for status in ("Platba online - zaplatené", "Čaká na vybavenie", "Storno", "madfrog stara odoslana"):
            source = order()
            source["status"]["name"] = status
            expected, _ = self.exporter._realized_revenue_decision(source)
            with self.subTest(status=status):
                self.assertEqual([source] if expected else [], self.exporter._filter_by_status([source], track_excluded=False))

    def test_vevo_default_and_2025_legacy_membership_remain_unchanged(self):
        exporter = self.make_exporter("vevo", "off")
        for status in ("Odoslaná", "Platba online - zaplatené", "Storno", "madfrog stara odoslana"):
            source = order()
            source["pur_date"] = "2025-08-01 10:00:00"
            source["status"]["name"] = status
            expected, _ = exporter._realized_revenue_decision(source)
            with self.subTest(status=status):
                self.assertEqual([source] if expected else [], exporter._filter_by_status([source], track_excluded=False))

    def test_preflight_aggregates_issues_without_recording_partial_exclusions(self):
        first = order()
        second = order(amount="NaN")
        second["order_num"] = "SYNTHETIC-OTHER"
        with self.assertRaises(UnsupportedShippedPaymentError) as raised:
            self.exporter._filter_by_status([first, second])
        self.assertEqual(2, len(raised.exception.issues))
        self.assertEqual([], self.exporter.excluded_status_orders)

    def test_fresh_month_and_period_paths_block_before_returning_included_subset(self):
        page = {"getOrderList": {"data": [order()], "pageInfo": {"hasNextPage": False}}}
        for method in (self.exporter.fetch_orders_for_month, self.exporter.fetch_orders_for_period):
            with self.subTest(method=method.__name__), patch.object(self.exporter, "_execute_order_page_with_price_elements_fallback", return_value=page):
                with self.assertRaises(UnsupportedShippedPaymentError):
                    method(self.date, self.date)

    def test_out_of_period_source_order_does_not_block_requested_window(self):
        source = order()
        source["pur_date"] = "2025-08-01 10:00:00"
        page = {"getOrderList": {"data": [source], "pageInfo": {"hasNextPage": False}}}
        with patch.object(self.exporter, "_execute_order_page_with_price_elements_fallback", return_value=page):
            self.assertEqual([], self.exporter.fetch_orders_for_period(self.date, self.date))

    def test_cache_cannot_silently_omit_unknown_shipped_merchandise(self):
        with TemporaryDirectory() as folder:
            self.exporter.cache_dir = Path(folder)
            path = self.exporter.get_cache_filename(self.date)
            path.write_text(json.dumps({"schema_version": ORDER_CACHE_SCHEMA_VERSION, "orders": [order()]}), encoding="utf-8")
            with patch.object(self.exporter, "should_use_cache", return_value=True), patch.object(self.exporter, "fetch_all_orders_bulk") as fetch:
                with self.assertRaises(UnsupportedShippedPaymentError):
                    self.exporter.fetch_orders(self.date, self.date)
                fetch.assert_not_called()

    def test_price_elements_fallback_enriches_but_does_not_hide_unknown_identity(self):
        source = order()
        reduced = copy.deepcopy(source)
        reduced.pop("price_elements")
        responses = [
            RuntimeError("provider price_elements field unavailable"),
            {"getOrderList": {"data": [reduced], "pageInfo": {"hasNextPage": False}}},
            {"getOrder": source},
        ]
        with patch.object(self.exporter, "_execute_graphql", side_effect=responses) as execute:
            with self.assertRaises(UnsupportedShippedPaymentError):
                self.exporter.fetch_orders_for_period(self.date, self.date)
        self.assertEqual(3, execute.call_count)

    def test_retry_and_chunk_paths_propagate_coverage_error_without_retry(self):
        issue = UnsupportedShippedPaymentError([{"order_num": "SYNTHETIC-COVERAGE", "reason": "unsupported_shipped_payment"}])
        calls = (
            lambda: self.exporter.fetch_orders_for_month(self.date, self.date),
            lambda: self.exporter.fetch_orders_for_period(self.date, self.date),
            lambda: self.exporter.fetch_all_orders_bulk(max_orders=30),
        )
        for call in calls:
            with patch.object(self.exporter, "_execute_order_page_with_price_elements_fallback", side_effect=issue) as execute:
                with self.assertRaises(UnsupportedShippedPaymentError):
                    call()
                self.assertEqual(1, execute.call_count)
        with patch.object(self.exporter, "fetch_orders_for_period", side_effect=issue) as fetch:
            with self.assertRaises(UnsupportedShippedPaymentError):
                self.exporter._fetch_orders_original(self.date, self.date)
            self.assertEqual(1, fetch.call_count)


if __name__ == "__main__":
    unittest.main()
