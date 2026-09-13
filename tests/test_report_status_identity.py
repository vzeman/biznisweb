import copy
from datetime import datetime
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from graphql import print_ast

from creditnote_export import (
    build_creditnote_reporting_audit,
    fetch_creditnote_orders_by_number,
    order_was_sent_before_creditnote,
)
from export_orders import BizniWebExporter
from order_status_identity import canonical_order


CATALOGUE = [
    {"id": "4", "name": "Shipped"},
    {"id": "17", "name": "Cancelled"},
    {"id": "31", "name": "Payment online - paid"},
    {"id": "33", "name": "Payment online - expired"},
    {"id": "34", "name": "Payment online - expired"},
    {"id": "69", "name": "Stripe - unpaid"},
    {"id": "70", "name": "Stripe - paid"},
]


def order(key="4", label="Shipped", payment="7"):
    return {
        "id": "101", "order_num": "synthetic-order", "pur_date": "2026-09-01 12:00:00",
        "status": {"id": key, "name": label},
        "price_elements": [{"type": "payment", "reference_id": payment, "title": ""}],
    }


class Client:
    def __init__(self, project="vevo", rows=None, orders=None):
        self.transport = SimpleNamespace(url=f"https://{project}.flox.sk/api/graphql")
        self.rows = copy.deepcopy(CATALOGUE if rows is None else rows)
        self.orders = copy.deepcopy(orders or [order()])
        self.calls = []

    def execute(self, query, variable_values=None):
        text = print_ast(getattr(query, "document", query))
        if "mutation" in text:
            raise AssertionError("Reporting must not mutate the provider")
        if "listOrderStatuses" in text:
            self.calls.append("catalogue")
            return {"listOrderStatuses": copy.deepcopy(self.rows)}
        if "getOrderList" in text:
            self.calls.append("orders")
            return {"getOrderList": {"data": copy.deepcopy(self.orders), "pageInfo": {"hasNextPage": False}}}
        if "getOrder(" in text:
            self.calls.append("detail")
            return {"getOrder": copy.deepcopy(self.orders[0])}
        raise AssertionError("Unexpected provider query")


def exporter(project="vevo", rows=None, orders=None):
    result = BizniWebExporter(
        f"https://{project}.flox.sk/api/graphql", "", project_name=project,
        order_facts_only=True,
    )
    result.client = Client(project, rows, orders)
    return result


class ReportStatusIdentityTests(unittest.TestCase):
    def test_same_id_rename_revenue_parity_and_raw_output(self):
        exp = exporter()
        exp.prepare_reporting_status_identity()
        for key, old, new, payment in [
            ("31", "Platba online - zaplatené", "Payment online - paid", "6"),
            ("4", "Odoslaná", "Shipped", "7"),
            ("4", "Odoslaná", "Shipped", "6"),
        ]:
            with self.subTest(key=key, payment=payment):
                before = order(key, old, payment)
                after = order(key, new, payment)
                saved = copy.deepcopy(after)
                self.assertEqual(exp._realized_revenue_decision(before), exp._realized_revenue_decision(after))
                self.assertTrue(exp._realized_revenue_decision(after)[0])
                self.assertIs(exp._filter_by_status([after])[0], after)
                self.assertEqual(saved, after)
        exp.prepare_reporting_status_identity()
        self.assertEqual(["catalogue"], exp.client.calls)

    def test_both_failed_status_ids_remain_distinct_without_changing_display(self):
        exp = exporter()
        exp.prepare_reporting_status_identity()
        rows = [order(key, "Payment online - expired") for key in ("33", "34")]
        self.assertEqual([], exp._filter_by_status(rows))
        self.assertEqual(rows, exp.excluded_orders)
        self.assertNotEqual(exp._status_norm(rows[0]), exp._status_norm(rows[1]))
        self.assertEqual(["Payment online - expired"] * 2, [row["status"]["name"] for row in rows])

    def test_unbound_foreign_id_cannot_receive_paid_or_shipped_role(self):
        exp = exporter()
        exp.prepare_reporting_status_identity()
        for label in ("Odoslaná", "Shipped", "Platba online - zaplatené", "Payment online - paid"):
            with self.subTest(label=label):
                self.assertEqual((False, "unbound_status"), exp._realized_revenue_decision(order("999", label)))

    def test_role_collision_or_missing_reviewed_catalogue_id_blocks(self):
        for rows in (CATALOGUE[:4], [*CATALOGUE, {"id": "999", "name": "Odoslaná"}]):
            exp = exporter(rows=rows)
            with self.assertRaises(ValueError):
                exp.prepare_reporting_status_identity()
            self.assertEqual(["catalogue"], exp.client.calls)

    def test_unapproved_label_and_malformed_id_raise(self):
        exp = exporter()
        exp.prepare_reporting_status_identity()
        for row in (order("4", "Delivered"), order(True, "Shipped"), order("", "Shipped")):
            with self.subTest(row=row["status"]), self.assertRaises(ValueError):
                exp._realized_revenue_decision(row)

    def test_transport_and_project_changes_reject_reuse(self):
        for change in ("transport", "project"):
            exp = exporter()
            exp.prepare_reporting_status_identity()
            if change == "transport":
                exp.client.transport = SimpleNamespace(url=exp.client.transport.url)
            else:
                exp.project_name = "roy"
            with self.subTest(change=change), self.assertRaises(ValueError):
                exp._realized_revenue_decision(order())

    def test_canonical_context_requires_its_original_authority(self):
        exp = exporter()
        exp.prepare_reporting_status_identity()
        canonical = canonical_order(exp.client, order())
        self.assertTrue(exp._realized_revenue_decision(canonical)[0])
        unbound = exporter()
        with self.assertRaises(ValueError):
            unbound._realized_revenue_decision(canonical)

    def test_cached_report_binds_catalogue_before_classifying_raw_rows(self):
        exp = exporter()
        raw = order()
        with patch.object(exp, "should_use_cache", return_value=True), patch.object(exp, "load_from_cache", return_value=[raw]):
            result = exp.fetch_orders(datetime(2026, 9, 1), datetime(2026, 9, 1))
        self.assertEqual([raw], result)
        self.assertEqual("Shipped", result[0]["status"]["name"])
        self.assertEqual(["catalogue"], exp.client.calls)

    def test_creditnote_detail_acquisition_uses_same_catalogue_and_raw_rows(self):
        exp = exporter()
        rows, decisions, errors = fetch_creditnote_orders_by_number(exp, ["synthetic-order"])
        self.assertEqual({}, errors)
        self.assertTrue(decisions["synthetic-order"]["included"])
        self.assertEqual("Shipped", rows[0]["status"]["name"])
        self.assertEqual(["catalogue", "detail"], exp.client.calls)

    def test_all_live_order_read_paths_bind_before_provider_inventory(self):
        for method in ("fetch_all_orders_bulk", "fetch_orders_for_month", "fetch_orders_for_period"):
            with self.subTest(method=method):
                exp = exporter()
                with patch("export_orders.time.sleep"):
                    if method == "fetch_all_orders_bulk":
                        result, _ = exp.fetch_all_orders_bulk(max_orders=30)
                    else:
                        result = getattr(exp, method)(datetime(2026, 9, 1), datetime(2026, 9, 2))
                self.assertEqual(1, len(result))
                self.assertEqual("Shipped", result[0]["status"]["name"])
                self.assertEqual(["catalogue", "orders"], exp.client.calls)

    def test_bad_catalogue_stops_before_inventory_and_missing_payment_still_blocks(self):
        exp = exporter(rows=CATALOGUE[:1])
        with self.assertRaises(ValueError):
            exp.fetch_all_orders_bulk(max_orders=30)
        self.assertEqual(["catalogue"], exp.client.calls)
        exp = exporter()
        exp.prepare_reporting_status_identity()
        raw = order()
        raw.pop("price_elements")
        self.assertEqual((False, "fulfilled_status_missing_payment_metadata"), exp._realized_revenue_decision(raw))

    def test_period_copy_binds_same_catalogue_without_provider_reads(self):
        parent = exporter()
        parent.prepare_reporting_status_identity()
        child = exporter()
        parent._copy_reporting_status_identity(child)
        self.assertTrue(child._realized_revenue_decision(order())[0])
        self.assertEqual([], child.client.calls)
        with self.assertRaises(ValueError):
            parent._copy_reporting_status_identity(exporter("roy"))

    def test_roy_does_not_receive_vevo_aliases(self):
        exp = exporter("roy")
        exp.prepare_reporting_status_identity()
        self.assertTrue(exp._realized_revenue_decision(order(label="Odoslaná"))[0])
        self.assertFalse(exp._realized_revenue_decision(order())[0])
        self.assertEqual([], exp.client.calls)

    def test_current_stripe_paid_mapping_and_unused_label_remain_distinct(self):
        exp = exporter()
        exp.prepare_reporting_status_identity()
        self.assertEqual(
            (False, "non_realized_status"),
            exp._realized_revenue_decision(order("70", "Stripe - paid", "6")),
        )  # Current native Stripe PAID is mapped to reviewed ID 31, not ID 70.
        self.assertTrue(exp._realized_revenue_decision(order("31", "Payment online - paid", "6"))[0])

    def test_frozen_facts_only_path_remains_without_client_or_catalogue(self):
        exp = BizniWebExporter("https://vevo.flox.sk/api/graphql", "", project_name="vevo", order_facts_only=True)
        exp.prepare_reporting_status_identity()
        self.assertIsNone(exp.client)
        self.assertIsNone(exp._reporting_status_identity)
        self.assertTrue(exp._realized_revenue_decision(order(label="Odoslaná"))[0])
        self.assertFalse(exp._realized_revenue_decision(order())[0])

    def test_unpaid_lifecycle_does_not_match_paid_substring(self):
        self.assertEqual("awaiting_payment", BizniWebExporter._report_lifecycle_bucket("Stripe - unpaid")[0])
        self.assertEqual("paid_processing", BizniWebExporter._report_lifecycle_bucket("Stripe - paid")[0])
        self.assertEqual("paid_processing", BizniWebExporter._classify_lifecycle_bucket("Stripe - unpaid")[0])

    def test_shipped_creditnote_uses_native_identity_and_keeps_raw_label(self):
        exp = exporter()
        exp.prepare_reporting_status_identity()
        raw = order()
        self.assertTrue(order_was_sent_before_creditnote(raw, status_client=exp.client))
        self.assertEqual("Shipped", raw["status"]["name"])
        self.assertFalse(order_was_sent_before_creditnote(order("999", "Odoslaná"), status_client=exp.client))

    def test_historical_shipped_id_survives_rename_and_wrong_project_rejects(self):
        exp = exporter()
        exp.prepare_reporting_status_identity()
        current = order("17", "Cancelled")
        audit = {"project": "vevo", "orders": [{"order_num": "synthetic-order", "previous_status_id": "4", "previous_status": "Shipped"}]}
        self.assertTrue(order_was_sent_before_creditnote(current, status_change_audit=audit, status_client=exp.client))
        audit["project"] = "roy"
        with self.assertRaises(ValueError):
            order_was_sent_before_creditnote(current, status_change_audit=audit, status_client=exp.client)

    def test_old_name_only_history_stays_readable_but_new_name_requires_id(self):
        exp = exporter()
        exp.prepare_reporting_status_identity()
        audit = {"orders": [{"order_num": "synthetic-order", "previous_status": "Odoslaná"}]}
        current = order("17", "Cancelled")
        self.assertTrue(order_was_sent_before_creditnote(current, status_change_audit=audit, status_client=exp.client))
        audit["orders"][0]["previous_status"] = "Shipped"
        self.assertFalse(order_was_sent_before_creditnote(current, status_change_audit=audit, status_client=exp.client))

    def test_creditnote_reporting_denominator_uses_bound_context(self):
        exp = exporter()
        exp.prepare_reporting_status_identity()
        raw = order()
        context = {"project": "vevo", "included_orders": [raw], "all_orders": [raw], "_status_client": exp.client}
        _, carriers, _ = build_creditnote_reporting_audit([], {"vevo": context})
        self.assertEqual(1, sum(row["Odoslane objednavky"] for row in carriers))
        with self.assertRaises(ValueError):
            build_creditnote_reporting_audit([], {"roy": context})


if __name__ == "__main__":
    unittest.main()
