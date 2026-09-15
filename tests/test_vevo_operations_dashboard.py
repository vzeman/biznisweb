import copy
import hashlib
import io
import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import operations_inventory as inventory
import roy_operations_dashboard as operations
from live_dashboard_server import LiveDashboardHandler, build_roy_operations_dashboard_html
from order_status_identity import REVIEWED_RENAMES, bind_status_identity, canonical_order
from reporting_core import load_project_settings
from roy_picking_lists_pdf import build_roy_picking_lists_filename, build_roy_picking_lists_pdf
from tests.test_roy_operations_dashboard import make_order, price_element


class VevoOperationsTests(unittest.TestCase):
    def setUp(self):
        self.project_settings = load_project_settings("vevo")
        self.settings = operations.resolve_roy_operations_settings(self.project_settings)
        self.client = SimpleNamespace(transport=SimpleNamespace(url=self.project_settings["biznisweb_api_url"]))
        self.catalogue = [{"id": key, "name": aliases[-1]} for key, (_, aliases) in REVIEWED_RENAMES["vevo"].items()]
        self.catalogue += [{"id": "69", "name": "Stripe - unpaid"}, {"id": "70", "name": "Stripe - paid"}]
        bind_status_identity(self.client, "vevo", self.project_settings).bind_catalogue(self.client, self.catalogue)

    def row(self, status_id, payment_id="18", pickup=False):
        name = next(row["name"] for row in self.catalogue if row["id"] == str(status_id))
        order = make_order("V-TEST", name,
                           price_element("payment", "Dobierkou" if payment_id == "7" else "Bankovym prevodom", payment_id),
                           price_element("shipping", "Osobný odber na sklade BB" if pickup else "DPD", "11" if pickup else "8"))
        order["status"]["id"] = str(status_id)
        return operations._public_order_row(canonical_order(self.client, order), self.settings)

    def test_paid_by_bank_and_both_gateways_remain_fulfillable(self):
        for status_id, payment in [(31, "6"), (31, "1"), (70, "18")]:
            self.assertTrue(self.row(status_id, payment)["fulfillable"])
        self.assertEqual("Payment online - paid", self.row(31)["status"])

    def test_unpaid_expired_cancelled_shipped_cannot_be_picked(self):
        for status_id in [69, 33, 34, 17, 4]:
            for payment in ["6", "7", "18"]:
                self.assertFalse(self.row(status_id, payment)["fulfillable"])
        self.assertFalse(self.row(1, "18")["fulfillable"])
        self.assertFalse(self.row(1, "6")["fulfillable"])
        self.assertTrue(self.row(1, "7")["fulfillable"])

    def test_paid_label_on_foreign_id_is_not_paid(self):
        raw = make_order("V", "Payment online - paid", price_element("payment", "Online", "18"), {})
        raw["status"]["id"] = "777"
        with self.assertRaises(ValueError):
            canonical_order(self.client, raw)
        self.assertFalse(operations._is_paid_online(raw, self.settings))

    def test_sk_cz_hu_payment_and_shipping_eligibility(self):
        # Active catalogue read September 15; provider order reads return SK labels
        # for every storefront. Expected roles are independent of settings.json.
        shops = [
            ("EUR", "7", "Dobierkou", ["1", "6", "18"], ["2", "8", "9", "10", "12", "13", "14", "15", "17", "18"]),
            ("CZK", "10", "Dobírka", ["11", "19"], ["23", "25", "44", "45"]),
            ("HUF", "16", "Utánvétes fizetés", ["17", "20"], ["38", "39", "40", "41", "42", "43"]),
        ]
        for currency, cod_id, cod_title, prepaid_ids, shipping_ids in shops:
            for payment_id in [cod_id, *prepaid_ids]:
                for status_id in [1, 31, 70, 69, 33, 34, 17, 4]:
                    for shipping_id in shipping_ids:
                        with self.subTest(currency=currency, payment=payment_id, status=status_id, shipping=shipping_id):
                            name = next(row["name"] for row in self.catalogue if row["id"] == str(status_id))
                            raw = make_order("V-LOCALIZED", name,
                                price_element("payment", cod_title if payment_id == cod_id else "Online / bank", payment_id),
                                price_element("shipping", "Carrier", shipping_id))
                            raw["status"]["id"] = str(status_id)
                            raw["sum"] = {"value": 100, "formatted": "100 " + currency, "currency": {"code": currency}}
                            row = operations._public_order_row(canonical_order(self.client, raw), self.settings)
                            self.assertEqual(status_id in {31, 70} or (status_id == 1 and payment_id == cod_id), row["fulfillable"])

    def test_reviewed_cod_ids_survive_label_changes(self):
        for payment_id in ["7", "10", "16"]:
            raw = {"status": {"id": "1", "name": "New order"},
                   "price_elements": [price_element("payment", "Localized payment", payment_id)]}
            self.assertEqual((True, "cod_waiting"), operations._is_fulfillable_order(canonical_order(self.client, raw), self.settings))

    def test_localized_cod_enters_pdf_selection_and_preserves_print_tracking(self):
        rows = [self.row(1, payment_id) for payment_id in ["7", "10", "16"]]
        for row, number in zip(rows, ["V-SK", "V-CZ", "V-HU"]):
            row["order_num"] = number
            self.assertTrue(row["fulfillable"])
        state = {"printed_picking_orders": {"V-CZ": {"printed_at": "2026-09-15T10:00:00Z"}}}
        self.assertEqual(["V-SK", "V-HU"], [row["order_num"] for row in operations.select_picking_orders_for_print(rows, state)])
        self.assertEqual(["V-HU"], [row["order_num"] for row in operations.select_picking_orders_for_print(rows, state, order_nums=["V-HU"])])
        self.assertEqual(3, len(operations.select_picking_orders_for_print(rows, state, include_printed=True)))

    def test_no_invented_pickup_ready_status(self):
        row = self.row(31, "6", pickup=True)
        self.assertTrue(row["pickup_ship_action_allowed"])
        self.assertFalse(row["pickup_ready_action_allowed"])
        self.assertFalse(self.row(69, pickup=True)["pickup_ship_action_allowed"])
        self.assertEqual(4, operations._resolve_shipped_status_id(self.client, self.settings))
        with self.assertRaises(ValueError):
            operations._resolve_pickup_ready_status_id(self.client, self.settings)

    def test_foreign_currency_total_is_converted_only_for_eur_summary(self):
        raw = make_order("V-HUF", "Stripe - paid", price_element("payment", "Online", "18"), {})
        raw["status"]["id"] = "70"
        raw["sum"] = {"value": 10000, "formatted": "10 000 Ft", "currency": {"code": "HUF"}}
        row = operations._public_order_row(canonical_order(self.client, raw), self.settings)
        self.assertEqual(25, row["sum_value"])
        self.assertEqual("10 000 Ft", row["sum"])
        self.assertEqual("HUF", row["items"][0]["currency"])
        raw["sum"]["currency"]["code"] = "UNKNOWN"
        with self.assertRaisesRegex(ValueError, "currency rate"):
            operations._public_order_row(raw, self.settings)

    @patch.dict(os.environ, {"REPORT_PROJECT": "vevo"})
    def test_pickup_change_checks_saved_status_without_retrying_mutation(self):
        order = make_order("V", "Payment online - paid", price_element("payment", "Bank", "6"),
                           price_element("shipping", "Osobný odber na sklade BB", "11"))
        order["status"]["id"] = "31"
        for target, succeeds in [("4", True), ("31", False)]:
            observed = {"order_num": "V", "status": {"id": target, "name": "Shipped" if succeeds else "Payment online - paid"}}
            with patch.object(operations, "_build_client", return_value=self.client), patch.object(operations, "load_project_env"), patch.object(operations, "_clear_operations_cache"), patch.object(operations, "_execute_graphql", side_effect=[{"getOrder": order}, {"changeOrderStatus": observed}, {"getOrder": observed}]) as execute:
                if succeeds:
                    self.assertTrue(operations.mark_personal_pickup_shipped("vevo", "V")["ok"])
                else:
                    with self.assertRaisesRegex(RuntimeError, "requires verification"):
                        operations.mark_personal_pickup_shipped("vevo", "V")
                self.assertEqual(3, execute.call_count)
                self.assertFalse(execute.call_args_list[1].kwargs["retry_transient"])

    @patch.dict(os.environ, {"REPORT_PROJECT": "vevo"})
    def test_project_state_and_credentials_fail_closed(self):
        with self.assertRaises(ValueError):
            operations._state_s3_location("roy", load_project_settings("roy"))
        with self.assertRaises(ValueError):
            operations._build_client("roy", load_project_settings("roy"))
        _, key, _ = operations._state_s3_location("vevo", self.project_settings)
        self.assertEqual("daily-reports/vevo/operations/state.json", key)

    def test_html_pdf_and_manufacturing_link_are_project_specific(self):
        html = build_roy_operations_dashboard_html("vevo")
        self.assertIn("VEVO operations dashboard", html)
        self.assertIn('data-marker="vevo-operations-dashboard"', html)
        self.assertIn('href="/manufacturing/vevo"', html)
        self.assertIn('href="/api/operations/vevo/picking-lists.pdf', html)
        self.assertNotIn('/api/operations/roy/', html)
        self.assertNotIn('__SHOP', html)
        self.assertIn('ROY operations dashboard', build_roy_operations_dashboard_html("roy"))
        self.assertTrue(build_roy_picking_lists_filename([], project="vevo").startswith("vevo-"))
        pdf = build_roy_picking_lists_pdf([], project="vevo")
        self.assertIn(b"VEVO operations dashboard", pdf)
        self.assertNotIn(b"ROY operations dashboard", pdf)

    @patch.dict(os.environ, {"REPORT_PROJECT": "vevo"})
    def test_foreign_and_cross_site_posts_stop_before_action(self):
        handler = object.__new__(LiveDashboardHandler)
        handler._send_json = Mock()
        for path, headers, expected in [
            ("/api/operations/roy/inbound/SKU", {}, 409),
            ("/api/operations/vevo/inbound/SKU", {}, 403),
            ("/api/operations/vevo/pickup/123/ship", {
                "Content-Type": "application/json", "X-Operations-Action": "dashboard-action",
                "Sec-Fetch-Site": "cross-site"}, 403),
        ]:
            handler.path, handler.headers = path, headers
            with patch("live_dashboard_server.live_dashboard_auth_credentials", return_value=None), patch("live_dashboard_server.set_inbound_stock_order") as action:
                handler.do_POST()
                self.assertEqual(expected, handler._send_json.call_args.kwargs["status"])
                action.assert_not_called()


class InventorySourceTests(unittest.TestCase):
    def setUp(self):
        self.settings = load_project_settings("vevo")
        self.payload = {"project": "vevo", "generated_at": "2026-09-14T05:19:21Z",
                        "date_from": "2026-08-01", "date_to": "2026-09-13"}
        self.prefix = "daily-reports/vevo/20260914T051922Z/"
        self.raw = json.dumps(self.payload).encode()
        self.manifest = {"project": "vevo", "schema_version": 1, "generation_id": "20260914T051922Z",
                         "artifacts": {"dashboard_payload_latest.json": {
                             "key": self.prefix + "dashboard_payload_latest.json", "size": len(self.raw),
                             "sha256": hashlib.sha256(self.raw).hexdigest()}}}
        self.data = b"realized_revenue,order_num\nTrue,example\n"

    def read(self, manifest=None, payload=None):
        objects = {"daily-reports/vevo/latest/generation.json": json.dumps(manifest or self.manifest).encode(),
                   self.prefix + "dashboard_payload_latest.json": self.raw,
                   self.prefix + "export_20260801-20260913.csv": self.data}
        s3 = Mock()
        s3.get_object.side_effect = lambda **kw: {"Body": io.BytesIO(objects[kw["Key"]]), "ContentLength": len(objects[kw["Key"]])}
        return inventory.read_inventory_export(s3, "vevo", self.settings, payload or self.payload)

    def test_exact_generation_export_and_digest(self):
        data, source = self.read()
        self.assertEqual(data, self.data)
        self.assertEqual(hashlib.sha256(data).hexdigest(), source["export_sha256"])

    def test_project_path_hash_and_generation_mismatch_rejected(self):
        for change in ["project", "key", "sha256", "generation_id"]:
            manifest = copy.deepcopy(self.manifest)
            if change in {"key", "sha256"}:
                manifest["artifacts"]["dashboard_payload_latest.json"][change] = "foreign"
            else:
                manifest[change] = "roy"
            with self.assertRaises(ValueError):
                self.read(manifest)
        with self.assertRaises(ValueError):
            self.read(payload={**self.payload, "generated_at": "old"})

    @patch.dict(os.environ, {"REPORT_PROJECT": "vevo"})
    def test_project_model_costs_are_loaded_and_constants_restored_on_failure(self):
        import export_orders as model
        previous = copy.deepcopy(model.PRODUCT_EXPENSES)
        with self.assertRaisesRegex(RuntimeError, "test interruption"):
            with inventory.configured_model("vevo", self.settings):
                exporter = model.BizniWebExporter(self.settings["biznisweb_api_url"], "", project_name="vevo", order_facts_only=True)
                self.assertIn("Parfum do prania Vevo Natural No.07 Ylang Absolute (500ml)", exporter.product_expenses_exact)
                self.assertIsNone(exporter.fb_client)
                raise RuntimeError("test interruption")
        self.assertEqual(previous, model.PRODUCT_EXPENSES)

    def test_non_realized_export_cannot_enter_stock_model(self):
        data = b"order_num,purchase_date,product_sku,item_quantity,realized_revenue\nV,2026-09-01,S,1,False\n"
        with self.assertRaisesRegex(ValueError, "non-realized"):
            inventory.build_inventory_analytics("vevo", self.settings, data)

    @patch.dict(os.environ, {"REPORT_PROJECT": "vevo"})
    def test_live_catalogue_stock_matches_sku_without_fuzzy_titles(self):
        import pandas as pd
        import time
        frame = pd.DataFrame([
            {"reporting_sku": "V-500", "product_id": "1", "active": True, "quantity_raw": 8, "available_quantity_raw": 6},
            {"reporting_sku": "V-50", "product_id": "2", "active": True, "quantity_raw": 200, "available_quantity_raw": 198},
        ])
        with patch.dict(inventory._CATALOGUE_CACHE, {"vevo": (time.monotonic(), frame)}, clear=True):
            result, diagnostics = inventory.fetch_catalogue_stock("vevo", self.settings, [
                {"sku": "V-500", "product": "Vevo 500 ml"},
                {"sku": "OLD", "product": "Vevo 500 ml"},
            ])
        self.assertEqual({"V-500"}, set(result))
        self.assertEqual(6, result["V-500"]["available_quantity"])
        self.assertEqual(1, diagnostics["unmatched_count"])


class OperationsDeploymentTests(unittest.TestCase):
    def test_schedule_comparison_preserves_configuration_but_ignores_request_id(self):
        from datetime import datetime, timezone
        from scripts.deploy_vevo_board_image import schedule_boundary
        original = {"Name": "vevo-daily-report-email", "State": "ENABLED", "Target": {"Input": "report-only"},
                    "LastModificationDate": datetime(2026, 9, 14, tzinfo=timezone.utc), "ResponseMetadata": {"RequestId": "one"}}
        self.assertEqual(schedule_boundary(original), schedule_boundary({**original, "ResponseMetadata": {"RequestId": "two"}}))
        self.assertNotEqual(schedule_boundary(original), schedule_boundary({**original, "State": "DISABLED"}))
        self.assertEqual(schedule_boundary(original), json.loads(json.dumps(schedule_boundary(original))))

    def test_host_proof_binds_image_task_ip_capacity_and_functional_result(self):
        from scripts.deploy_vevo_board_image import SERVICE, validate_proof
        receipt = {"mode": "operations", "task_arn": "task", "definition": "definition", "started_by": "probe", "digest": "sha256:reviewed"}
        task = {"taskArn": "task", "taskDefinitionArn": "definition", "startedBy": "probe", "lastStatus": "STOPPED",
                "containers": [{"exitCode": 0, "imageDigest": "sha256:reviewed"}],
                "attachments": [{"details": [{"name": "privateIPv4Address", "value": "172.31.1.2"}]}]}
        proof = {"mode": "operations", "identity": {"task_arn": "task", "service": SERVICE, "path": "/app", "private_ips": ["172.31.1.2"]},
                 "summary": {"fulfillable_orders": 1}, "inventory": {"inventory_status": "ok", "inventory_products_total": 2},
                 "pdf_bytes": 2000, "max_rss_kib": 400000}
        validate_proof(task, proof, receipt)
        for changed_task, changed_proof in [
            ({**task, "taskArn": "other"}, proof),
            ({**task, "containers": [{"exitCode": 0, "imageDigest": "other"}]}, proof),
            (task, {**proof, "max_rss_kib": 600000}),
            (task, {**proof, "identity": {**proof["identity"], "private_ips": ["other"]}}),
            (task, {**proof, "inventory": {"inventory_status": "error", "inventory_products_total": 0}}),
        ]:
            with self.assertRaises(AssertionError):
                validate_proof(changed_task, changed_proof, receipt)


if __name__ == "__main__":
    unittest.main()
