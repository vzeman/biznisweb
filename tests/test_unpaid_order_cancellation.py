import json
import unittest
from copy import deepcopy
from dataclasses import replace
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from tests.test_order_status_safety import MemoryAutomationStore, money, receipt

from unpaid_order_cancellation import (
    cancellation_eligibility_reason,
    recovery_eligibility_reason,
    resolve_unpaid_cancellation_settings,
    run_unpaid_order_cancellation,
    fetch_orders_for_cancellation,
    UnpaidCancellationSummary,
)
from unpaid_order_cancellation_runner import run_unpaid_cancellation_runner


ROOT_DIR = Path(__file__).resolve().parents[1]


def price_element(kind: str, title: str, reference_id: str = "") -> dict:
    return {
        "type": kind,
        "title": title,
        "reference_id": reference_id,
        "value": "",
        "price": {"value": 0, "formatted": "0,00 EUR"},
    }


def make_invoice(
    invoice_num: str = "INV-1",
    *,
    created: str = "2026-05-02 10:00:00",
    paid: bool = False,
) -> dict:
    return {
        "id": invoice_num,
        "invoice_num": invoice_num,
        "created": created,
        "paid": paid,
        "pay_date": created if paid else None,
        "sum": money(), "payments": [], "preinvoice": None,
    }


def make_order(
    order_num: str,
    status_name: str,
    payment_title: str,
    payment_ref: str,
    pur_date: str,
    *,
    last_change: str | None = None,
    invoices: list | None = None,
) -> dict:
    return {
        "id": order_num,
        "order_num": order_num,
        "pur_date": pur_date,
        "last_change": last_change or pur_date,
        "blocked": False,
        "status": {"id": 1, "name": status_name},
        "price_elements": [price_element("payment", payment_title, payment_ref)],
        "invoices": list(invoices or []),
        "sum": money(), "shipments": [], "preinvoices": [],
    }


class FakeBizniswebClient:
    def __init__(self, pages, statuses=None, detail_overrides=None, detail_sequences=None):
        self.pages = list(pages)
        self.statuses = statuses or [
            {"id": 74, "name": "Nezaplaten\u00e1 - zru\u0161en\u00e1 objedn\u00e1vka"},
            {"id": 4, "name": "Odoslan\u00e1"},
            {"id": 55, "name": "Platba online - zaplaten\u00e9"},
            {"id": 1, "name": "\u010cak\u00e1 na vybavenie"},
            {"id": 2, "name": "\u010cak\u00e1 na \u00fahradu"},
            {"id": 68, "name": "Platba online - platnos\u0165 vypr\u0161ala"},
            {"id": 69, "name": "Stripe - expired"},
        ]
        self.detail_overrides = detail_overrides or {}
        self.detail_sequences = {key: list(value) for key, value in (detail_sequences or {}).items()}
        self.page_calls = 0
        self.page_requests = []
        self.before_page_read = lambda *_: None
        self.after_page_read = lambda *_: None
        self.mutations = []
        # Model actual immutable numeric IDs and the shop's status catalogue.
        for identity, order in enumerate((row for page in self.pages for row in page), 1):
            order["id"] = str(identity)
            if isinstance(order.get("status"), dict):
                match = next((row for row in self.statuses if row["name"] == order["status"].get("name")), None)
                if match:
                    order["status"] = dict(match)

    def execute(self, query, variable_values=None):  # noqa: ANN001 - mimics gql.Client
        variables = variable_values or {}
        if "lang_code" in variables:
            return {"listOrderStatuses": self.statuses}
        if "order_num" in variables and "status_id" in variables:
            self.mutations.append((variables["order_num"], variables["status_id"]))
            status = next(row for row in self.statuses if row["id"] == variables["status_id"])
            for page in self.pages:
                for order in page:
                    if order["order_num"] == variables["order_num"]:
                        order["status"] = dict(status)
            if variables["order_num"] in self.detail_sequences:
                self.detail_sequences[variables["order_num"]] = [{"order_num": variables["order_num"], "status": dict(status)}]
            return {
                "changeOrderStatus": {
                    "order_num": variables["order_num"],
                    "status": dict(status),
                }
            }
        if "order_num" in variables:
            order_num = str(variables["order_num"])
            if order_num in self.detail_sequences:
                sequence = self.detail_sequences[order_num]
                if len(sequence) > 1:
                    return {"getOrder": sequence.pop(0)}
                return {"getOrder": sequence[0]}
            if order_num in self.detail_overrides:
                return {"getOrder": self.detail_overrides[order_num]}
            for page in self.pages:
                for order in page:
                    if str(order.get("order_num")) == order_num:
                        return {"getOrder": order}
            return {"getOrder": None}
        if "params" in variables:
            self.page_calls += 1
            self.page_requests.append(deepcopy(variables))
            self.before_page_read(self, variables)
            params = variables["params"]
            rows = sorted((deepcopy(row) for page in self.pages for row in page),
                          key=lambda row: int(row["id"]), reverse=params["sort"] == "DESC")
            offset, limit = params.get("cursor", 0), params["limit"]
            data = rows[offset:offset + limit]
            has_next = offset + limit < len(rows)
            result = {
                "getOrderList": {
                    "data": data,
                    "pageInfo": {
                        "hasNextPage": has_next,
                        "nextCursor": offset + limit if has_next else None,
                        "totalRecords": len(rows),
                    },
                }
            }
            self.after_page_read(self, variables, result)
            return result
        raise AssertionError(f"Unexpected GraphQL variables: {variables}")


class PartialDataError(Exception):
    def __init__(self, data):
        super().__init__("partial data available")
        self.data = data


class PartialFirstPageClient(FakeBizniswebClient):
    def execute(self, query, variable_values=None):  # noqa: ANN001 - mimics gql.Client
        variables = variable_values or {}
        if "params" in variables and self.page_calls == 0:
            self.page_calls += 1
            raise PartialDataError(
                {
                    "getOrderList": {
                        "data": self.pages[0],
                        "pageInfo": {
                            "hasNextPage": False,
                            "nextCursor": None,
                            "pageIndex": 1,
                            "totalPages": 1,
                        },
                    }
                }
            )
        return super().execute(query, variable_values)


class UnpaidOrderCancellationTests(unittest.TestCase):
    def setUp(self):
        sleep = patch("unpaid_order_cancellation.time.sleep")
        sleep.start()
        self.addCleanup(sleep.stop)

    def make_settings(self):
        return resolve_unpaid_cancellation_settings(
            {
                "unpaid_order_cancellation": {
                    "enabled": True,
                    "target_status_name": "Nezaplaten\u00e1 - zru\u0161en\u00e1 objedn\u00e1vka",
                    "target_status_id": 74,
                    "recovery_enabled": True,
                    "recovery_target_status_name": "Platba online - zaplaten\u00e9",
                    "recovery_target_status_id": 55,
                    "recovery_source_statuses": ["Stripe - expired"],
                    "candidate_statuses": [
                        "\u010cak\u00e1 na vybavenie",
                        "\u010cak\u00e1 na \u00fahradu",
                        "Platba online - platnos\u0165 vypr\u0161ala",
                        "Stripe - expired",
                    ],
                    "excluded_statuses": [
                        "Nezaplaten\u00e1 - zru\u0161en\u00e1 objedn\u00e1vka",
                        "Platba online - zaplaten\u00e9",
                        "Odoslan\u00e1",
                    ],
                    "payment_reference_ids": ["6", "18"],
                    "payment_title_patterns": ["Bankov\u00fdm prevodom", "Okam\u017eit\u00e1 platba online"],
                    "scan_max_pages": 5,
                }
            }
        )

    def test_eligibility_uses_age_status_and_payment_type(self) -> None:
        settings = self.make_settings()
        cutoff = date(2026, 5, 13)

        cases = [
            (
                make_order("R-1", "\u010cak\u00e1 na \u00fahradu", "Bankov\u00fdm prevodom", "6", "2026-05-13 10:00:00"),
                "eligible",
            ),
            (
                make_order("R-2", "Platba online - platnos\u0165 vypr\u0161ala", "Okam\u017eit\u00e1 platba online", "18", "2026-05-01 10:00:00"),
                "eligible",
            ),
            (
                make_order("R-3", "\u010cak\u00e1 na \u00fahradu", "Bankov\u00fdm prevodom", "6", "2026-05-14 10:00:00"),
                "not_old_enough",
            ),
            (
                make_order("R-4", "\u010cak\u00e1 na vybavenie", "Dobierkou", "7", "2026-05-01 10:00:00"),
                "payment_not_matched",
            ),
            (
                make_order("R-5", "Platba online - zaplaten\u00e9", "Okam\u017eit\u00e1 platba online", "18", "2026-05-01 10:00:00"),
                "excluded_status",
            ),
            (
                make_order(
                    "R-6",
                    "Nezaplaten\u00e1 - zru\u0161en\u00e1 objedn\u00e1vka",
                    "Bankov\u00fdm prevodom",
                    "6",
                    "2026-05-01 10:00:00",
                ),
                "already_target_status",
            ),
            (
                make_order(
                    "R-7",
                    "\u010cak\u00e1 na vybavenie",
                    "Bankov\u00fdm prevodom",
                    "6",
                    "2026-05-01 10:00:00",
                ),
                "eligible",
            ),
            (
                make_order(
                    "R-8",
                    "Stripe - expired",
                    "Bankov\u00fdm prevodom",
                    "6",
                    "2026-05-01 10:00:00",
                    last_change="2026-05-20 10:00:00",
                    invoices=[make_invoice()],
                ),
                "final_invoice_present",
            ),
        ]

        for order, expected_reason in cases:
            with self.subTest(order=order["order_num"]):
                self.assertEqual(expected_reason, cancellation_eligibility_reason(order, settings, cutoff))

    def test_recovery_requires_payment_and_fulfillment_evidence(self) -> None:
        settings = self.make_settings()
        value = make_order("R-1", "Stripe - expired", "Bank transfer", "6", "2026-05-01",
                           invoices=[make_invoice()])
        self.assertEqual("no_settlement_evidence", recovery_eligibility_reason(value, settings, creditnote_status="clear"))
        value["invoices"][0]["payments"] = [receipt()]
        self.assertEqual("shipment_history_unavailable", recovery_eligibility_reason(value, settings, creditnote_status="clear"))
        value["shipments"] = [{"shipment_number": "TEST-TRACK", "status": "delivered"}]
        self.assertEqual("eligible", recovery_eligibility_reason(value, settings, creditnote_status="clear"))
        self.assertEqual("creditnote_context_missing", recovery_eligibility_reason(value, settings))
        value["invoices"] = []
        self.assertEqual("missing_final_invoice", recovery_eligibility_reason(value, settings, creditnote_status="clear"))

    def test_runner_dry_run_resolves_target_status_without_mutation(self) -> None:
        project_settings = {
            "unpaid_order_cancellation": {
                "enabled": True,
                "target_status_name": "Nezaplaten\u00e1 - zru\u0161en\u00e1 objedn\u00e1vka",
                "target_status_id": 74,
                "candidate_statuses": ["\u010cak\u00e1 na \u00fahradu"],
                "excluded_statuses": ["Nezaplaten\u00e1 - zru\u0161en\u00e1 objedn\u00e1vka", "Platba online - zaplaten\u00e9"],
                "payment_reference_ids": ["6"],
            }
        }
        client = FakeBizniswebClient(
            [
                [
                    make_order("R-1", "\u010cak\u00e1 na \u00fahradu", "Bankov\u00fdm prevodom", "6", "2026-05-01 10:00:00"),
                    make_order("R-2", "Platba online - zaplaten\u00e9", "Okam\u017eit\u00e1 platba online", "18", "2026-05-01 10:00:00"),
                ]
            ]
        )

        summary = run_unpaid_order_cancellation(
            "roy",
            reference_date="2026-05-27",
            dry_run=True,
            client=client,
            project_settings=project_settings,
        )

        self.assertEqual(74, summary.target_status_id)
        self.assertEqual(2, summary.total_orders_scanned)
        self.assertEqual(1, summary.eligible_orders)
        self.assertEqual(["R-1"], summary.eligible_order_nums)
        self.assertEqual([], client.mutations)

    def test_runner_updates_only_eligible_orders(self) -> None:
        project_settings = {
            "unpaid_order_cancellation": {
                "enabled": True,
                "target_status_name": "Nezaplaten\u00e1 - zru\u0161en\u00e1 objedn\u00e1vka",
                "target_status_id": 74,
                "candidate_statuses": ["\u010cak\u00e1 na \u00fahradu"],
                "excluded_statuses": ["Nezaplaten\u00e1 - zru\u0161en\u00e1 objedn\u00e1vka", "Platba online - zaplaten\u00e9"],
                "payment_reference_ids": ["6"],
            }
        }
        client = FakeBizniswebClient(
            [
                [
                    make_order("R-1", "\u010cak\u00e1 na \u00fahradu", "Bankov\u00fdm prevodom", "6", "2026-05-01 10:00:00"),
                    make_order("R-2", "\u010cak\u00e1 na \u00fahradu", "Dobierkou", "7", "2026-05-01 10:00:00"),
                ]
            ]
        )

        summary = run_unpaid_order_cancellation(
            "roy",
            reference_date="2026-05-27",
            dry_run=False,
            client=client,
            automation_state_store=MemoryAutomationStore(),
            creditnote_context={},
            project_settings=project_settings,
        )

        self.assertEqual(1, summary.updated_orders)
        self.assertEqual(["R-1"], summary.updated_order_nums)
        self.assertEqual([("R-1", 74)], client.mutations)

    def test_runner_recovers_verified_delivered_order_to_shipped(self) -> None:
        project_settings = {
            "unpaid_order_cancellation": {
                "enabled": True,
                "target_status_name": "Nezaplaten\u00e1 - zru\u0161en\u00e1 objedn\u00e1vka",
                "target_status_id": 74,
                "recovery_enabled": True,
                "recovery_target_status_name": "Platba online - zaplaten\u00e9",
                "recovery_target_status_id": 55,
                "recovery_source_statuses": ["Stripe - expired"],
                "candidate_statuses": ["Stripe - expired"],
                "payment_reference_ids": ["6", "18"],
            }
        }
        order = make_order(
            "R-RECOVER",
            "Stripe - expired",
            "Bankov\u00fdm prevodom",
            "6",
            "2026-05-01 10:00:00",
            last_change="2026-05-20 10:00:00",
            invoices=[make_invoice(created="2026-05-02 10:00:00", paid=True)],
        )
        order["shipments"] = [{"shipment_number": "TEST-TRACK", "status": "delivered"}]
        client = FakeBizniswebClient([[order]])

        summary = run_unpaid_order_cancellation(
            "roy",
            reference_date="2026-05-27",
            dry_run=False,
            client=client,
            automation_state_store=MemoryAutomationStore(),
            creditnote_context={},
            project_settings=project_settings,
        )

        self.assertEqual(1, summary.recovery_candidates)
        self.assertEqual(["R-RECOVER"], summary.recovery_candidate_order_nums)
        self.assertEqual(2, summary.rechecked_orders)
        self.assertEqual(1, summary.recovered_orders)
        self.assertEqual(["R-RECOVER"], summary.recovered_order_nums)
        self.assertEqual(0, summary.updated_orders)
        self.assertEqual([("R-RECOVER", 4)], client.mutations)

    def test_live_invoice_recheck_prevents_cancellation_without_inventing_payment(self) -> None:
        project_settings = {
            "unpaid_order_cancellation": {
                "enabled": True,
                "target_status_name": "Nezaplaten\u00e1 - zru\u0161en\u00e1 objedn\u00e1vka",
                "target_status_id": 74,
                "recovery_enabled": True,
                "recovery_target_status_name": "Platba online - zaplaten\u00e9",
                "recovery_target_status_id": 55,
                "recovery_source_statuses": ["Stripe - expired"],
                "candidate_statuses": ["\u010cak\u00e1 na vybavenie", "Stripe - expired"],
                "excluded_statuses": [
                    "Nezaplaten\u00e1 - zru\u0161en\u00e1 objedn\u00e1vka",
                    "Platba online - zaplaten\u00e9",
                    "Odoslan\u00e1",
                ],
                "payment_reference_ids": ["6", "18"],
            }
        }
        listed_order = make_order(
            "R-RACE",
            "\u010cak\u00e1 na vybavenie",
            "Bankov\u00fdm prevodom",
            "6",
            "2026-05-01 10:00:00",
        )
        live_order = make_order(
            "R-RACE",
            "Stripe - expired",
            "Bankov\u00fdm prevodom",
            "6",
            "2026-05-01 10:00:00",
            last_change="2026-05-20 10:00:00",
            invoices=[make_invoice(created="2026-05-02 10:00:00")],
        )
        client = FakeBizniswebClient(
            [[listed_order]],
            detail_sequences={"R-RACE": [listed_order, live_order]},
        )

        summary = run_unpaid_order_cancellation(
            "roy",
            reference_date="2026-05-27",
            dry_run=False,
            client=client,
            automation_state_store=MemoryAutomationStore(),
            creditnote_context={},
            project_settings=project_settings,
        )

        self.assertEqual(1, summary.eligible_orders)
        self.assertEqual(0, summary.recovery_candidates)
        self.assertEqual(0, summary.updated_orders)
        self.assertEqual(0, summary.recovered_orders)
        self.assertEqual(1, summary.review_required_orders)
        self.assertEqual([], client.mutations)

    def test_runner_rejects_partial_graphql_page_before_mutations(self) -> None:
        project_settings = {
            "unpaid_order_cancellation": {
                "enabled": True,
                "target_status_name": "Nezaplaten\u00e1 - zru\u0161en\u00e1 objedn\u00e1vka",
                "target_status_id": 74,
                "candidate_statuses": ["\u010cak\u00e1 na \u00fahradu"],
                "excluded_statuses": ["Nezaplaten\u00e1 - zru\u0161en\u00e1 objedn\u00e1vka"],
                "payment_reference_ids": ["6"],
            }
        }
        client = PartialFirstPageClient(
            [[make_order("R-1", "\u010cak\u00e1 na \u00fahradu", "Bankov\u00fdm prevodom", "6", "2026-05-01 10:00:00")]]
        )

        with self.assertRaises(PartialDataError):
            run_unpaid_order_cancellation(
                "roy", reference_date="2026-05-27", dry_run=False, client=client,
                project_settings=project_settings, automation_state_store=MemoryAutomationStore(),
            )
        self.assertEqual([], client.mutations)

    def test_roy_settings_enable_unpaid_order_cancellation_scheduler(self) -> None:
        project_settings = json.loads((ROOT_DIR / "projects" / "roy" / "settings.json").read_text(encoding="utf-8"))
        settings = resolve_unpaid_cancellation_settings(project_settings)

        self.assertTrue(settings.enabled)
        self.assertEqual(14, settings.age_days)
        self.assertEqual(74, settings.target_status_id)
        self.assertTrue(settings.recovery_enabled)
        self.assertIsNone(settings.recovery_target_status_id)
        self.assertEqual("Platba online - zaplaten\u00e9", settings.recovery_target_status_name)
        self.assertIn("Stripe - expired", settings.recovery_source_statuses)
        self.assertEqual("roy-unpaid-order-cancellation", settings.schedule_name)
        self.assertEqual("cron(10 2 * * ? *)", settings.schedule_expression)
        self.assertIn("6", settings.payment_reference_ids)
        self.assertIn("18", settings.payment_reference_ids)
        self.assertIn("\u010cak\u00e1 na vybavenie", settings.candidate_statuses)
        self.assertNotIn("\u010cak\u00e1 na vybavenie", settings.excluded_statuses)

    def test_legacy_deploy_cannot_bypass_protected_automation_deploy(self) -> None:
        workflow = (ROOT_DIR / ".github" / "workflows" / "deploy-unpaid-order-cancellation.yml").read_text(encoding="utf-8")
        self.assertNotIn("  push:", workflow)
        self.assertNotIn("configure-aws-credentials", workflow)
        self.assertNotIn("aws scheduler", workflow)
        self.assertIn("deploy-order-automations.yml", workflow)


class CancellationSafetyRegressionTests(unittest.TestCase):
    def setUp(self):
        sleep = patch("unpaid_order_cancellation.time.sleep")
        sleep.start()
        self.addCleanup(sleep.stop)

    def settings(self, **overrides):
        return {"unpaid_order_cancellation": {
            "enabled": True, "target_status_id": 74,
            "candidate_statuses": ["Čaká na úhradu"], "payment_reference_ids": ["6"],
            "recovery_enabled": False, **overrides,
        }}

    def order(self, number="R-1"):
        return make_order(number, "Čaká na úhradu", "Bankovým prevodom", "6", "2026-05-01 10:00:00")

    def run_case(self, client, *, settings=None, store=None, dry_run=False, **kwargs):
        return run_unpaid_order_cancellation(
            "roy", reference_date="2026-05-27", dry_run=dry_run, client=client,
            project_settings=settings or self.settings(), automation_state_store=store or MemoryAutomationStore(), **kwargs,
        )

    def test_page_budget_exhaustion_prevents_all_writes(self):
        client = FakeBizniswebClient([[self.order()], [self.order("R-2")]])
        with self.assertRaisesRegex(RuntimeError, "scan limit"):
            self.run_case(client, settings=self.settings(scan_max_pages=1))
        self.assertEqual([], client.mutations)

    def test_repeated_cursor_duplicate_order_and_missing_pagination_fail_closed(self):
        settings = resolve_unpaid_cancellation_settings(self.settings())
        payloads = [
            {"data": [self.order()], "pageInfo": {"hasNextPage": True, "nextCursor": 0}},
            {"data": [self.order(), self.order()], "pageInfo": {"hasNextPage": False}},
            {"data": [self.order()], "pageInfo": {}},
        ]
        for payload in payloads:
            client = FakeBizniswebClient([])
            with patch.object(client, "execute", return_value={"getOrderList": payload}):
                with self.assertRaises(RuntimeError):
                    fetch_orders_for_cancellation(client, settings, [2])
            self.assertEqual([], client.mutations)

    def test_manual_bank_receipt_blocks_cancellation_even_without_final_invoice(self):
        for amount in (10, 100):
            value = self.order()
            value["preinvoices"] = [{"id": "pre-1", "payments": [receipt(amount=amount)]}]
            client = FakeBizniswebClient([[value]])
            summary = self.run_case(client)
            self.assertEqual(0, summary.updated_orders)
            self.assertEqual(1, summary.skipped_by_reason["payment_present"])
            self.assertEqual([], client.mutations)

    def test_missing_payment_field_is_review_failure_not_successful_skip(self):
        value = self.order()
        value.pop("preinvoices")
        client = FakeBizniswebClient([[value]])
        summary = self.run_case(client)
        self.assertEqual(1, summary.failed_orders)
        self.assertEqual(1, summary.review_required_orders)
        self.assertEqual(0, summary.recovery_failed_orders)
        self.assertEqual([], client.mutations)

    def test_new_payment_at_final_recheck_prevents_cancellation(self):
        listed = self.order()
        paid = self.order()
        paid["preinvoices"] = [{"id": "pre-1", "payments": [receipt()]}]
        client = FakeBizniswebClient([[listed]], detail_sequences={"R-1": [listed, paid]})
        summary = self.run_case(client)
        self.assertEqual(1, summary.eligible_orders)
        self.assertEqual(0, summary.updated_orders)
        self.assertEqual(1, summary.recheck_skipped_by_reason["payment_present"])
        self.assertEqual([], client.mutations)

    def test_pending_journal_blocks_replay(self):
        client = FakeBizniswebClient([[self.order()]])
        store = MemoryAutomationStore({"R-1": {"status_mutation": {"state": "pending"}}})
        summary = self.run_case(client, store=store)
        self.assertEqual(1, summary.failed_orders)
        self.assertEqual([], client.mutations)

    def test_failed_readback_persists_uncertainty_and_does_not_replay(self):
        value = self.order()
        client = FakeBizniswebClient([[value]])
        store = MemoryAutomationStore()
        original = client.execute
        def wrong_outcome(query, variable_values=None):
            if "status_id" in (variable_values or {}):
                client.mutations.append((variable_values["order_num"], variable_values["status_id"]))
                return {"changeOrderStatus": {"order_num": "OTHER", "status": {"id": 74, "name": "incorrect"}}}
            return original(query, variable_values)
        with patch.object(client, "execute", side_effect=wrong_outcome):
            first = self.run_case(client, store=store)
            client.page_calls = 0
            second = self.run_case(client, store=store)
        self.assertEqual(1, first.failed_orders)
        self.assertEqual(1, second.failed_orders)
        self.assertEqual("uncertain", store.orders["R-1"]["status_mutation"]["state"])
        self.assertEqual(1, len(client.mutations))

    def test_creditnote_added_at_recovery_recheck_prevents_status_write(self):
        value = self.order()
        value.update(status={"id": 69, "name": "Stripe - expired"}, invoices=[make_invoice(paid=True)],
                     shipments=[{"status": "delivered", "shipment_number": "shipment-1"}])
        client = FakeBizniswebClient([[value]])
        context_calls = []
        def context(project, *, progress_callback=None):
            self.assertTrue(callable(progress_callback))
            progress_callback()
            context_calls.append(project)
            return {} if len(context_calls) == 1 else {"R-1": [{"id": "credit-1"}]}
        settings = self.settings(recovery_enabled=True, recovery_source_statuses=["Stripe - expired"])
        with patch("creditnote_export.fetch_creditnote_automation_context", side_effect=context):
            summary = self.run_case(client, settings=settings)
        self.assertEqual(2, len(context_calls))
        self.assertEqual(1, summary.review_required_orders)
        self.assertEqual([], client.mutations)

    def test_dry_run_never_acquires_mutation_lease(self):
        client = FakeBizniswebClient([[self.order()]])
        store = MemoryAutomationStore()
        summary = self.run_case(client, store=store, dry_run=True)
        self.assertEqual(1, summary.eligible_orders)
        self.assertEqual(0, store.acquired)
        self.assertEqual([], client.mutations)


class CancellationRunnerHealthTests(unittest.TestCase):
    def test_failure_or_incomplete_scan_never_emits_success_in_either_run_mode(self):
        for dry_run in (False, True):
            for failure in ({"failed_orders": 1}, {"scan_limit_reached": True}, {"scan_stop_reason": "incomplete"}):
                summary = UnpaidCancellationSummary(project="roy", enabled=True, dry_run=dry_run,
                    reference_date="2026-05-27", cutoff_date="2026-05-13", target_status_name="Cancelled", **failure)
                self.assert_runner_metrics(summary, dry_run, expect_failure=True)

    def test_completed_scan_reports_success_with_correct_run_mode(self):
        for dry_run in (False, True):
            summary = UnpaidCancellationSummary(project="roy", enabled=True, dry_run=dry_run,
                reference_date="2026-05-27", cutoff_date="2026-05-13", target_status_name="Cancelled", scan_stop_reason="api_exhausted")
            self.assert_runner_metrics(summary, dry_run, expect_failure=False)

    def assert_runner_metrics(self, summary, dry_run, *, expect_failure):
        args = SimpleNamespace(project="roy", reference_date="2026-05-27", timezone="Europe/Bratislava", dry_run=dry_run)
        with patch("unpaid_order_cancellation_runner.load_project_settings", return_value={"unpaid_order_cancellation": {"enabled": True}}), patch(
            "unpaid_order_cancellation_runner.resolve_reporting_defaults", return_value={}
        ), patch("unpaid_order_cancellation_runner.run_unpaid_order_cancellation", return_value=summary), patch(
            "unpaid_order_cancellation_runner.put_metric"
        ) as metric, patch("builtins.print"):
            if expect_failure:
                with self.assertRaises(RuntimeError):
                    run_unpaid_cancellation_runner(args)
            else:
                run_unpaid_cancellation_runner(args)
        names = [call.args[0] for call in metric.call_args_list]
        self.assertEqual(not expect_failure, "UnpaidCancellationRunSucceeded" in names)
        self.assertEqual(expect_failure, "UnpaidCancellationRunFailed" in names)
        for call in metric.call_args_list:
            self.assertEqual("dry-run" if dry_run else "live", call.args[3]["metric_run_mode"])


class CancellationInventoryTests(unittest.TestCase):
    def setUp(self):
        sleep = patch("unpaid_order_cancellation.time.sleep")
        self.sleep = sleep.start()
        self.addCleanup(sleep.stop)
        self.settings = resolve_unpaid_cancellation_settings({"unpaid_order_cancellation": {
            "enabled": True, "page_limit": 2, "page_delay_seconds": 0,
            "candidate_statuses": ["Čaká na úhradu"], "payment_reference_ids": ["6"],
        }})

    def client(self, count=4):
        return FakeBizniswebClient([[make_order(f"synthetic-{i}", "Čaká na úhradu", "Bankovým prevodom", "6",
                                                "2026-01-01 10:00:00") for i in range(1, count + 1)]])

    def scan(self, client, status_ids=(2,), settings=None):
        return fetch_orders_for_cancellation(client, settings or self.settings, status_ids)

    def test_offset_shrink_cannot_silently_skip_a_surviving_order(self):
        client = self.client()
        def remove_earlier_row(client, variables):
            if client.page_calls == 3:
                client.pages[0].pop(0)
        client.before_page_read = remove_earlier_row
        orders, info = self.scan(client)
        self.assertEqual(["synthetic-1", "synthetic-2", "synthetic-3", "synthetic-4"],
                         [row["order_num"] for row in orders])
        self.assertEqual("api_exhausted", info["scan_stop_reason"])
        self.assertTrue(any(call["params"]["limit"] == 1 for call in client.page_requests[1:]))
        self.assertEqual([], client.mutations)

    def test_moving_between_candidate_statuses_cannot_escape_separate_status_loops(self):
        client = self.client(1)
        def move_to_previously_empty_status(client, variables):
            if client.page_calls == 2:
                client.pages[0][0]["status"] = {"id": 1, "name": "Čaká na vybavenie"}
        client.before_page_read = move_to_previously_empty_status
        orders, _ = self.scan(client, status_ids=(1, 2))
        self.assertEqual(["synthetic-1"], [row["order_num"] for row in orders])
        self.assertTrue(all("status" not in call for call in client.page_requests))
        self.assertEqual([], client.mutations)

    def test_local_status_selection_keeps_payment_fields_and_ignores_null_unrelated_status(self):
        client = self.client()
        client.pages[0][1]["status"] = {"id": 4, "name": "Odoslaná"}
        client.pages[0][2]["status"] = None
        client.pages[0][3]["price_elements"] = None
        orders, info = self.scan(client)
        self.assertEqual(["synthetic-1", "synthetic-4"], [row["order_num"] for row in orders])
        self.assertEqual(4, info["inventory_orders_scanned"])
        self.assertEqual("6", orders[0]["price_elements"][0]["reference_id"])
        self.assertIsNone(orders[1]["price_elements"])
        from graphql import print_ast
        from unpaid_order_cancellation import UNPAID_ORDER_QUERY
        query = print_ast(getattr(UNPAID_ORDER_QUERY, "document", UNPAID_ORDER_QUERY))
        self.assertIn("price_elements", query)
        self.assertIn("reference_id", query)
        self.assertIn("include_blocking: true", query)
        self.assertNotIn("$status", query)

    def test_missing_payment_or_status_field_is_incomplete_not_an_unsupported_payment_skip(self):
        for field in ("price_elements", "status"):
            with self.subTest(field=field):
                client = self.client()
                client.pages[0][2].pop(field)
                with self.assertRaisesRegex(RuntimeError, "incomplete"):
                    self.scan(client)
                self.assertEqual([], client.mutations)

    def test_inconsistent_later_page_stops_before_mutation_lease(self):
        client = self.client(5)
        def truncate(client, variables, result):
            if client.page_calls == 3:
                result["getOrderList"]["pageInfo"]["hasNextPage"] = False
        client.after_page_read = truncate
        store = SimpleNamespace(lease=lambda **kwargs: self.fail("incomplete discovery acquired mutation lease"))
        with self.assertRaisesRegex(RuntimeError, "pagination metadata"):
            run_unpaid_order_cancellation("roy", reference_date="2026-05-27", client=client,
                automation_state_store=store, project_settings={"unpaid_order_cancellation": {
                    "enabled": True, "page_limit": 2, "page_delay_seconds": 0,
                    "candidate_statuses": ["Čaká na úhradu"], "payment_reference_ids": ["6"],
                }})
        self.assertEqual([], client.mutations)

    def test_new_upper_ids_are_deferred_and_deleted_upper_anchor_can_finish(self):
        for append in (False, True):
            with self.subTest(append=append):
                client = self.client()
                def change_upper_boundary(client, variables):
                    if client.page_calls == 3:
                        if append:
                            added = deepcopy(client.pages[0][-1])
                            added.update(id="5", order_num="synthetic-5")
                            client.pages[0].append(added)
                        else:
                            client.pages[0].pop()
                client.before_page_read = change_upper_boundary
                rows, _ = self.scan(client)
                self.assertEqual(list(range(1, 5 if append else 4)), [int(row["id"]) for row in rows])

    def test_page_budget_includes_anchor_and_repair_requests(self):
        client = self.client()
        def remove_prefix(client, variables):
            if client.page_calls == 3:
                client.pages[0].pop(0)
        client.before_page_read = remove_prefix
        with self.assertRaisesRegex(RuntimeError, "scan limit"):
            self.scan(client, settings=replace(self.settings, scan_max_pages=3))
        self.assertEqual(3, client.page_calls)
        self.assertEqual([], client.mutations)

    def test_paced_read_retry_is_bounded(self):
        client = self.client(1)
        original = client.execute
        attempts = []
        def temporary_quota(query, variable_values=None):
            attempts.append(True)
            if len(attempts) == 1:
                error = RuntimeError("synthetic quota response")
                error.code = 429
                raise error
            return original(query, variable_values)
        with patch.object(client, "execute", side_effect=temporary_quota):
            rows, _ = self.scan(client, settings=replace(self.settings, page_delay_seconds=2))
        self.assertEqual(1, len(rows))
        waits = [call.args[0] for call in self.sleep.call_args_list]
        self.assertIn(10, waits)
        self.assertTrue(any(0 < delay <= 2 for delay in waits))
        error = RuntimeError("synthetic permanent quota")
        error.code = 429
        with patch.object(client, "execute", side_effect=error) as execute:
            with self.assertRaises(RuntimeError):
                self.scan(client)
        self.assertEqual(4, execute.call_count)

    def test_twenty_minute_deadline_covers_entire_inventory_and_retry_wait(self):
        client = self.client(3)
        elapsed = [0.0]
        def slow_read(client, variables):
            elapsed[0] += 450
        client.before_page_read = slow_read
        with patch("unpaid_order_cancellation.time.monotonic", side_effect=lambda: elapsed[0]):
            with self.assertRaisesRegex(RuntimeError, "time limit"):
                self.scan(client)
        self.assertEqual(3, client.page_calls)
        self.assertEqual([], client.mutations)
        elapsed[0] = 0
        def quota_near_deadline(*args, **kwargs):
            elapsed[0] = 1195
            error = RuntimeError("synthetic quota")
            error.code = 429
            raise error
        self.sleep.reset_mock()
        with patch("unpaid_order_cancellation.time.monotonic", side_effect=lambda: elapsed[0]), \
                patch.object(client, "execute", side_effect=quota_near_deadline):
            with self.assertRaisesRegex(RuntimeError, "time limit"):
                self.scan(client)
        self.sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
