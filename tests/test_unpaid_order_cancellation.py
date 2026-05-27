import json
import unittest
from datetime import date
from pathlib import Path

from unpaid_order_cancellation import (
    cancellation_eligibility_reason,
    resolve_unpaid_cancellation_settings,
    run_unpaid_order_cancellation,
)


ROOT_DIR = Path(__file__).resolve().parents[1]


def price_element(kind: str, title: str, reference_id: str = "") -> dict:
    return {
        "type": kind,
        "title": title,
        "reference_id": reference_id,
        "value": "",
        "price": {"value": 0, "formatted": "0,00 EUR"},
    }


def make_order(order_num: str, status_name: str, payment_title: str, payment_ref: str, pur_date: str) -> dict:
    return {
        "id": order_num,
        "order_num": order_num,
        "pur_date": pur_date,
        "last_change": pur_date,
        "status": {"id": 1, "name": status_name},
        "price_elements": [price_element("payment", payment_title, payment_ref)],
        "sum": {"value": 100, "formatted": "100,00 EUR"},
    }


class FakeBizniswebClient:
    def __init__(self, pages, statuses=None):
        self.pages = list(pages)
        self.statuses = statuses or [{"id": 74, "name": "Nezaplaten\u00e1 - zru\u0161en\u00e1 objedn\u00e1vka"}]
        self.page_calls = 0
        self.mutations = []

    def execute(self, query, variable_values=None):  # noqa: ANN001 - mimics gql.Client
        variables = variable_values or {}
        if "lang_code" in variables:
            return {"listOrderStatuses": self.statuses}
        if "order_num" in variables and "status_id" in variables:
            self.mutations.append((variables["order_num"], variables["status_id"]))
            return {
                "changeOrderStatus": {
                    "order_num": variables["order_num"],
                    "status": {"id": variables["status_id"], "name": self.statuses[0]["name"]},
                }
            }
        if "params" in variables:
            index = self.page_calls
            self.page_calls += 1
            data = self.pages[index] if index < len(self.pages) else []
            has_next = index + 1 < len(self.pages)
            return {
                "getOrderList": {
                    "data": data,
                    "pageInfo": {
                        "hasNextPage": has_next,
                        "nextCursor": str(index + 1) if has_next else None,
                        "pageIndex": index + 1,
                        "totalPages": len(self.pages),
                    },
                }
            }
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
    def make_settings(self):
        return resolve_unpaid_cancellation_settings(
            {
                "unpaid_order_cancellation": {
                    "enabled": True,
                    "target_status_name": "Nezaplaten\u00e1 - zru\u0161en\u00e1 objedn\u00e1vka",
                    "target_status_id": 74,
                    "candidate_statuses": [
                        "\u010cak\u00e1 na \u00fahradu",
                        "Platba online - platnos\u0165 vypr\u0161ala",
                    ],
                    "excluded_statuses": [
                        "Nezaplaten\u00e1 - zru\u0161en\u00e1 objedn\u00e1vka",
                        "Platba online - zaplaten\u00e9",
                        "\u010cak\u00e1 na vybavenie",
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
                "excluded_status",
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
        ]

        for order, expected_reason in cases:
            with self.subTest(order=order["order_num"]):
                self.assertEqual(expected_reason, cancellation_eligibility_reason(order, settings, cutoff))

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
            project_settings=project_settings,
        )

        self.assertEqual(1, summary.updated_orders)
        self.assertEqual(["R-1"], summary.updated_order_nums)
        self.assertEqual([("R-1", 74)], client.mutations)

    def test_runner_uses_partial_order_pages_from_biznisweb_errors(self) -> None:
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

        summary = run_unpaid_order_cancellation(
            "roy",
            reference_date="2026-05-27",
            dry_run=True,
            client=client,
            project_settings=project_settings,
        )

        self.assertEqual(1, summary.total_orders_scanned)
        self.assertEqual(1, summary.eligible_orders)

    def test_roy_settings_enable_unpaid_order_cancellation_scheduler(self) -> None:
        project_settings = json.loads((ROOT_DIR / "projects" / "roy" / "settings.json").read_text(encoding="utf-8"))
        settings = resolve_unpaid_cancellation_settings(project_settings)

        self.assertTrue(settings.enabled)
        self.assertEqual(14, settings.age_days)
        self.assertEqual(74, settings.target_status_id)
        self.assertEqual("roy-unpaid-order-cancellation", settings.schedule_name)
        self.assertEqual("cron(10 2 * * ? *)", settings.schedule_expression)
        self.assertIn("6", settings.payment_reference_ids)
        self.assertIn("18", settings.payment_reference_ids)


if __name__ == "__main__":
    unittest.main()
