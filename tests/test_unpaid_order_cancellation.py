import json
import unittest
from datetime import date
from pathlib import Path

from unpaid_order_cancellation import (
    cancellation_eligibility_reason,
    recovery_eligibility_reason,
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
        "status": {"id": 1, "name": status_name},
        "price_elements": [price_element("payment", payment_title, payment_ref)],
        "invoices": list(invoices or []),
        "sum": {"value": 100, "formatted": "100,00 EUR"},
    }


class FakeBizniswebClient:
    def __init__(self, pages, statuses=None, detail_overrides=None, detail_sequences=None):
        self.pages = list(pages)
        self.statuses = statuses or [
            {"id": 74, "name": "Nezaplaten\u00e1 - zru\u0161en\u00e1 objedn\u00e1vka"},
            {"id": 4, "name": "Odoslan\u00e1"},
            {"id": 1, "name": "\u010cak\u00e1 na vybavenie"},
            {"id": 2, "name": "\u010cak\u00e1 na \u00fahradu"},
            {"id": 68, "name": "Platba online - platnos\u0165 vypr\u0161ala"},
            {"id": 69, "name": "Stripe - expired"},
        ]
        self.detail_overrides = detail_overrides or {}
        self.detail_sequences = {key: list(value) for key, value in (detail_sequences or {}).items()}
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
                    "recovery_enabled": True,
                    "recovery_target_status_name": "Odoslan\u00e1",
                    "recovery_target_status_id": 4,
                    "recovery_source_statuses": ["Stripe - expired"],
                    "recovery_payment_reference_ids": ["6"],
                    "recovery_payment_title_patterns": ["Bankov\u00fdm prevodom"],
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

    def test_recovery_requires_preexisting_invoice_and_payment_resolution_evidence(self) -> None:
        settings = self.make_settings()
        cases = [
            (
                make_order(
                    "R-1",
                    "Stripe - expired",
                    "Bankov\u00fdm prevodom",
                    "6",
                    "2026-05-01 10:00:00",
                    last_change="2026-05-20 10:00:00",
                    invoices=[make_invoice(created="2026-05-02 10:00:00")],
                ),
                "eligible",
            ),
            (
                make_order(
                    "R-2",
                    "Stripe - expired",
                    "Okam\u017eit\u00e1 platba online",
                    "18",
                    "2026-05-01 10:00:00",
                    last_change="2026-05-20 10:00:00",
                    invoices=[make_invoice(created="2026-05-02 10:00:00", paid=True)],
                ),
                "eligible",
            ),
            (
                make_order(
                    "R-3",
                    "Stripe - expired",
                    "Okam\u017eit\u00e1 platba online",
                    "18",
                    "2026-05-01 10:00:00",
                    last_change="2026-05-20 10:00:00",
                    invoices=[make_invoice(created="2026-05-02 10:00:00")],
                ),
                "missing_payment_resolution_evidence",
            ),
            (
                make_order(
                    "R-4",
                    "Stripe - expired",
                    "Bankov\u00fdm prevodom",
                    "6",
                    "2026-05-01 10:00:00",
                    last_change="2026-05-20 10:00:00",
                    invoices=[make_invoice(created="2026-05-21 10:00:00")],
                ),
                "final_invoice_not_preexisting",
            ),
            (
                make_order(
                    "R-5",
                    "Stripe - expired",
                    "Bankov\u00fdm prevodom",
                    "6",
                    "2026-05-01 10:00:00",
                    last_change="2026-05-20 10:00:00",
                ),
                "missing_final_invoice",
            ),
        ]

        for order, expected_reason in cases:
            with self.subTest(order=order["order_num"]):
                self.assertEqual(expected_reason, recovery_eligibility_reason(order, settings))

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

    def test_runner_recovers_resolved_stripe_expired_order_to_shipped(self) -> None:
        project_settings = {
            "unpaid_order_cancellation": {
                "enabled": True,
                "target_status_name": "Nezaplaten\u00e1 - zru\u0161en\u00e1 objedn\u00e1vka",
                "target_status_id": 74,
                "recovery_enabled": True,
                "recovery_target_status_name": "Odoslan\u00e1",
                "recovery_target_status_id": 4,
                "recovery_source_statuses": ["Stripe - expired"],
                "recovery_payment_reference_ids": ["6"],
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
            invoices=[make_invoice(created="2026-05-02 10:00:00")],
        )
        client = FakeBizniswebClient([[order]])

        summary = run_unpaid_order_cancellation(
            "roy",
            reference_date="2026-05-27",
            dry_run=False,
            client=client,
            project_settings=project_settings,
        )

        self.assertEqual(1, summary.recovery_candidates)
        self.assertEqual(["R-RECOVER"], summary.recovery_candidate_order_nums)
        self.assertEqual(2, summary.rechecked_orders)
        self.assertEqual(1, summary.recovered_orders)
        self.assertEqual(["R-RECOVER"], summary.recovered_order_nums)
        self.assertEqual(0, summary.updated_orders)
        self.assertEqual([("R-RECOVER", 4)], client.mutations)

    def test_live_recheck_prefers_recovery_over_planned_cancellation(self) -> None:
        project_settings = {
            "unpaid_order_cancellation": {
                "enabled": True,
                "target_status_name": "Nezaplaten\u00e1 - zru\u0161en\u00e1 objedn\u00e1vka",
                "target_status_id": 74,
                "recovery_enabled": True,
                "recovery_target_status_name": "Odoslan\u00e1",
                "recovery_target_status_id": 4,
                "recovery_source_statuses": ["Stripe - expired"],
                "recovery_payment_reference_ids": ["6"],
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
            project_settings=project_settings,
        )

        self.assertEqual(1, summary.eligible_orders)
        self.assertEqual(0, summary.recovery_candidates)
        self.assertEqual(0, summary.updated_orders)
        self.assertEqual(1, summary.recovered_orders)
        self.assertEqual([("R-RACE", 4)], client.mutations)

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
        self.assertTrue(settings.recovery_enabled)
        self.assertEqual(4, settings.recovery_target_status_id)
        self.assertEqual("Odoslan\u00e1", settings.recovery_target_status_name)
        self.assertIn("Stripe - expired", settings.recovery_source_statuses)
        self.assertIn("6", settings.recovery_payment_reference_ids)
        self.assertEqual("roy-unpaid-order-cancellation", settings.schedule_name)
        self.assertEqual("cron(10 2 * * ? *)", settings.schedule_expression)
        self.assertIn("6", settings.payment_reference_ids)
        self.assertIn("18", settings.payment_reference_ids)
        self.assertIn("\u010cak\u00e1 na vybavenie", settings.candidate_statuses)
        self.assertNotIn("\u010cak\u00e1 na vybavenie", settings.excluded_statuses)

    def test_deploy_waits_for_the_exact_merge_image(self) -> None:
        build_workflow = (
            ROOT_DIR / ".github" / "workflows" / "build-and-push-ecr.yml"
        ).read_text(encoding="utf-8")
        deploy_workflow = (
            ROOT_DIR / ".github" / "workflows" / "deploy-unpaid-order-cancellation.yml"
        ).read_text(encoding="utf-8")

        exact_tag = 'COMMIT_IMAGE_TAG="${COMMIT_IMAGE_TAG_PREFIX}${GITHUB_SHA}"'
        self.assertIn(exact_tag, build_workflow)
        self.assertIn('docker push "$COMMIT_IMAGE_URI"', build_workflow)
        self.assertIn("ECR_EXACT_IMAGE_OK", build_workflow)
        self.assertIn('IMAGE_TAG="${COMMIT_IMAGE_TAG_PREFIX}${GITHUB_SHA}"', deploy_workflow)
        self.assertIn("Waiting for exact ECR image tag", deploy_workflow)
        self.assertIn("ECR_EXACT_IMAGE_RESOLVED", deploy_workflow)
        self.assertLess(
            deploy_workflow.index("ECR_EXACT_IMAGE_RESOLVED"),
            deploy_workflow.index("aws ecs register-task-definition"),
        )


if __name__ == "__main__":
    unittest.main()
