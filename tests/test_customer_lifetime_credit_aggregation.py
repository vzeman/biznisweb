from contextlib import redirect_stdout
from datetime import datetime
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock

import pandas as pd

from export_orders import BizniWebExporter


class CustomerLifetimeCreditAggregationTests(unittest.TestCase):
    def test_item_only_and_precomputed_order_net_apply_credit_once(self):
        for precomputed in (False, True):
            for credit in (0.0, -10.0):
                with self.subTest(precomputed=precomputed, credit=credit), TemporaryDirectory() as folder:
                    exporter = object.__new__(BizniWebExporter)
                    exporter.output_path = lambda filename: Path(folder) / filename
                    exporter.get_daily_fixed_cost = Mock(return_value=0)
                    exporter._build_creditnote_fulfillment_costs_by_date = Mock(return_value=pd.DataFrame())
                    rows = []
                    for order, revenue in [("A", 60.0), ("A", 40.0), ("B", 50.0)]:
                        rows.append({
                            "order_num": order, "customer_email": "synthetic@example.invalid",
                            "purchase_date": "2026-10-01 10:00:00", "product_sku": f"SKU-{len(rows)}",
                            "item_label": "Synthetic", "item_quantity": 1,
                            "item_total_without_tax": revenue, "total_expense": revenue / 2,
                            "profit_before_ads": revenue / 2, "fb_ads_daily_spend": 0.0,
                            "google_ads_daily_spend": 0.0,
                            "order_credit_net_adjustment": credit if order == "A" else 0,
                        })
                    frame = pd.DataFrame(rows)
                    if precomputed:
                        frame["order_revenue_net"] = [100 + credit, 100 + credit, 50]
                        # A supplied canonical column must never be recalculated.
                        exporter.add_order_revenue_net_column = Mock(side_effect=AssertionError("already adjusted"))
                    with redirect_stdout(StringIO()):
                        _, daily, items, _, lifetime = exporter.create_aggregated_reports(
                            frame, datetime(2026, 10, 1), datetime(2026, 10, 1), {}, {},
                        )
                    self.assertEqual(150 + credit, lifetime["ltv_revenue"].sum())
                    self.assertEqual(150 + credit, daily["total_revenue"].sum())
                    self.assertEqual(credit, daily["revenue_credit_adjustment"].sum())
                    self.assertEqual(150, items["total_revenue"].sum())
                    self.assertEqual(2, lifetime["total_lifetime_orders"].sum())
                    self.assertEqual(1, lifetime["customers_acquired"].sum())


if __name__ == "__main__":
    unittest.main()
