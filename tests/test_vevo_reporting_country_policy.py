"""Verified Hungarian COD identity must follow the existing realization policy."""
import unittest

from tests.test_report_status_identity import exporter, order


class VevoCountryPolicyTests(unittest.TestCase):
    def test_verified_country_cod_methods_share_realization_policy(self):
        exp = exporter()
        exp.prepare_reporting_status_identity()
        for payment in ("7", "10", "16"):
            for status, label in (("1", "New order"), ("4", "Shipped")):
                with self.subTest(payment=payment, status=status):
                    self.assertTrue(exp._realized_revenue_decision(order(status, label, payment))[0])

    def test_hungarian_cod_does_not_rescue_cancelled_expired_or_unpaid_orders(self):
        exp = exporter()
        exp.prepare_reporting_status_identity()
        for status, label in (("17", "Cancelled"), ("33", "Payment online - expired"),
                              ("34", "Payment online - expired"), ("69", "Stripe - unpaid")):
            with self.subTest(status=status):
                self.assertFalse(exp._realized_revenue_decision(order(status, label, "16"))[0])

    def test_unknown_or_missing_payment_metadata_does_not_become_cod(self):
        exp = exporter()
        exp.prepare_reporting_status_identity()
        for payment in ("999", ""):
            with self.subTest(payment=payment):
                self.assertFalse(exp._realized_revenue_decision(order(payment=payment))[0])
        missing = order(payment="16")
        missing.pop("price_elements")
        self.assertFalse(exp._realized_revenue_decision(missing)[0])


if __name__ == "__main__":
    unittest.main()
