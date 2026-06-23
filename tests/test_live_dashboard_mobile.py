import unittest

from live_dashboard_server import build_production_board_html


class LiveDashboardMobileTests(unittest.TestCase):
    def test_production_board_contains_mobile_products_layout(self):
        html = build_production_board_html("vevo")

        self.assertIn('data-marker="vevo-production-board"', html)
        self.assertIn('class="table-wrap desktop-products"', html)
        self.assertIn('id="productsCards"', html)
        self.assertIn(".products-cards", html)
        self.assertIn("product-card", html)
        self.assertIn("@media (max-width:680px)", html)
        self.assertIn("const apiUrl = (path) => new URL(path, window.location.origin).toString();", html)
        self.assertIn("fetchApi(path, { cache: 'no-store' })", html)
        self.assertNotIn("await fetch(url,", html)


if __name__ == "__main__":
    unittest.main()
