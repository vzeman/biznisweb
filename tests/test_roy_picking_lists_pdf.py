import unittest

from roy_picking_lists_pdf import build_roy_picking_lists_filename, build_roy_picking_lists_pdf


class RoyPickingListsPdfTests(unittest.TestCase):
    def test_builds_single_pdf_for_fulfillable_orders(self) -> None:
        orders = [
            {
                "order_num": "2677009999",
                "purchase_at": "2026-05-27 10:00:00",
                "status": "Platba online - zaplaten\u00e9",
                "sum": "120,00 EUR",
                "payment": {"title": "Okam\u017eit\u00e1 platba online"},
                "shipping": {"title": "Packeta"},
                "items": [
                    {
                        "label": "Fotopasca Wachman Rio 4G",
                        "quantity": 1,
                        "import_code": "F_1472",
                        "ean": "",
                    },
                    {
                        "label": "Univerz\u00e1lne sol\u00e1rne nap\u00e1janie BL8000 pre fotopascu",
                        "quantity": 1,
                        "import_code": "F_486",
                        "ean": "",
                    },
                ],
            }
        ]

        pdf = build_roy_picking_lists_pdf(orders)
        filename = build_roy_picking_lists_filename(orders)

        self.assertTrue(pdf.startswith(b"%PDF-"))
        self.assertGreater(len(pdf), 1500)
        self.assertTrue(filename.startswith("roy-vyskladnovacie-listy-1-"))
        self.assertTrue(filename.endswith(".pdf"))

    def test_empty_pdf_is_still_downloadable(self) -> None:
        pdf = build_roy_picking_lists_pdf([])

        self.assertTrue(pdf.startswith(b"%PDF-"))
        self.assertGreater(len(pdf), 1000)


if __name__ == "__main__":
    unittest.main()
