import unittest
from io import BytesIO

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
                "customer": {"display_name": "B2B Partner s.r.o."},
                "customer_note": "Objednavku pripravit k osobnemu odberu.",
                "invoice_address": {"lines": ["B2B Partner s.r.o.", "Hlavna 12", "81101 Bratislava Slovensko"]},
                "delivery_address": {"lines": ["Sklad B2B", "Skladova 5", "91701 Trnava Slovensko"]},
                "wholesale_pricing": {"is_wholesale": True, "max_discount_pct": 20.0},
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

    def test_pdf_text_contains_customer_note_wholesale_badge_and_order_number(self) -> None:
        try:
            from pypdf import PdfReader
        except ImportError:
            self.skipTest("pypdf is only used for local PDF text verification")

        pdf = build_roy_picking_lists_pdf(
            [
                {
                    "order_num": "2677009999",
                    "purchase_at": "2026-05-27 10:00:00",
                    "status": "Platba online - zaplaten\u00e9",
                    "sum": "120,00 EUR",
                    "payment": {"title": "Okam\u017eit\u00e1 platba online"},
                    "shipping": {"title": "Osobn\u00fd odber na sklade"},
                    "customer": {"display_name": "B2B Partner s.r.o."},
                    "customer_note": "Objednavku pripravit v piatok doobeda.",
                    "invoice_address": {"lines": ["B2B Partner s.r.o.", "Hlavna 12", "81101 Bratislava Slovensko"]},
                    "delivery_address": {"lines": ["B2B Partner s.r.o.", "Hlavna 12", "81101 Bratislava Slovensko"]},
                    "wholesale_pricing": {"is_wholesale": True, "max_discount_pct": 20.0},
                    "items": [{"label": "Fotopasca Wachman Solar Pro", "quantity": 1, "import_code": "12474", "ean": "8586024430013"}],
                }
            ]
        )

        text = PdfReader(BytesIO(pdf)).pages[0].extract_text() or ""

        self.assertIn("2677009999", text)
        self.assertIn("Pozn\u00e1mka klienta", text)
        self.assertIn("Objednavku pripravit v piatok doobeda.", text)
        self.assertIn("VE\u013dKOOBCHOD / VO CENY", text)
        self.assertIn("B2B Partner s.r.o.", text)

    def test_empty_pdf_is_still_downloadable(self) -> None:
        pdf = build_roy_picking_lists_pdf([])

        self.assertTrue(pdf.startswith(b"%PDF-"))
        self.assertGreater(len(pdf), 1000)


if __name__ == "__main__":
    unittest.main()
