import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "producer"))

from events import PRODUCTS, generate_event  # noqa: E402


class EventTests(unittest.TestCase):
    def test_event_has_expected_fields(self):
        event = generate_event()
        for field in ("order_id", "sku", "product", "quantity",
                      "price_cents", "amount_cents", "timestamp"):
            self.assertIn(field, event)

    def test_amount_is_price_times_quantity(self):
        event = generate_event()
        self.assertEqual(event["amount_cents"], event["price_cents"] * event["quantity"])

    def test_quantity_in_valid_range(self):
        for _ in range(50):
            event = generate_event()
            self.assertGreaterEqual(event["quantity"], 1)
            self.assertLessEqual(event["quantity"], 5)

    def test_product_matches_known_catalog(self):
        known_names = {name for _, name, _ in PRODUCTS}
        event = generate_event()
        self.assertIn(event["product"], known_names)

    def test_order_ids_are_unique(self):
        ids = {generate_event()["order_id"] for _ in range(50)}
        self.assertEqual(len(ids), 50)


if __name__ == "__main__":
    unittest.main()
