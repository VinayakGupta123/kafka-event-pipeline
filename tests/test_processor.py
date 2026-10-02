import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "consumer"))

from processor import format_summary, new_state, process_event  # noqa: E402


def make_event(order_id, product, quantity, price_cents):
    return {
        "order_id": order_id,
        "sku": "SKU-TEST",
        "product": product,
        "quantity": quantity,
        "price_cents": price_cents,
        "amount_cents": price_cents * quantity,
        "timestamp": 0,
    }


class ProcessorTests(unittest.TestCase):
    def test_single_event_updates_totals(self):
        state = new_state()
        process_event(make_event("o1", "Mouse", 2, 500), state)
        self.assertEqual(state["total_orders"], 1)
        self.assertEqual(state["total_amount_cents"], 1000)

    def test_multiple_events_accumulate(self):
        state = new_state()
        process_event(make_event("o1", "Mouse", 2, 500), state)
        process_event(make_event("o2", "Keyboard", 1, 3000), state)
        self.assertEqual(state["total_orders"], 2)
        self.assertEqual(state["total_amount_cents"], 4000)

    def test_per_product_breakdown(self):
        state = new_state()
        process_event(make_event("o1", "Mouse", 2, 500), state)
        process_event(make_event("o2", "Mouse", 1, 500), state)
        mouse = state["by_product"]["Mouse"]
        self.assertEqual(mouse["orders"], 2)
        self.assertEqual(mouse["quantity"], 3)
        self.assertEqual(mouse["amount_cents"], 1500)

    def test_log_line_contains_running_total(self):
        state = new_state()
        process_event(make_event("o1", "Mouse", 2, 500), state)
        line = process_event(make_event("o2", "Keyboard", 1, 3000), state)
        self.assertIn("running_total_cents=4000", line)

    def test_events_are_processed_in_arrival_order(self):
        # The running total after each call reflects only events seen so far,
        # i.e. events are processed in the order they arrive (message ordering).
        state = new_state()
        line1 = process_event(make_event("o1", "Mouse", 1, 100), state)
        line2 = process_event(make_event("o2", "Mouse", 1, 200), state)
        self.assertIn("running_total_cents=100", line1)
        self.assertIn("running_total_cents=300", line2)

    def test_summary_contains_totals_and_products(self):
        state = new_state()
        process_event(make_event("o1", "Mouse", 2, 500), state)
        summary = format_summary(state)
        self.assertIn("Total orders: 1", summary)
        self.assertIn("Mouse", summary)

    def test_empty_state_summary_has_no_products(self):
        summary = format_summary(new_state())
        self.assertIn("Total orders: 0", summary)


if __name__ == "__main__":
    unittest.main()
