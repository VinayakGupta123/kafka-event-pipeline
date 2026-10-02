import json

class OrderProcessor:
    def __init__(self):
        self.total_orders = 0
        self.total_amount_cents = 0
        self.product_breakdown = {}

    def process_event(self, event_dict):
        product = event_dict["product"]
        amount = event_dict["amount_cents"]
        
        self.total_orders += 1
        self.total_amount_cents += amount
        
        if product not in self.product_breakdown:
            self.product_breakdown[product] = {"count": 0, "amount_cents": 0}
            
        self.product_breakdown[product]["count"] += 1
        self.product_breakdown[product]["amount_cents"] += amount
        
        return (
            f"ORDER processed: {event_dict['order_id']} | Product: {product} | "
            f"Amount: ${amount/100:.2f} | Running Total: ${self.total_amount_cents/100:.2f}"
        )

    def generate_summary(self):
        summary = [
            "\n==========================================",
            "          FINAL PIPELINE SUMMARY          ",
            "==========================================",
            f"Total Processed Orders : {self.total_orders}",
            f"Total Revenue Generated: ${self.total_amount_cents/100:.2f}",
            "------------------------------------------",
            "Breakdown Per Product Profile:"
        ]
        for prod, stats in sorted(self.product_breakdown.items()):
            summary.append(
                f"  - {prod}: {stats['count']} items sold | "
                f"Total Segment Value: ${stats['amount_cents']/100:.2f}"
            )
        summary.append("==========================================\n")
        return "\n".join(summary)
