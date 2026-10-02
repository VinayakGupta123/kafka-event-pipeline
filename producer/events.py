import uuid
import random
from datetime import datetime, timezone

CATALOG = {
    "Laptop": 99999,
    "Smartphone": 49999,
    "Headphones": 14999,
    "Smartwatch": 19999,
    "Keyboard": 8999
}

def generate_order_event():
    product = random.choice(list(CATALOG.keys()))
    price_cents = CATALOG[product]
    quantity = random.randint(1, 5)
    amount_cents = price_cents * quantity
    
    return {
        "order_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "product": product,
        "quantity": quantity,
        "price_cents": price_cents,
        "amount_cents": amount_cents
    }
