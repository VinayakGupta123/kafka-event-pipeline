import os
import sys
import json
import time
from confluent_kafka import Producer
from events import generate_order_event

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
EVENT_INTERVAL = float(os.getenv("EVENT_INTERVAL_SECONDS", "1.0"))
NUM_EVENTS = int(os.getenv("NUM_EVENTS", "0"))

config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'client.id': 'order-producer-client',
    'acks': 'all',                      # Guarantee high durability
    'retries': 5,
    'retry.backoff.ms': 500
}

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}", file=sys.stderr)
    else:
        print(f"Sent: {msg.key().decode('utf-8')} -> Partition {msg.partition()}")

print("Initializing Producer...")
producer = Producer(config)
topic = "orders"
events_sent = 0

try:
    while True:
        event = generate_order_event()
        key = event["product"] # Keying by product guarantees partition-level ordering
        payload = json.dumps(event).encode('utf-8')
        
        producer.produce(
            topic=topic,
            key=key.encode('utf-8'),
            value=payload,
            callback=delivery_report
        )
        producer.poll(0)
        
        events_sent += 1
        if NUM_EVENTS > 0 and events_sent >= NUM_EVENTS:
            print(f"Reached Target Batch Limit ({NUM_EVENTS}). Closing down...")
            break
            
        time.sleep(EVENT_INTERVAL)

except KeyboardInterrupt:
    print("\nStopping producer application via user interrupt...")

finally:
    print("Flushing transient buffers...")
    producer.flush(timeout=10.0)
