import os
import sys
import json
from confluent_kafka import Consumer, KafkaError, KafkaException
from processor import OrderProcessor

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
EXPECTED_EVENTS = int(os.getenv("EXPECTED_EVENTS", "0"))
OUTPUT_PATH = "output/orders.log"

config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': 'order-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

processor = OrderProcessor()
consumer = Consumer(config)
events_processed = 0

try:
    consumer.subscribe(["orders"])
    print("📥 Ingestion consumer online. Awaiting broker transmission...")
    
    # Ensure target output buffer directory structures are intact
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())
                
        # Handle inbound payload
        raw_payload = msg.value().decode('utf-8')
        event_data = json.loads(raw_payload)
        
        log_line = processor.process_event(event_data)
        print(log_line)
        
        with open(OUTPUT_PATH, "a") as log_file:
            log_file.write(log_line + "\n")
            
        events_processed += 1
        if EXPECTED_EVENTS > 0 and events_processed >= EXPECTED_EVENTS:
            print(f"Target verification watermark reached ({EXPECTED_EVENTS}). Generating final logs...")
            summary_output = processor.generate_summary()
            print(summary_output)
            with open(OUTPUT_PATH, "a") as log_file:
                log_file.write(summary_output)
            break

except KeyboardInterrupt:
    print("\nGracefully severing consumer cluster configuration links...")
finally:
    consumer.close()
