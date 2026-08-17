import json
import logging
import os
import random
import time
from datetime import datetime, timezone
import uuid
from kafka import KafkaProducer

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# Configuration from Environment or Defaults
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders")
PRODUCE_INTERVAL_SEC = float(os.getenv("PRODUCE_INTERVAL_SEC", "1.0"))

REGIONS = ["North America", "Europe", "Asia Pacific", "Latin America", "Middle East"]
PRODUCT_CATEGORIES = ["Electronics", "Clothing", "Home & Kitchen", "Books", "Beauty"]

# Price ranges per category to make synthetic data realistic
CATEGORY_PRICE_RANGES = {
    "Electronics": (49.99, 1499.99),
    "Clothing": (15.00, 199.99),
    "Home & Kitchen": (10.00, 499.99),
    "Books": (7.99, 59.99),
    "Beauty": (9.99, 129.99)
}

def generate_order() -> dict:
    """Generates a synthetic e-commerce order record."""
    category = random.choice(PRODUCT_CATEGORIES)
    min_price, max_price = CATEGORY_PRICE_RANGES[category]
    unit_price = round(random.uniform(min_price, max_price), 2)
    quantity = random.randint(1, 10)
    total_amount = round(unit_price * quantity, 2)

    return {
        "order_id": f"ORD-{uuid.uuid4().hex[:8].upper()}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "region": random.choice(REGIONS),
        "product_category": category,
        "amount": total_amount,
        "quantity": quantity
    }

def create_kafka_producer(bootstrap_servers: str, max_retries: int = 10, retry_delay: int = 3) -> KafkaProducer:
    """Creates KafkaProducer with retry logic for initial connection."""
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all"
            )
            logging.info(f"Successfully connected to Kafka at {bootstrap_servers}")
            return producer
        except Exception as e:
            logging.warning(f"Connection attempt {attempt}/{max_retries} failed: {e}")
            if attempt < max_retries:
                time.sleep(retry_delay)
    raise ConnectionError(f"Could not connect to Kafka at {bootstrap_servers} after {max_retries} attempts.")

def main():
    logging.info("Starting Sales Analytics Order Data Producer...")
    producer = create_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)
    
    orders_sent = 0
    try:
        while True:
            order_data = generate_order()
            # Send message using order_id as key for partition routing
            producer.send(
                topic=KAFKA_TOPIC,
                key=order_data["order_id"],
                value=order_data
            )
            orders_sent += 1
            logging.info(
                f"[{orders_sent}] Sent Order {order_data['order_id']} | "
                f"Region: {order_data['region']} | "
                f"Category: {order_data['product_category']} | "
                f"Amount: ${order_data['amount']:.2f} (Qty: {order_data['quantity']})"
            )
            time.sleep(PRODUCE_INTERVAL_SEC)
    except KeyboardInterrupt:
        logging.info("Producer stopped by user (KeyboardInterrupt). Flushed remaining messages.")
    except Exception as e:
        logging.error(f"Unexpected error in producer: {e}", exc_info=True)
    finally:
        producer.flush()
        producer.close()
        logging.info("Kafka Producer closed gracefully.")

if __name__ == "__main__":
    main()
