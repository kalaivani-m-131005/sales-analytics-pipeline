import json
import logging
import os
import time
from kafka import KafkaConsumer

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# Configuration from Environment or Defaults
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "sales-analytics-group")

def create_kafka_consumer(bootstrap_servers: str, topic: str, group_id: str, max_retries: int = 10, retry_delay: int = 3) -> KafkaConsumer:
    """Creates KafkaConsumer with retry logic for initial connection."""
    for attempt in range(1, max_retries + 1):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None
            )
            logging.info(f"Successfully connected Kafka consumer to '{bootstrap_servers}' on topic '{topic}'")
            return consumer
        except Exception as e:
            logging.warning(f"Consumer connection attempt {attempt}/{max_retries} failed: {e}")
            if attempt < max_retries:
                time.sleep(retry_delay)
    raise ConnectionError(f"Could not connect Kafka consumer to {bootstrap_servers} after {max_retries} attempts.")

def main():
    logging.info("Starting Sales Analytics Order Data Consumer...")
    consumer = create_kafka_consumer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, CONSUMER_GROUP)

    print("\n" + "="*80)
    print(f" LISTENING FOR LIVE ORDERS ON KAFKA TOPIC: '{KAFKA_TOPIC}' ")
    print("="*80 + "\n")

    message_count = 0
    try:
        for message in consumer:
            message_count += 1
            order = message.value
            partition = message.partition
            offset = message.offset

            print(
                f"[Msg #{message_count:05d} | Partition {partition} | Offset {offset}] "
                f"ID: {order.get('order_id')} | "
                f"Time: {order.get('timestamp')} | "
                f"Region: {order.get('region'):<15} | "
                f"Category: {order.get('product_category'):<15} | "
                f"Amount: ${order.get('amount'):>8.2f} | "
                f"Qty: {order.get('quantity'):>2}"
            )
    except KeyboardInterrupt:
        logging.info("Consumer stopped by user (KeyboardInterrupt).")
    except Exception as e:
        logging.error(f"Unexpected error in consumer: {e}", exc_info=True)
    finally:
        consumer.close()
        logging.info("Kafka Consumer closed gracefully.")

if __name__ == "__main__":
    main()
