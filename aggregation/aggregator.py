import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional
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
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "sales-analytics-aggregator-group")
WINDOW_SIZE_SEC = int(os.getenv("WINDOW_SIZE_SEC", "30"))


class OrderAggregator:
    """
    Real-time windowed aggregator for streaming order data.
    Computes region-wise metrics (total sales, total quantity, order count, average order value)
    over tumbling time windows (default 30 seconds).
    Stores results in an in-memory dictionary structure accessible by downstream modules.
    """

    def __init__(self, window_size_sec: int = WINDOW_SIZE_SEC):
        self.window_size_sec = window_size_sec
        # Open window buffer: window_start_epoch -> region -> metric aggregations
        self.active_windows: Dict[int, Dict[str, Dict[str, Any]]] = {}
        # In-memory storage for completed window summaries
        # Format: { window_key_str: { "window_start": ..., "window_end": ..., "regions": { region: metrics }, "total": summary } }
        self.completed_windows: Dict[str, Dict[str, Any]] = {}
        # Quick reference to the latest completed window metrics
        self.latest_completed_metrics: Dict[str, Any] = {}

    def _get_window_start(self, timestamp_epoch: float) -> int:
        """Returns the epoch timestamp of the start of the tumbling window."""
        return int(timestamp_epoch // self.window_size_sec) * self.window_size_sec

    def process_order(self, order: dict, event_time_epoch: Optional[float] = None) -> None:
        """
        Processes a single order record and updates internal active window statistics.
        """
        region = order.get("region", "Unknown")
        amount = float(order.get("amount", 0.0))
        quantity = int(order.get("quantity", 0))

        # Parse timestamp from order or fallback to event_time_epoch / current time
        if event_time_epoch is None:
            ts_str = order.get("timestamp")
            if ts_str:
                try:
                    dt = datetime.fromisoformat(ts_str)
                    event_time_epoch = dt.timestamp()
                except Exception:
                    event_time_epoch = time.time()
            else:
                event_time_epoch = time.time()

        window_start = self._get_window_start(event_time_epoch)

        if window_start not in self.active_windows:
            self.active_windows[window_start] = {}

        category = order.get("product_category", "Unknown")

        if region not in self.active_windows[window_start]:
            self.active_windows[window_start][region] = {
                "total_sales": 0.0,
                "total_quantity": 0,
                "order_count": 0,
                "categories": {}
            }

        stats = self.active_windows[window_start][region]
        stats["total_sales"] += amount
        stats["total_quantity"] += quantity
        stats["order_count"] += 1
        stats["categories"][category] = stats["categories"].get(category, 0.0) + amount

    def check_and_close_windows(self, current_time_epoch: Optional[float] = None) -> list:
        """
        Closes windows whose end time is less than or equal to current_time_epoch.
        Calculates final metrics (including average order value), updates in-memory dictionary,
        and prints completed window summary. Returns list of completed window data objects.
        """
        if current_time_epoch is None:
            current_time_epoch = time.time()

        closed_window_starts = []
        for window_start in list(self.active_windows.keys()):
            window_end = window_start + self.window_size_sec
            # Close window if current time is past window_end
            if current_time_epoch >= window_end:
                closed_window_starts.append(window_start)

        closed_window_starts.sort()
        newly_completed = []

        for window_start in closed_window_starts:
            window_end = window_start + self.window_size_sec
            raw_regions = self.active_windows.pop(window_start)

            start_dt = datetime.fromtimestamp(window_start, tz=timezone.utc)
            end_dt = datetime.fromtimestamp(window_end, tz=timezone.utc)

            window_key = f"{start_dt.strftime('%H:%M:%S')} - {end_dt.strftime('%H:%M:%S')}"

            region_metrics = {}
            grand_sales = 0.0
            grand_qty = 0
            grand_orders = 0

            for region, stats in sorted(raw_regions.items()):
                total_sales = round(stats["total_sales"], 2)
                total_qty = stats["total_quantity"]
                order_count = stats["order_count"]
                avg_val = round(total_sales / order_count, 2) if order_count > 0 else 0.0
                categories = stats.get("categories", {})
                top_category = max(categories, key=categories.get) if categories else "N/A"
                cat_sales = {cat: round(amt, 2) for cat, amt in categories.items()}

                region_metrics[region] = {
                    "total_sales": total_sales,
                    "total_quantity": total_qty,
                    "order_count": order_count,
                    "avg_order_value": avg_val,
                    "top_category": top_category,
                    "category_sales": cat_sales
                }

                grand_sales += total_sales
                grand_qty += total_qty
                grand_orders += order_count

            grand_avg_val = round(grand_sales / grand_orders, 2) if grand_orders > 0 else 0.0

            window_summary = {
                "window_key": window_key,
                "window_start": start_dt.isoformat(),
                "window_end": end_dt.isoformat(),
                "regions": region_metrics,
                "overall": {
                    "total_sales": round(grand_sales, 2),
                    "total_quantity": grand_qty,
                    "order_count": grand_orders,
                    "avg_order_value": grand_avg_val
                }
            }

            # Update in-memory structures
            self.completed_windows[window_key] = window_summary
            self.latest_completed_metrics = window_summary

            # Print to console
            self._print_window_summary(window_summary)
            newly_completed.append(window_summary)

        return newly_completed

    def _print_window_summary(self, window_summary: dict) -> None:
        """Formats and prints the window summary to the console."""
        window_key = window_summary["window_key"]
        regions = window_summary["regions"]
        overall = window_summary["overall"]

        print("\n" + "=" * 80)
        print(f" COMPLETED TIME WINDOW: {window_key} UTC ")
        print("=" * 80)
        print(f"{'Region':<18} | {'Total Sales':>12} | {'Total Qty':>10} | {'Orders':>8} | {'Avg Order Value':>15}")
        print("-" * 80)

        for region, metrics in regions.items():
            print(
                f"{region:<18} | "
                f"${metrics['total_sales']:>11.2f} | "
                f"{metrics['total_quantity']:>10d} | "
                f"{metrics['order_count']:>8d} | "
                f"${metrics['avg_order_value']:>14.2f}"
            )

        print("-" * 80)
        print(
            f"{'TOTAL':<18} | "
            f"${overall['total_sales']:>11.2f} | "
            f"{overall['total_quantity']:>10d} | "
            f"{overall['order_count']:>8d} | "
            f"${overall['avg_order_value']:>14.2f}"
        )
        print("=" * 80 + "\n")

    # --- Query API for downstream modules (e.g., Anomaly Detection) ---
    def get_latest_metrics(self) -> dict:
        """Returns the metrics dictionary for the most recently completed window."""
        return self.latest_completed_metrics

    def get_completed_windows(self) -> dict:
        """Returns the full dictionary of all stored completed window metrics."""
        return self.completed_windows

    def get_region_metrics(self, region: str) -> Optional[dict]:
        """Returns latest metrics for a specific region if available."""
        latest = self.get_latest_metrics()
        return latest.get("regions", {}).get(region)


def create_kafka_consumer(bootstrap_servers: str, topic: str, group_id: str, max_retries: int = 10, retry_delay: int = 3) -> KafkaConsumer:
    """Creates KafkaConsumer with retry logic for initial connection."""
    for attempt in range(1, max_retries + 1):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None
            )
            logging.info(f"Successfully connected Aggregator Kafka consumer to '{bootstrap_servers}' on topic '{topic}'")
            return consumer
        except Exception as e:
            logging.warning(f"Consumer connection attempt {attempt}/{max_retries} failed: {e}")
            if attempt < max_retries:
                time.sleep(retry_delay)
    raise ConnectionError(f"Could not connect Kafka consumer to {bootstrap_servers} after {max_retries} attempts.")


def main():
    logging.info(f"Starting Real-Time Aggregator (Window Size: {WINDOW_SIZE_SEC}s)...")
    aggregator = OrderAggregator(window_size_sec=WINDOW_SIZE_SEC)
    consumer = create_kafka_consumer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, CONSUMER_GROUP)

    print("\n" + "=" * 80)
    print(f" AGGREGATING LIVE ORDERS FROM KAFKA TOPIC: '{KAFKA_TOPIC}' ({WINDOW_SIZE_SEC}s Windows) ")
    print("=" * 80 + "\n")

    try:
        while True:
            # Poll Kafka for records with a 1-second timeout
            records_by_partition = consumer.poll(timeout_ms=1000)

            for partition, records in records_by_partition.items():
                for record in records:
                    order = record.value
                    aggregator.process_order(order)

            # Check if any window boundaries have been crossed
            aggregator.check_and_close_windows()

    except KeyboardInterrupt:
        logging.info("Aggregator stopped by user (KeyboardInterrupt).")
    except Exception as e:
        logging.error(f"Unexpected error in aggregator: {e}", exc_info=True)
    finally:
        consumer.close()
        logging.info("Kafka Consumer for Aggregator closed gracefully.")


if __name__ == "__main__":
    main()
