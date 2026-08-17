import json
import logging
import math
import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Any, Optional

# Ensure workspace root is in python path for internal imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from aggregation.aggregator import (
    OrderAggregator,
    create_kafka_consumer,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    WINDOW_SIZE_SEC
)
from explanation.explainer import AnomalyExplainer

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "sales-analytics-anomaly-detector-group")
Z_THRESHOLD = float(os.getenv("Z_THRESHOLD", "2.0"))
MIN_HISTORICAL_WINDOWS = int(os.getenv("MIN_HISTORICAL_WINDOWS", "5"))


class AnomalyDetector:
    """
    Statistical Anomaly Detector for region-wise sales metrics.
    Uses Z-Score algorithm against historical rolling windows to detect sales spikes and drops.
    Handles cold-start mitigation, identifies top contributing product categories,
    and calls Google Gemini API to generate plain-English explanations.
    """

    def __init__(self, z_threshold: float = Z_THRESHOLD, min_historical_windows: int = MIN_HISTORICAL_WINDOWS, max_history_size: int = 30):
        self.z_threshold = z_threshold
        self.min_historical_windows = min_historical_windows
        self.max_history_size = max_history_size

        # Historical total_sales per region: { region_name: [sales_w1, sales_w2, ...] }
        self.region_history: Dict[str, List[float]] = {}
        # In-memory list storing detected anomaly records (including LLM explanations)
        self.detected_anomalies: List[Dict[str, Any]] = []
        # Initialize LLM Explanation generator module
        self.explainer = AnomalyExplainer()

    def process_window_summary(self, window_summary: dict) -> List[Dict[str, Any]]:
        """
        Evaluates a completed window summary for region-level anomalies using Z-score detection.
        Returns a list of newly detected anomaly dicts.
        """
        window_key = window_summary.get("window_key", "Unknown Window")
        timestamp = window_summary.get("window_start") or window_summary.get("window_end") or window_key
        regions_data = window_summary.get("regions", {})

        newly_detected = []

        for region, metrics in regions_data.items():
            current_sales = float(metrics.get("total_sales", 0.0))
            top_category = metrics.get("top_category", "N/A")

            history = self.region_history.get(region, [])

            # Cold-start check: Need at least min_historical_windows baseline data points
            if len(history) >= self.min_historical_windows:
                mean = sum(history) / len(history)
                variance = sum((x - mean) ** 2 for x in history) / len(history)
                std_dev = math.sqrt(variance)

                if std_dev > 0:
                    z_score = (current_sales - mean) / std_dev
                else:
                    z_score = 0.0

                # Flag anomaly if z-score exceeds threshold (spike or drop)
                if abs(z_score) >= self.z_threshold:
                    anomaly_type = "SPIKE" if z_score > 0 else "DROP"
                    anomaly_record = {
                        "timestamp": timestamp,
                        "window_key": window_key,
                        "region": region,
                        "type": anomaly_type,
                        "current_value": round(current_sales, 2),
                        "historical_avg": round(mean, 2),
                        "std_dev": round(std_dev, 2),
                        "z_score": round(z_score, 2),
                        "affected_category": top_category
                    }

                    # Generate plain-English LLM explanation using Gemini API
                    explanation = self.explainer.explain_anomaly(anomaly_record)
                    anomaly_record["explanation"] = explanation

                    # Store in-memory for downstream access (e.g. Phase 5 RAG interface)
                    self.detected_anomalies.append(anomaly_record)
                    newly_detected.append(anomaly_record)
                    self._print_anomaly_alert(anomaly_record)

            # Update history rolling buffer AFTER evaluation against historical baseline
            if region not in self.region_history:
                self.region_history[region] = []
            self.region_history[region].append(current_sales)

            # Maintain maximum rolling buffer size
            if len(self.region_history[region]) > self.max_history_size:
                self.region_history[region].pop(0)

        return newly_detected

    def _print_anomaly_alert(self, anomaly: dict) -> None:
        """Prints a high-visibility alert banner to the console upon anomaly detection, including LLM explanation."""
        indicator = "[SPIKE ^]" if anomaly["type"] == "SPIKE" else "[DROP v]"
        z_sign = f"+{anomaly['z_score']:.2f}" if anomaly["z_score"] > 0 else f"{anomaly['z_score']:.2f}"

        print("\n" + "!" * 80)
        print(f" {indicator} [ANOMALY DETECTED] Region: {anomaly['region']} | Type: {anomaly['type']} (Z-Score: {z_sign}) ")
        print("!" * 80)
        print(f" Window           : {anomaly['window_key']} UTC")
        print(f" Current Sales    : ${anomaly['current_value']:,.2f}")
        print(f" Historical Avg   : ${anomaly['historical_avg']:,.2f} (StdDev: ${anomaly['std_dev']:,.2f})")
        print(f" Affected Category: {anomaly['affected_category']} (Highest contributor in window)")
        if anomaly.get("explanation"):
            print("-" * 80)
            print(" LLM EXPLANATION & LIKELY CAUSE (Gemini API):")
            for line in anomaly["explanation"].split("\n"):
                print(f"   {line}")
        print("=" * 80 + "\n")

    # --- Query API for downstream modules (e.g. Dashboard / Alerts) ---
    def get_anomalies(self) -> List[Dict[str, Any]]:
        """Returns all detected anomalies."""
        return self.detected_anomalies

    def get_anomalies_by_region(self, region: str) -> List[Dict[str, Any]]:
        """Returns detected anomalies filtered by region."""
        return [a for a in self.detected_anomalies if a["region"] == region]

    def get_latest_anomaly(self) -> Optional[Dict[str, Any]]:
        """Returns the most recently detected anomaly, or None."""
        return self.detected_anomalies[-1] if self.detected_anomalies else None

    def get_region_history(self, region: str) -> List[float]:
        """Returns the historical sales values buffer for a given region."""
        return self.region_history.get(region, [])


def main():
    logging.info(f"Starting Anomaly Detector (Z-Threshold: ±{Z_THRESHOLD}, Min Windows: {MIN_HISTORICAL_WINDOWS})...")
    aggregator = OrderAggregator(window_size_sec=WINDOW_SIZE_SEC)
    detector = AnomalyDetector(z_threshold=Z_THRESHOLD, min_historical_windows=MIN_HISTORICAL_WINDOWS)
    consumer = create_kafka_consumer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, CONSUMER_GROUP)

    print("\n" + "=" * 80)
    print(f" MONITORING FOR REGIONAL SALES ANOMALIES ON TOPIC: '{KAFKA_TOPIC}' ")
    print(f" (Z-Score Threshold: ±{Z_THRESHOLD} | Cold Start: {MIN_HISTORICAL_WINDOWS} windows) ")
    print("=" * 80 + "\n")

    try:
        while True:
            records_by_partition = consumer.poll(timeout_ms=1000)

            for partition, records in records_by_partition.items():
                for record in records:
                    order = record.value
                    aggregator.process_order(order)

            # Close windows and evaluate newly completed ones
            completed_windows = aggregator.check_and_close_windows()
            for window_summary in completed_windows:
                detector.process_window_summary(window_summary)

    except KeyboardInterrupt:
        logging.info("Anomaly Detector stopped by user (KeyboardInterrupt).")
    except Exception as e:
        logging.error(f"Unexpected error in anomaly detector: {e}", exc_info=True)
    finally:
        consumer.close()
        logging.info("Kafka Consumer for Anomaly Detector closed gracefully.")


if __name__ == "__main__":
    main()
