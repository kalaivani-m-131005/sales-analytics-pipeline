import os
import logging
import warnings
from dotenv import load_dotenv, find_dotenv

warnings.filterwarnings("ignore", category=FutureWarning)
logger = logging.getLogger(__name__)

# Search and load dotenv files from current directory, parent directory, or consumer/.env
load_dotenv(find_dotenv(usecwd=True))
if not os.getenv("GEMINI_API_KEY"):
    workspace_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    consumer_env = os.path.join(workspace_root, "consumer", ".env")
    root_env = os.path.join(workspace_root, ".env")
    if os.path.exists(consumer_env):
        load_dotenv(consumer_env)
    elif os.path.exists(root_env):
        load_dotenv(root_env)


class AnomalyExplainer:
    """
    LLM-powered explanation generator for detected sales anomalies using Google Gemini API.
    Converts quantitative anomaly metrics (Z-score, region, spike/drop, category) into plain-English,
    business-friendly diagnostic explanations.
    """

    def __init__(self, model_name: str = None):
        self.api_key = os.getenv("GEMINI_API_KEY")
        self.model_name = model_name or os.getenv("GEMINI_MODEL", "gemini-3.6-flash")
        self._genai = None

        if self.api_key:
            try:
                import google.generativeai as genai
                genai.configure(api_key=self.api_key)
                self._genai = genai
            except ImportError:
                logger.warning("[LLM Explainer] 'google-generativeai' package is not installed. LLM explanations will be disabled.")
            except Exception as e:
                logger.warning(f"[LLM Explainer] Failed to initialize Gemini API: {e}")
        else:
            logger.warning("[LLM Explainer] GEMINI_API_KEY not found in environment variables or .env file.")

    def explain_anomaly(self, anomaly: dict) -> str:
        """
        Generates a plain-English explanation of an anomaly object using Gemini API.
        Handles API errors gracefully and returns a clear fallback message on failure.

        Expected anomaly dict structure:
            - region: str
            - type: str ("SPIKE" or "DROP")
            - current_value: float
            - historical_avg: float
            - std_dev: float
            - z_score: float
            - affected_category: str
            - window_key / timestamp: str
        """
        if not self.api_key:
            return "[Explanation Unavailable: GEMINI_API_KEY not found in environment or .env file]"

        if not self._genai:
            return "[Explanation Unavailable: google-generativeai library missing or failed initialization]"

        region = anomaly.get("region", "Unknown Region")
        anomaly_type = anomaly.get("type", "ANOMALY")
        current_value = anomaly.get("current_value", 0.0)
        historical_avg = anomaly.get("historical_avg", 0.0)
        z_score = anomaly.get("z_score", 0.0)
        category = anomaly.get("affected_category", "N/A")
        timestamp = anomaly.get("timestamp") or anomaly.get("window_key", "Recent Window")

        z_desc = f"+{z_score:.2f}" if z_score > 0 else f"{z_score:.2f}"

        prompt = f"""You are a senior revenue operations and sales data analyst.
An anomaly has been detected in real-time regional sales streaming data:

- Region Affected: {region}
- Anomaly Type: {anomaly_type} (Sales {anomaly_type.lower()} detected)
- Current Sales Volume: ${current_value:,.2f}
- Historical Baseline Average: ${historical_avg:,.2f}
- Deviation Z-Score: {z_desc} (Threshold: ±2.0)
- Primary Product Category Contributor: {category}
- Window Timestamp: {timestamp}

Write a concise, professional, plain-English executive explanation (2 to 4 sentences).
1. Highlight which region was affected, whether sales spiked or dropped, the magnitude (z-score), and the key product category driving the change.
2. Provide a plausible, business-friendly diagnosis of the root cause (e.g., targeted promotional surge, viral ad campaign, inventory stockout, payment processing outage, or regional demand shift).
Keep the tone natural, concise, and executive-ready."""

        candidate_models = [
            self.model_name,
            "gemini-3.6-flash",
            "gemini-flash-latest",
            "gemini-2.5-flash",
            "gemini-1.5-flash"
        ]
        # Deduplicate while preserving order
        unique_models = []
        for m in candidate_models:
            if m and m not in unique_models:
                unique_models.append(m)

        last_error = None
        for model_id in unique_models:
            try:
                model = self._genai.GenerativeModel(model_id)
                response = model.generate_content(prompt)
                if response and response.text:
                    return response.text.strip()
            except Exception as e:
                last_error = e
                logger.warning(f"[LLM Explainer Warning] Gemini model '{model_id}' failed: {e}. Trying fallback...")
                continue

        return f"[Explanation Unavailable: API call failed - {last_error}]"
