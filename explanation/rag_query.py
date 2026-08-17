import os, json, logging
try:
    import google.generativeai as genai
    from dotenv import load_dotenv
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
ANOMALY_HISTORY_FILE = os.path.join(os.getcwd(), "anomaly_history.json")

class AnomalyRAGQuery:
    def __init__(self):
        self.model = None
        if GEMINI_AVAILABLE:
            env_path = os.path.join(os.getcwd(), ".env")
            load_dotenv(dotenv_path=env_path)
            api_key = os.getenv("GEMINI_API_KEY")
            if api_key:
                try:
                    genai.configure(api_key=api_key)
                    self.model = genai.GenerativeModel("models/gemini-3.6-flash")
                except Exception as e:
                    logger.warning("RAG Gemini init failed: " + str(e))
            else:
                logger.warning("RAG: GEMINI_API_KEY not found")

    def load_anomaly_history(self):
        if not os.path.exists(ANOMALY_HISTORY_FILE):
            return []
        try:
            with open(ANOMALY_HISTORY_FILE, "r", encoding="utf-8-sig") as f:
                return json.load(f)
        except Exception as e:
            logger.warning("RAG Could not read anomaly history: " + str(e))
            return []

    def retrieve_relevant_anomalies(self, question, anomalies, max_results=5):
        question_lower = question.lower()
        scored = []
        for anomaly in anomalies:
            score = 0
            if str(anomaly.get("region","")).lower() in question_lower: score += 3
            if str(anomaly.get("affected_category","")).lower() in question_lower: score += 2
            if str(anomaly.get("type","")).lower() in question_lower: score += 2
            if "drop" in question_lower and str(anomaly.get("type","")).lower() == "drop": score += 2
            if "spike" in question_lower and str(anomaly.get("type","")).lower() == "spike": score += 2
            if score > 0: scored.append((score, anomaly))
        scored.sort(key=lambda x: x[0], reverse=True)
        relevant = [a for s,a in scored[:max_results]]
        if not relevant: relevant = anomalies[-max_results:]
        return relevant

    def answer_question(self, question):
        anomalies = self.load_anomaly_history()
        if not anomalies:
            return "No anomalies recorded yet. Run the pipeline first."
        relevant = self.retrieve_relevant_anomalies(question, anomalies)
        context = "\n".join([
            "- [" + str(a.get("timestamp","")) + "] Region: " + str(a.get("region","")) +
            " | Type: " + str(a.get("type","")) +
            " | Z-Score: " + str(a.get("z_score","")) +
            " | Sales: $" + str(a.get("current_value","")) +
            " | Avg: $" + str(a.get("historical_avg","")) +
            " | Category: " + str(a.get("affected_category","")) +
            " | Explanation: " + str(a.get("explanation","N/A"))
            for a in relevant
        ])
        if not self.model:
            return "Gemini unavailable. Raw data:\n" + context
        prompt = "You are a business analytics assistant. Answer based ONLY on these anomaly records:\n\n" + context + "\n\nQuestion: " + question + "\n\nAnswer:"
        try:
            return self.model.generate_content(prompt).text.strip()
        except Exception as e:
            return "API error: " + str(e) + "\n\nRaw data:\n" + context

def save_anomaly_to_history(anomaly_dict):
    history = []
    if os.path.exists(ANOMALY_HISTORY_FILE):
        try:
            with open(ANOMALY_HISTORY_FILE, "r", encoding="utf-8-sig") as f:
                history = json.load(f)
        except: pass
    history.append(anomaly_dict)
    with open(ANOMALY_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2, default=str)

if __name__ == "__main__":
    print("=" * 60)
    print(" ANOMALY QUERY INTERFACE (type exit to quit)")
    print("=" * 60)
    rag = AnomalyRAGQuery()
    while True:
        question = input("\nAsk a question: ").strip()
        if question.lower() in ("exit","quit"): break
        if not question: continue
        print("\nThinking...\n")
        print("-" * 60)
        print(rag.answer_question(question))
        print("-" * 60)

