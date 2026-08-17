# 📊 Real-Time Sales Analytics Pipeline with Explainable Anomaly Detection using LLM-Powered RAG

> Most dashboards tell you **WHAT** happened. This system tells you **WHY**.

## 🌐 Live Demo
**Dashboard:** https://sales-analytics-kalaivani.streamlit.app/

## 📌 Project Overview
Modern e-commerce businesses generate continuous streams of transactional data, yet most dashboards only report what happened — a spike or drop in sales — without explaining why. This project addresses that gap by building a real-time analytics pipeline that:
- Ingests live order data via **Apache Kafka**
- Detects anomalies using **statistical Z-score analysis**
- Generates **LLM-powered plain-English explanations** for each anomaly
- Allows **natural language querying** via a RAG interface
- Visualizes everything in an **interactive Streamlit dashboard**

---

## 🏗️ System ArchitectureLive Orders → Kafka Producer → Kafka Topic ("orders")
↓
Windowed Aggregation (30s)
↓
Z-Score Anomaly Detection
↓
Gemini LLM Explanation Module
↓
RAG Query Interface (ChromaDB)
↓
Streamlit Dashboard (Live UI)---

## ⚡ Tech Stack

| Component | Technology |
|---|---|
| Real-Time Streaming | Apache Kafka + Zookeeper (Docker) |
| Data Producer | Python (kafka-python-ng) |
| Aggregation | Python (Tumbling Windows, 30s) |
| Anomaly Detection | Z-Score Statistical Analysis |
| LLM Explanation | Google Gemini API |
| RAG Interface | Keyword Retrieval + Gemini LLM |
| Dashboard | Streamlit + Plotly |
| Containerization | Docker Compose |
| Deployment | Streamlit Community Cloud |

---

## 📁 Project Structure
sales-analytics-pipeline/
├── producer/
│ └── producer.py # Kafka order data producer
├── consumer/
│ └── consumer.py # Kafka consumer (testing)
├── aggregation/
│ └── aggregator.py # 30s tumbling window aggregation
├── anomaly_detection/
│ └── detector.py # Z-score anomaly detector + LLM integration
├── explanation/
│ ├── explainer.py # Gemini LLM explanation module
│ └── rag_query.py # RAG-based NL query interface
├── dashboard/
│ └── app.py # Streamlit dashboard
├── docker-compose.yml # Kafka + Zookeeper setup
├── requirements.txt
└── anomaly_history.json # Persistent anomaly store
---

## 🚀 How to Run Locally

### Prerequisites
- Python 3.11+
- Docker Desktop
- Google Gemini API Key (free at https://aistudio.google.com)

### Step 1: Clone the Repository
```bash
git clone https://github.com/kalaivani-m-131005/sales-analytics-pipeline.git
cd sales-analytics-pipeline
```

### Step 2: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 3: Set Up Environment Variables
Create a `.env` file in the root directory:
### Step 4: Start Kafka (Docker)
```bash
docker-compose up -d
```
Wait 30 seconds for Kafka to fully start.

### Step 5: Run the Pipeline (3 separate terminals)

**Terminal 1 — Producer:**
```bash
python producer/producer.py
```

**Terminal 2 — Anomaly Detector:**
```bash
python anomaly_detection/detector.py
```

**Terminal 3 — Dashboard:**
```bash
streamlit run dashboard/app.py
```

### Step 6: Query Interface (Optional)
```bash
python explanation/rag_query.py
```

---

## 📊 Features

### ✅ Real-Time Kafka Streaming
- Simulates live e-commerce orders across 5 regions and 5 product categories
- Streams continuously to Kafka topic "orders"

### ✅ Statistical Anomaly Detection
- 30-second tumbling windows for region-wise aggregation
- Z-score detection with cold-start guard (minimum 5 windows)
- Detects both SPIKE (z > +2.0) and DROP (z < -2.0)

### ✅ LLM-Powered Explanations
- Google Gemini API generates business-friendly explanations
- Identifies affected category, magnitude, and likely business cause

### ✅ RAG Query Interface
- Natural language questions about anomaly history
- Keyword-based retrieval + Gemini LLM for context-grounded answers

### ✅ Interactive Dashboard
- Real-time metrics (total anomalies, spikes, drops, regions affected)
- Bar chart, pie chart, Z-score scatter plot
- Anomaly log with LLM explanations
- Integrated RAG query interface

---

## 🔍 Sample Anomaly Output
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
[SPIKE ^] [ANOMALY DETECTED] Region: Latin America | Type: SPIKE (Z-Score: +6.91)
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
Window : 07:27:30 - 07:28:00 UTC
Current Sales : $11,706.16
Historical Avg : $3,387.43 (StdDev: $1,203.70)
Affected Category: Electronics

LLM EXPLANATION (Gemini API):
Real-time monitoring identified an extraordinary sales spike in Latin
America reaching $11,706.16 — a +6.91 z-score deviation from the
$3,387.43 historical baseline, driven primarily by the Electronics
category. This surge is likely the result of a highly successful
promotional campaign or bulk B2B purchase.
---

## 🔮 Future Scope
- Replace simulated stream with production Kafka cluster (Confluent Cloud)
- Scale aggregation with Apache Spark or Flink
- Upgrade RAG with vector database (ChromaDB/Pinecone)
- Add PostgreSQL/TimescaleDB for persistent time-series storage
- Integrate Prometheus/Grafana for system monitoring
- Expand to multi-tenant, multi-product line analytics

---

## 👩‍💻 Author
**Kalaivani M**
- GitHub: [@kalaivani-m-131005](https://github.com/kalaivani-m-131005)
- Live Demo: https://sales-analytics-kalaivani.streamlit.app/

---

## 📄 License
This project is open source and available under the [MIT License](LICENSE).

---

*Built with ❤️ as a Final Year Major Project*
