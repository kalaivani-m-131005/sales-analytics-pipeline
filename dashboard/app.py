import streamlit as st
import json
import os
import sys
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

st.set_page_config(
    page_title="Real-Time Sales Analytics",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Real-Time Sales Analytics Pipeline")
st.markdown("**Explainable Anomaly Detection with LLM-Powered RAG**")
st.divider()

ANOMALY_FILE = os.path.join(os.path.dirname(__file__), "..", "anomaly_history.json")

def load_anomalies():
    if not os.path.exists(ANOMALY_FILE):
        return []
    try:
        with open(ANOMALY_FILE, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except:
        return []

anomalies = load_anomalies()

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Anomalies", len(anomalies))
with col2:
    spikes = len([a for a in anomalies if a.get("type") == "SPIKE"])
    st.metric("Spikes Detected", spikes, delta=f"+{spikes}", delta_color="inverse")
with col3:
    drops = len([a for a in anomalies if a.get("type") == "DROP"])
    st.metric("Drops Detected", drops, delta=f"-{drops}", delta_color="normal")
with col4:
    regions = len(set([a.get("region","") for a in anomalies]))
    st.metric("Regions Affected", regions)

st.divider()

if anomalies:
    df = pd.DataFrame(anomalies)

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("🔍 Anomalies by Region")
        region_counts = df["region"].value_counts().reset_index()
        region_counts.columns = ["Region", "Count"]
        fig1 = px.bar(region_counts, x="Region", y="Count",
                     color="Region", title="Anomaly Count by Region")
        st.plotly_chart(fig1, use_container_width=True)

    with col_right:
        st.subheader("📈 Spike vs Drop Distribution")
        type_counts = df["type"].value_counts().reset_index()
        type_counts.columns = ["Type", "Count"]
        fig2 = px.pie(type_counts, names="Type", values="Count",
                     color="Type",
                     color_discrete_map={"SPIKE": "#ff4444", "DROP": "#4444ff"},
                     title="Anomaly Type Distribution")
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("📊 Z-Score by Region")
    fig3 = px.scatter(df, x="region", y="z_score",
                     color="type", size="current_value",
                     hover_data=["affected_category", "historical_avg"],
                     color_discrete_map={"SPIKE": "#ff4444", "DROP": "#4444ff"},
                     title="Z-Score Distribution Across Regions")
    fig3.add_hline(y=2, line_dash="dash", line_color="orange", annotation_text="Z=+2 threshold")
    fig3.add_hline(y=-2, line_dash="dash", line_color="orange", annotation_text="Z=-2 threshold")
    st.plotly_chart(fig3, use_container_width=True)

    st.subheader("🚨 Anomaly Log")
    for i, anomaly in enumerate(reversed(anomalies)):
        anomaly_type = anomaly.get("type", "")
        icon = "🔴" if anomaly_type == "SPIKE" else "🔵"
        with st.expander(f"{icon} {anomaly.get('region')} — {anomaly_type} (Z-Score: {anomaly.get('z_score')}) | {anomaly.get('timestamp')}"):
            col_a, col_b = st.columns(2)
            with col_a:
                st.metric("Current Sales", f"${anomaly.get('current_value'):,.2f}")
                st.metric("Historical Avg", f"${anomaly.get('historical_avg'):,.2f}")
            with col_b:
                st.metric("Z-Score", anomaly.get("z_score"))
                st.metric("Top Category", anomaly.get("affected_category","N/A"))
            if anomaly.get("explanation"):
                st.markdown("**💡 LLM Explanation:**")
                st.info(anomaly.get("explanation"))

    st.divider()
    st.subheader("🤖 Ask About Anomalies (RAG Query)")
    
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    if os.path.exists(env_path):
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=env_path)
    
    api_key = os.getenv("GEMINI_API_KEY")
    if api_key:
        os.environ["GEMINI_API_KEY"] = api_key
    
    question = st.text_input("Ask a question about sales anomalies:", 
                              placeholder="e.g. Why did sales spike in Latin America?")
    if st.button("Ask") and question:
        with st.spinner("Thinking..."):
            try:
                from explanation.rag_query import AnomalyRAGQuery
                rag = AnomalyRAGQuery()
                answer = rag.answer_question(question)
                st.success(answer)
            except Exception as e:
                st.error(f"Error: {str(e)}")
else:
    st.warning("No anomalies detected yet. Run the pipeline first!")
    st.info("Steps to generate data:\n1. Start Docker: `docker-compose up -d`\n2. Run producer: `python producer/producer.py`\n3. Run detector: `python anomaly_detection/detector.py`\n4. Wait 5-10 minutes for anomalies to appear, then refresh this page.")

st.divider()
st.caption("Real-Time Sales Analytics Pipeline | Kafka + Z-Score + Gemini LLM + RAG")
