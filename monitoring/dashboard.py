import os
import pandas as pd
import streamlit as st
import plotly.express as px

from streamlit_autorefresh import st_autorefresh

st.set_page_config(
    
    page_title="Real-Time Fraud Detection Dashboard",
    layout="wide"
)
st_autorefresh(interval=5000, key="refresh")

st.title("💳 Real-Time Fraud Detection Dashboard")

LOG_FILE = "logs/prediction_logs.csv"

if not os.path.exists(LOG_FILE):
    st.error("Prediction log file not found!")
    st.stop()

df = pd.read_csv(LOG_FILE)

# Convert confidence to numeric
df["Confidence"] = pd.to_numeric(df["Confidence"], errors="coerce")

# ---------------- KPI Cards ----------------
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Total Predictions", len(df))

with col2:
    fraud = len(df[df["Prediction"] == "Fraud"])
    st.metric("Fraud Predictions", fraud)

with col3:
    legit = len(df[df["Prediction"] == "Legitimate"])
    st.metric("Legitimate Predictions", legit)

st.divider()

col4, col5 = st.columns(2)

with col4:
    st.metric("Average Confidence", f"{df['Confidence'].mean():.2f}")

with col5:
    fraud_rate = (fraud / len(df) * 100) if len(df) else 0
    st.metric("Fraud Rate", f"{fraud_rate:.2f}%")

st.divider()

st.subheader("Prediction Logs")
st.dataframe(df, use_container_width=True)
st.divider()

st.header("📈 Analytics")

col1, col2 = st.columns(2)

# Pie Chart
with col1:
    fig = px.pie(
        df,
        names="Prediction",
        title="Fraud vs Legitimate Predictions"
    )
    st.plotly_chart(fig, use_container_width=True)

# Confidence Histogram
with col2:
    fig = px.histogram(
        df,
        x="Confidence",
        nbins=10,
        title="Confidence Distribution"
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# Prediction Timeline
df["Timestamp"] = pd.to_datetime(df["Timestamp"])

trend = (
    df.groupby("Timestamp")
      .size()
      .reset_index(name="Predictions")
)

fig = px.line(
    trend,
    x="Timestamp",
    y="Predictions",
    markers=True,
    title="Prediction Trend"
)

st.plotly_chart(fig, use_container_width=True)