import os
import time
import streamlit as st
import pandas as pd
import plotly.express as px

# === 0️⃣ PAGE CONFIG ===
st.set_page_config(
    page_title="Twitter Sentiment Control Center",
    layout="wide",
    page_icon="📈"
)

st.title("📈 Real-time Twitter Sentiment Control Center")
st.markdown("##### Live analytics powered by Kafka + Spark Structured Streaming (Enhanced Dashboard 🚀)")

# === 1️⃣ DATA PATH ===
DATA_DIR = "Output/step3_stream_output"
os.makedirs(DATA_DIR, exist_ok=True)

# === 2️⃣ LOAD LATEST DATA (recursive) ===
@st.cache_data(ttl=5)
def load_latest_data():
    all_csv = []
    for root, _, files in os.walk(DATA_DIR):
        for f in files:
            if f.endswith(".csv"):
                all_csv.append(os.path.join(root, f))
    if not all_csv:
        return pd.DataFrame(columns=["ID", "Topic", "Sentiment", "Text", "Timestamp", "IngestedAt", "TextLength"])
    latest = max(all_csv, key=os.path.getmtime)
    df = pd.read_csv(latest)
    df["BatchFile"] = os.path.basename(latest)
    return df

# === 3️⃣ STREAMING LOOP ===
placeholder = st.empty()
progress = st.progress(0)

refresh_interval = 10  # giây

while True:
    df = load_latest_data()

    with placeholder.container():
        if df.empty:
            st.warning("⏳ Waiting for streaming data from Spark...")
            time.sleep(5)
            continue

        # --- Thống kê ---
        total_tweets = len(df)
        pos_count = int(df[df["Sentiment"] == "Positive"].shape[0])
        neu_count = int(df[df["Sentiment"] == "Neutral"].shape[0])
        neg_count = int(df[df["Sentiment"] == "Negative"].shape[0])
        avg_len = df["TextLength"].mean().round(2) if "TextLength" in df else 0

        # --- Header stats ---
        st.markdown("## 🌍 System Overview")
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("💬 Total Tweets", f"{total_tweets:,}")
        col2.metric("😊 Positive", pos_count)
        col3.metric("😐 Neutral", neu_count)
        col4.metric("😠 Negative", neg_count)
        col5.metric("✍️ Avg Length", avg_len)

        st.markdown(f"**📦 Latest Batch:** `{df['BatchFile'].iloc[0]}` — _{time.strftime('%Y-%m-%d %H:%M:%S')}_")
        st.markdown("---")

        # === CHARTS ===
        sentiment_dist = df["Sentiment"].value_counts().reset_index()
        sentiment_dist.columns = ["Sentiment", "Count"]

        top_topics = (
            df["Topic"].value_counts().head(5).reset_index()
            .rename(columns={"index": "Topic", "Topic": "Count"})
        )

        # sentiment distribution
        fig_sent = px.pie(
            sentiment_dist,
            names="Sentiment",
            values="Count",
            title="🎭 Sentiment Distribution",
            color_discrete_sequence=px.colors.qualitative.Safe,
        )

        # top trending topics
        fig_topic = px.bar(
            top_topics,
            x="Topic",
            y="Count",
            color="Count",
            title="🔥 Top 5 Trending Topics",
            color_continuous_scale="Bluered_r",
        )

        # tweet length histogram
        if "TextLength" in df:
            fig_len = px.histogram(
                df,
                x="TextLength",
                nbins=25,
                title="✍️ Tweet Length Distribution",
                color_discrete_sequence=["#00CC96"]
            )
        else:
            fig_len = px.histogram(title="✍️ Tweet Length Distribution (No TextLength field)")

        colA, colB, colC = st.columns(3)
        colA.plotly_chart(fig_sent, use_container_width=True)
        colB.plotly_chart(fig_topic, use_container_width=True)
        colC.plotly_chart(fig_len, use_container_width=True)

        st.markdown("---")

        # === Timeline chart (new feature) ===
        if "Timestamp" in df.columns and not df["Timestamp"].isna().all():
            try:
                df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
                df_time = (
                    df.groupby([pd.Grouper(key="Timestamp", freq="1min"), "Sentiment"])
                    .size().reset_index(name="Count")
                )
                fig_time = px.line(
                    df_time,
                    x="Timestamp",
                    y="Count",
                    color="Sentiment",
                    title="🕒 Sentiment Trend Over Time (Realtime)",
                    markers=True
                )
                st.plotly_chart(fig_time, use_container_width=True)
            except Exception as e:
                st.error(f"⚠️ Unable to plot timeline: {e}")

        # === Latest tweets table ===
        st.markdown("### 🕊️ Latest Tweets")
        st.dataframe(
            df.tail(20)[["ID", "Topic", "Sentiment", "Text", "Timestamp"]],
            use_container_width=True,
            height=400
        )

        st.markdown(
            "<div style='text-align:center; color:gray;'>Auto-refresh every 10 seconds ⏱️</div>",
            unsafe_allow_html=True
        )

    for i in range(refresh_interval):
        progress.progress(int((i + 1) / refresh_interval * 100))
        time.sleep(1)
    progress.progress(0)
