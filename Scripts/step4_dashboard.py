import os
import time
import streamlit as st
import pandas as pd
import plotly.express as px

# === 0️⃣ PAGE CONFIG ===
st.set_page_config(
    page_title="Twitter Sentiment Control Center",
    layout="wide",
    page_icon="📈",
)

st.title("📈 Real-time Twitter Sentiment Control Center")
st.markdown(
    "##### Live analytics powered by Kafka + Spark Structured Streaming (Enhanced Dashboard 🚀)"
)

# === 1️⃣ DATA PATH ===
DATA_DIR = "Output/step3_stream_output"
os.makedirs(DATA_DIR, exist_ok=True)

REFRESH_INTERVAL = 10  # giây


# === 2️⃣ LOAD LATEST DATA (recursive) ===
@st.cache_data(ttl=REFRESH_INTERVAL)
def load_latest_data():
    all_csv = []
    for root, _, files in os.walk(DATA_DIR):
        for f in files:
            if f.endswith(".csv"):
                all_csv.append(os.path.join(root, f))

    # Không có file nào -> DataFrame rỗng với schema chuẩn
    if not all_csv:
        return pd.DataFrame(
            columns=[
                "ID",
                "Topic",
                "Sentiment",
                "Text",
                "Timestamp",
                "IngestedAt",
                "TextLength",
            ]
        )

    latest = max(all_csv, key=os.path.getmtime)
    df = pd.read_csv(
        latest,
        encoding="utf-8",
        engine="python",
        on_bad_lines="skip",
    )
    df["BatchFile"] = os.path.basename(latest)
    return df


def main():
    df = load_latest_data()

    # === SESSION STATE CHO PAUSE KHI ALERT ===
    if "alert_active" not in st.session_state:
        st.session_state["alert_active"] = False
    if "pause_on_alert" not in st.session_state:
        st.session_state["pause_on_alert"] = True  # mặc định bật

    # Sidebar config
    st.sidebar.markdown("## ⚙️ Cấu hình giám sát")
    st.session_state["pause_on_alert"] = st.sidebar.checkbox(
        "⏸ Tự dừng dashboard khi có topic nguy hiểm",
        value=st.session_state["pause_on_alert"],
        help=(
            "Khi bật, nếu có topic có tỷ lệ Negative > 50% thì dashboard sẽ "
            "tạm ngừng auto-refresh để bạn kiểm tra."
        ),
    )

    placeholder = st.empty()
    progress = st.progress(0)

    with placeholder.container():
        if df.empty:
            st.warning("⏳ Waiting for streaming data from Spark...")
        else:
            # === OVERVIEW STATS ===
            total_tweets = len(df)
            pos_count = (df["Sentiment"] == "Positive").sum()
            neu_count = (df["Sentiment"] == "Neutral").sum()
            neg_count = (df["Sentiment"] == "Negative").sum()
            avg_len = df["TextLength"].mean().round(2) if "TextLength" in df else 0

            # === TOPIC-LEVEL STATS ===
            topic_stats = (
                df.groupby("Topic")["Sentiment"]
                .agg(
                    Total="size",
                    Pos=lambda s: (s == "Positive").sum(),
                    Neu=lambda s: (s == "Neutral").sum(),
                    Neg=lambda s: (s == "Negative").sum(),
                )
                .reset_index()
                .set_index("Topic")
            )
            topic_stats["NegRatio"] = topic_stats["Neg"] / topic_stats["Total"]
            NEG_RATIO_THRESHOLD = 0.5

            risk_topics = (
                topic_stats[topic_stats["NegRatio"] > NEG_RATIO_THRESHOLD]
                .sort_values("NegRatio", ascending=False)
            )

            # === OVERVIEW UI ===
            st.markdown("## 🌍 System Overview")
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("💬 Total Tweets", f"{total_tweets:,}")
            col2.metric("😊 Positive", int(pos_count))
            col3.metric("😐 Neutral", int(neu_count))
            col4.metric("😠 Negative", int(neg_count))
            col5.metric("✍️ Avg Length", avg_len)

            st.markdown(
                f"**📦 Latest Batch:** `{df['BatchFile'].iloc[0]}` — "
                f"_{time.strftime('%Y-%m-%d %H:%M:%S')}_"
            )
            st.markdown("---")

            # === GLOBAL NEGATIVE ALERT ===
            if total_tweets > 0:
                global_neg_ratio = neg_count / total_tweets
                if global_neg_ratio > NEG_RATIO_THRESHOLD:
                    st.error(
                        "🚨 CẢNH BÁO TOÀN HỆ THỐNG: "
                        f"Tỷ lệ Negative quá cao ({global_neg_ratio:.1%})!"
                    )

            # === TOPIC RISK ALERTS + CẬP NHẬT alert_active ===
            if not risk_topics.empty:
                st.warning(
                    f"⚠️ Có {len(risk_topics)} topic có tỷ lệ Negative vượt quá "
                    f"{NEG_RATIO_THRESHOLD*100:.0f}% – cần theo dõi kỹ:"
                )
                for topic_name, row in risk_topics.iterrows():
                    st.markdown(
                        f"- **{topic_name}** → Negative: {row['Neg']}/{row['Total']} "
                        f"({row['NegRatio']:.1%})"
                    )

                if st.session_state["pause_on_alert"]:
                    st.session_state["alert_active"] = True
            else:
                st.info("✅ Không có topic nguy hiểm.")
                st.session_state["alert_active"] = False

            # === GỢI Ý ỨNG DỤNG THỰC TẾ ===
            st.markdown(
                """
                **🧠 Ứng dụng thực tế:**

                - Các topic có tỷ lệ Negative cao thường là dấu hiệu **khách hàng không hài lòng**
                  với một chủ đề cụ thể (chất lượng sản phẩm, giao hàng, lỗi hệ thống,...).
                - Dashboard này có thể dùng cho **Marketing / CSKH** để:
                  - Ưu tiên xử lý các topic tiêu cực cao.
                  - Điều chỉnh chiến dịch truyền thông theo phản hồi thực tế.
                  - Phát hiện sớm **khủng hoảng truyền thông** theo từng chủ đề.
                """
            )

            # === BẢNG THỐNG KÊ THEO TOPIC ===
            st.markdown("### 📊 Thống kê cảm xúc theo từng Topic")
            st.dataframe(
                topic_stats.reset_index().sort_values("Total", ascending=False),
                use_container_width=True,
                height=300,
            )

            st.markdown("---")

            # ==========================
            # 🎯 DRILL-DOWN THEO TỪNG TOPIC
            # ==========================
            st.markdown("## 🎯 Phân tích chi tiết theo Topic")

            topic_list = topic_stats.index.tolist()
            selected_topic = st.selectbox(
                "Chọn Topic để xem chi tiết:",
                options=topic_list,
                index=0 if topic_list else None,
            )

            if selected_topic:
                df_focus = df[df["Topic"] == selected_topic]
            else:
                df_focus = df

            if not df_focus.empty:
                f_total = len(df_focus)
                f_pos = (df_focus["Sentiment"] == "Positive").sum()
                f_neu = (df_focus["Sentiment"] == "Neutral").sum()
                f_neg = (df_focus["Sentiment"] == "Negative").sum()
                f_ratio = f_neg / f_total if f_total > 0 else 0
                f_avglen = (
                    df_focus["TextLength"].mean().round(2)
                    if "TextLength" in df_focus.columns
                    else 0
                )

                c1, c2, c3, c4, c5 = st.columns(5)
                c1.metric("Tweets (topic)", int(f_total))
                c2.metric("Positive", int(f_pos))
                c3.metric("Neutral", int(f_neu))
                c4.metric("Negative", int(f_neg))
                c5.metric("Avg Length (topic)", f_avglen)

                if f_ratio > NEG_RATIO_THRESHOLD:
                    st.error(
                        f"🚨 Topic **{selected_topic}** đang có tỷ lệ Negative rất cao "
                        f"({f_ratio:.1%}) – có thể là dấu hiệu khủng hoảng liên quan "
                        "đến chủ đề này."
                    )

                # 🔹 PHÂN PHỐI CẢM XÚC RIÊNG CHO TOPIC
                sentiment_focus = (
                    df_focus.groupby("Sentiment")
                    .size()
                    .reset_index(name="Count")
                )

                fig_sent_focus = px.pie(
                    sentiment_focus,
                    names="Sentiment",
                    values="Count",
                    title=f"🎭 Phân phối cảm xúc – Topic: {selected_topic}",
                )

                # 🔹 PHÂN PHỐI ĐỘ DÀI TEXT RIÊNG CHO TOPIC
                if "TextLength" in df_focus.columns:
                    fig_len_focus = px.histogram(
                        df_focus,
                        x="TextLength",
                        nbins=20,
                        title=f"✍️ Phân phối độ dài Text – Topic: {selected_topic}",
                    )
                else:
                    fig_len_focus = px.histogram(
                        title=(
                            f"✍️ Không có cột TextLength – Topic: {selected_topic}"
                        )
                    )

                cA, cB = st.columns(2)
                cA.plotly_chart(fig_sent_focus, use_container_width=True)
                cB.plotly_chart(fig_len_focus, use_container_width=True)

                # 🔹 BẢNG LATEST TWEETS CHO TOPIC + NÚT TẢI CSV NGAY TRÊN BẢNG
                st.markdown("#### 🕊️ Latest Tweets – Topic: " + selected_topic)

                view_cols = [
                    c
                    for c in ["ID", "Topic", "Sentiment", "Text", "Timestamp"]
                    if c in df_focus.columns
                ]

                # Lấy các tweet mới nhất của topic (sắp xếp theo Timestamp nếu có)
                if "Timestamp" in df_focus.columns:
                    df_topic_latest = df_focus.sort_values("Timestamp").tail(50)
                else:
                    df_topic_latest = df_focus.tail(50)

                # Nút tải CSV đặt NGAY TRÊN bảng
                csv_topic_latest = df_topic_latest[view_cols].to_csv(
                    index=False
                ).encode("utf-8-sig")
                st.download_button(
                    label="⬇️ Tải CSV các tweet mới nhất của topic này",
                    data=csv_topic_latest,
                    file_name=f"topic_{selected_topic}_latest_tweets.csv",
                    mime="text/csv",
                    key=f"download_topic_latest_{selected_topic}",
                )

                st.dataframe(
                    df_topic_latest[view_cols],
                    use_container_width=True,
                    height=350,
                )

            else:
                st.info("Topic đang chọn chưa có dữ liệu.")

            st.markdown("---")

            # ==========================
            # 📊   BIỂU ĐỒ TOÀN HỆ THỐNG
            # ==========================
            sentiment_dist = df["Sentiment"].value_counts().reset_index()
            sentiment_dist.columns = ["Sentiment", "Count"]

            top_topics = (
                df.groupby("Topic")
                .size()
                .reset_index(name="Count")
                .sort_values("Count", ascending=False)
                .head(5)
            )

            fig_sent = px.pie(
                sentiment_dist,
                names="Sentiment",
                values="Count",
                title="🎭 Sentiment Distribution (All Topics)",
            )

            fig_topic = px.bar(
                top_topics,
                x="Topic",
                y="Count",
                title="🔥 Top 5 Trending Topics",
            )

            if "TextLength" in df.columns:
                fig_len = px.histogram(
                    df,
                    x="TextLength",
                    nbins=25,
                    title="✍️ Tweet Length Distribution (All Topics)",
                )
            else:
                fig_len = px.histogram(
                    title="✍️ Tweet Length Distribution (No TextLength field)"
                )

            row1_colA, row1_colB, row1_colC = st.columns(3)
            row1_colA.plotly_chart(fig_sent, use_container_width=True)
            row1_colB.plotly_chart(fig_topic, use_container_width=True)
            row1_colC.plotly_chart(fig_len, use_container_width=True)

            st.markdown("---")

            # === TIMELINE CHART (ALL TOPICS) ===
            if "Timestamp" in df.columns:
                try:
                    df_time = df.copy()
                    df_time["Timestamp"] = pd.to_datetime(
                        df_time["Timestamp"], errors="coerce"
                    )
                    df_time = df_time.dropna(subset=["Timestamp"])
                    df_time = (
                        df_time.groupby(
                            [pd.Grouper(key="Timestamp", freq="1min"), "Sentiment"]
                        )
                        .size()
                        .reset_index(name="Count")
                    )
                    if not df_time.empty:
                        fig_time = px.line(
                            df_time,
                            x="Timestamp",
                            y="Count",
                            color="Sentiment",
                            title="🕒 Realtime Sentiment Trend (All Topics)",
                            markers=True,
                        )
                        st.plotly_chart(fig_time, use_container_width=True)
                except Exception as e:
                    st.error(f"⚠️ Timeline Error: {e}")

            # === Latest tweets table (toàn hệ thống) ===
            st.markdown("### 🕊️ Latest Tweets")
            cols = [
                c
                for c in ["ID", "Topic", "Sentiment", "Text", "Timestamp"]
                if c in df.columns
            ]
            latest_df = df[cols]
            st.dataframe(
                latest_df,
                use_container_width=True,
                height=400,
            )

            # Nút tải CSV của batch mới nhất
            csv_latest = latest_df.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                label="⬇️ Tải CSV các tweet mới nhất",
                data=csv_latest,
                file_name="latest_batch_tweets.csv",
                mime="text/csv",
            )

            st.markdown(
                f"<div style='text-align:center; color:gray;'>"
                f"Auto-refresh every {REFRESH_INTERVAL} seconds ⏱️</div>",
                unsafe_allow_html=True,
            )

    # ==========================
    # ⏸ LOGIC DỪNG KHI ALERT
    # ==========================
    if (
        st.session_state.get("pause_on_alert", False)
        and st.session_state.get("alert_active", False)
    ):
        st.warning(
            "🚨 Dashboard đang **TẠM DỪNG** vì phát hiện topic có tỷ lệ Negative vượt ngưỡng.\n\n"
            "Hãy kiểm tra các topic cảnh báo phía trên. "
            "Sau khi xử lý xong, bấm nút bên dưới để tiếp tục chạy realtime."
        )
        if st.button("✅ Tôi đã xem và xử lý, tiếp tục realtime"):
            st.session_state["alert_active"] = False
            st.rerun()
        # Không auto-refresh nữa, dừng tại đây
        return

    # ==========================
    # 🌀 TRẠNG THÁI BÌNH THƯỜNG: AUTO-REFRESH
    # ==========================
    for i in range(REFRESH_INTERVAL):
        progress.progress(int((i + 1) / REFRESH_INTERVAL * 100))
        time.sleep(1)
    progress.progress(0)

    st.rerun()


if __name__ == "__main__":
    main()
