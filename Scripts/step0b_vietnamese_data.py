import pandas as pd

# Đọc file
df = pd.read_csv("../data/vietnamese_sentiment.csv")

# Xóa cột Unnamed nếu tồn tại
if "Unnamed: 0" in df.columns:
    df = df.drop(columns=["Unnamed: 0"])

# Map emotion → sentiment 3-class
emotion_to_sentiment = {
    "enjoyment": "Positive",
    "anger": "Negative",
    "disgust": "Negative",
    "fear": "Negative",
    "sadness": "Negative",
    "surprise": "Neutral",
    "other": "Neutral"
}

# Tạo cột Sentiment chuẩn hóa
df["Sentiment"] = df["Emotion"].str.lower().map(emotion_to_sentiment)

# Tạo DataFrame đầu ra với đúng schema của pipeline
df_clean = pd.DataFrame({
    "ID": range(len(df)),
    "Topic": df["Sentence"],
    "Sentiment": df["Sentiment"]
})

# Lưu file
df_clean.to_csv("../Output/step1/clean_vietnamese.csv", index=False)

print("Done!")
