import os
import pandas as pd

# ==== PATH ====
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_path = os.path.join(BASE_DIR, "Output", "step0", "vietnamese_sentiment.csv")

step1_dir = os.path.join(BASE_DIR, "Output", "step1")
os.makedirs(step1_dir, exist_ok=True)

output_path = os.path.join(step1_dir, "vietnamese_clean.csv")
log_path = os.path.join(step1_dir, "vietnamese_clean_log.txt")


def main():
    print("=== STEP1A: Clean vietnamese_sentiment.csv ===")
    print(f"Input  file: {input_path}")
    print(f"Output file: {output_path}")
    print()

    # ==== READ INPUT ====
    try:
        df = pd.read_csv(input_path, encoding="utf-8")
    except Exception as e:
        print("[ERROR] Cannot read CSV:", e)
        return

    n_raw = len(df)

    # Remove Unnamed column if exists
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # ==== MAPPING EMOTION → SENTIMENT ====
    emotion_to_sentiment = {
        "enjoyment": "Positive",
        "anger": "Negative",
        "disgust": "Negative",
        "fear": "Negative",
        "sadness": "Negative",
        "surprise": "Neutral",
        "other": "Neutral"
    }

    df["Sentiment"] = df["Emotion"].str.lower().map(emotion_to_sentiment)

    # Drop rows with missing sentiment (unexpected emotion)
    df = df.dropna(subset=["Sentiment"])

    # ==== UNIFIED SCHEMA ====
    df_out = pd.DataFrame({
        "ID": range(1, len(df) + 1),
        "Topic": ["VietnameseSentiment"] * len(df),
        "Sentiment": df["Sentiment"],
        "Text": df["Sentence"].astype(str).str.strip()
    })

    # ==== SAVE OUTPUT ====
    df_out.to_csv(output_path, index=False, encoding="utf-8")

    # ==== LOG SUMMARY ====
    sentiment_counts = df_out["Sentiment"].value_counts().sort_index()

    with open(log_path, "w", encoding="utf-8") as f:
        f.write("STEP1A - Clean vietnamese_sentiment.csv\n")
        f.write(f"Input : {input_path}\n")
        f.write(f"Output: {output_path}\n\n")
        f.write(f"Raw rows   : {n_raw}\n")
        f.write(f"Clean rows : {len(df_out)}\n\n")
        f.write("Sentiment distribution:\n")
        f.write(sentiment_counts.to_string())
        f.write("\n")

    # ==== PRINT SUMMARY ====
    print("=== Cleaned dataset statistics ===")
    print(f"Rows after cleaning: {len(df_out)}")
    print("Sentiment distribution:")
    print(sentiment_counts)
    print()
    print(f"✔ Done. Clean Vietnamese file saved to: {output_path}")
    print(f"📄 Log saved to: {log_path}")


if __name__ == "__main__":
    main()
