# step1c_clean_reddit.py
# Clean reddit_labeled.csv → unified schema: ID, Topic, Sentiment, Text

import os
import pandas as pd

# ========= 1. Paths =========
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

step0_dir = os.path.join(BASE_DIR, "Output", "step0")
input_path = os.path.join(step0_dir, "reddit_labeled.csv")

step1_dir = os.path.join(BASE_DIR, "Output", "step1")
os.makedirs(step1_dir, exist_ok=True)

output_path = os.path.join(step1_dir, "reddit_clean.csv")
log_path = os.path.join(step1_dir, "reddit_clean_log.txt")


def main():
    print("=== STEP1C: Clean reddit_labeled.csv ===")
    print(f"[INPUT ] {input_path}")
    print(f"[OUTPUT] {output_path}\n")

    # ========= 2. Read labeled reddit data =========
    df = pd.read_csv(
        input_path,
        encoding="utf-8",
        engine="python",
        on_bad_lines="skip"
    )

    n_raw = len(df)
    print(f"Total raw rows: {n_raw}")

    # Expect columns: clean_text, sentiment
    needed_cols = ["clean_text", "sentiment"]
    for c in needed_cols:
        if c not in df.columns:
            raise ValueError(f"Missing required column: {c}")

    df = df[needed_cols].copy()

    # ========= 3. Basic cleaning =========
    df = df.dropna(subset=["clean_text", "sentiment"])

    df["clean_text"] = df["clean_text"].astype(str).str.strip()
    df["sentiment"] = df["sentiment"].astype(str).str.strip()

    df = df[df["clean_text"] != ""]

    # ========= 4. Normalize sentiment =========
    mapping = {
        "positive": "Positive",
        "neg": "Negative",
        "negative": "Negative",
        "neu": "Neutral",
        "neutral": "Neutral",
        "irrelevant": "Irrelevant",
        "other": "Irrelevant",
    }

    df["Sentiment"] = (
        df["sentiment"]
        .str.lower()
        .map(mapping)
        .fillna(df["sentiment"].str.title())
    )

    # ========= 5. Build output schema =========
    df["ID"] = range(1, len(df) + 1)
    df["Topic"] = "Reddit"
    df["Text"] = df["clean_text"]

    df_out = df[["ID", "Topic", "Sentiment", "Text"]]

    # ========= 6. Save =========
    df_out.to_csv(output_path, index=False, encoding="utf-8")

    sentiment_stats = df_out["Sentiment"].value_counts().sort_index()

    print("\n=== Cleaned dataset statistics ===")
    print(f"Rows after cleaning: {len(df_out)}")
    print("Sentiment distribution:")
    print(sentiment_stats)

    with open(log_path, "w", encoding="utf-8") as f:
        f.write("STEP1C - Clean reddit_labeled.csv\n")
        f.write(f"Input : {input_path}\n")
        f.write(f"Output: {output_path}\n\n")
        f.write(f"Raw rows   : {n_raw}\n")
        f.write(f"Clean rows : {len(df_out)}\n\n")
        f.write("Sentiment distribution:\n")
        f.write(sentiment_stats.to_string())
        f.write("\n")

    print("\n✔ DONE — Saved clean file:", output_path)
    print("✔ Log:", log_path)


if __name__ == "__main__":
    main()
