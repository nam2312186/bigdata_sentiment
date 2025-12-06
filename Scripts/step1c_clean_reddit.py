# step1c_clean_reddit.py
# Clean reddit_labeled.csv to unified schema: ID, Topic, Sentiment, Text

import os
import pandas as pd

# ========= 1. Paths =========
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

data_dir = os.path.join(BASE_DIR, "data")
input_path = os.path.join(data_dir, "reddit_labeled.csv")

output_dir = os.path.join(BASE_DIR, "Output", "step1")
os.makedirs(output_dir, exist_ok=True)

output_path = os.path.join(output_dir, "clean_reddit.csv")
log_path = os.path.join(output_dir, "clean_reddit_log.txt")


def main():
    print("=== STEP1C: Clean reddit_labeled.csv ===")
    print(f"Input file : {input_path}")
    print(f"Output file: {output_path}")
    print()

    # ========= 2. Read raw reddit data =========
    df = pd.read_csv(
        input_path,
        encoding="utf-8",
        engine="python",        # tolerant parser (emoji, quotes, etc.)
        on_bad_lines="skip"
    )

    n_raw = len(df)
    print(f"Total raw rows: {n_raw}")

    # Expect columns: text, clean_text, sentiment
    # Keep only what we need
    cols_needed = ["clean_text", "sentiment"]
    for c in cols_needed:
        if c not in df.columns:
            raise ValueError(f"Required column '{c}' not found in reddit_labeled.csv")

    df = df[cols_needed].copy()

    # ========= 3. Basic cleaning =========
    # Drop rows with missing clean_text or sentiment
    df = df.dropna(subset=["clean_text", "sentiment"])

    # Strip spaces
    df["clean_text"] = df["clean_text"].astype(str).str.strip()
    df["sentiment"] = df["sentiment"].astype(str).str.strip()

    # Remove empty clean_text after strip
    df = df[df["clean_text"] != ""]

    # ========= 4. Normalize sentiment labels =========
    # Map to format used in main pipeline: Positive / Neutral / Negative / Irrelevant
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

    # ========= 5. Build unified schema =========
    df["ID"] = range(1, len(df) + 1)      # sequential ID
    df["Topic"] = "Reddit"                # fixed topic name
    df["Text"] = df["clean_text"]         # use cleaned text as Text

    df_out = df[["ID", "Topic", "Sentiment", "Text"]]

    # ========= 6. Save output & log =========
    df_out.to_csv(output_path, index=False, encoding="utf-8")

    # Some stats
    sentiment_counts = df_out["Sentiment"].value_counts().sort_index()

    print()
    print("=== Cleaned dataset statistics ===")
    print(f"Rows after cleaning: {len(df_out)}")
    print("Sentiment distribution:")
    print(sentiment_counts)

    with open(log_path, "w", encoding="utf-8") as f:
        f.write("STEP1C - Clean reddit_labeled.csv\n")
        f.write(f"Input : {input_path}\n")
        f.write(f"Output: {output_path}\n\n")
        f.write(f"Raw rows   : {n_raw}\n")
        f.write(f"Clean rows : {len(df_out)}\n\n")
        f.write("Sentiment distribution:\n")
        f.write(sentiment_counts.to_string())
        f.write("\n")

    print()
    print(f"Done. Clean reddit file saved to {output_path}")
    print(f"Log written to {log_path}")


if __name__ == "__main__":
    main()
