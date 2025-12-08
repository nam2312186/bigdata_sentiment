import pandas as pd
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import os
import sys

MODEL = "ProsusAI/finbert"

tokenizer = AutoTokenizer.from_pretrained(MODEL)
model = AutoModelForSequenceClassification.from_pretrained(MODEL)


def predict_sentiment(text: str) -> str:
    inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=128)
    outputs = model(**inputs)
    probs = torch.nn.functional.softmax(outputs.logits, dim=1)
    label_id = torch.argmax(probs).item()

    mapping = {0: "negative", 1: "neutral", 2: "positive"}
    return mapping[label_id]


if __name__ == "__main__":
    # Xác định thư mục project (bigdata_sentiment/)
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_DIR = os.path.dirname(SCRIPT_DIR)

    # Đường dẫn file input: data/clean/reddit_clean.csv
    CLEAN_DIR = os.path.join(PROJECT_DIR, "Output", "step0")
    INPUT_PATH = os.path.join(CLEAN_DIR, "preprocessed_clean_reddit.csv")

    print(f"[INFO] Đọc file: {INPUT_PATH}")

    if not os.path.exists(INPUT_PATH):
        print("[ERROR] Không tìm thấy reddit_clean.csv. Nhớ chạy clean_data.py trước!")
        sys.exit(1)

    if os.path.getsize(INPUT_PATH) == 0:
        print("[ERROR] File reddit_clean.csv đang RỖNG. Kiểm tra lại bước clean.")
        sys.exit(1)

    # Đọc dữ liệu đã clean
    df = pd.read_csv(INPUT_PATH)

    if "clean_text" not in df.columns:
        print(f"[ERROR] Không thấy cột 'clean_text'. Các cột hiện có: {list(df.columns)}")
        sys.exit(1)

    print(f"[INFO] Số dòng cần gắn nhãn: {len(df)}")

    sentiments = []
    for t in df["clean_text"]:
        sentiments.append(predict_sentiment(t))

    df["sentiment"] = sentiments

    # Lưu ra data/labeled/reddit_labeled.csv
    LABELED_DIR = os.path.join(PROJECT_DIR, "Output", "step0")
    os.makedirs(LABELED_DIR, exist_ok=True)
    OUTPUT_PATH = os.path.join(LABELED_DIR, "reddit_labeled.csv")

    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
    print(f"✔ DONE: Saved labeled file → {OUTPUT_PATH}")
