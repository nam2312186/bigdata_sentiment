import pandas as pd
import re
import os
import sys
import unicodedata


def clean_text(s: str) -> str:
    if not isinstance(s, str):
        return ""

    # Chuẩn hóa unicode (loại kí tự lỗi)
    s = unicodedata.normalize("NFKC", s)

    # Bỏ đoạn trong [] như [removed], [deleted]
    s = re.sub(r"\[.*?\]", " ", s)

    # Bỏ link
    s = re.sub(r"http\S+|www\.\S+", " ", s)

    # Bỏ username @abc
    s = re.sub(r"@\w+", " ", s)

    # Bỏ reddit quotes (> something)
    s = re.sub(r"^>\s*", " ", s)

    # Bỏ ký tự không phải chữ, số, dấu cơ bản
    s = re.sub(r"[^A-Za-z0-9\s.,!?'\-]", " ", s)

    # Gộp dấu chấm/question mark/exclamation quá nhiều
    s = re.sub(r"([!?.,])\1+", r"\1", s)

    # Gộp nhiều khoảng trắng
    s = re.sub(r"\s+", " ", s)

    # Xóa khoảng trắng đầu đuôi
    s = s.strip()

    return s


if __name__ == "__main__":
    # Đường dẫn gốc project
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_DIR = os.path.dirname(SCRIPT_DIR)

    RAW_DIR = os.path.join(PROJECT_DIR, "Output", "step0")
    INPUT_PATH = os.path.join(RAW_DIR, "reddit_raw.csv")

    print(f"[INFO] Đọc file: {INPUT_PATH}")

    if not os.path.exists(INPUT_PATH):
        print("[ERROR] Không tìm thấy file reddit_raw.csv.")
        sys.exit(1)

    if os.path.getsize(INPUT_PATH) == 0:
        print("[ERROR] File reddit_raw.csv RỖNG.")
        sys.exit(1)

    # Đọc CSV
    try:
        df = pd.read_csv(INPUT_PATH)
    except Exception as e:
        print("[ERROR] Không đọc được CSV:", e)
        sys.exit(1)

    # Kiểm tra cột text
    if "text" not in df.columns:
        print(f"[ERROR] Không có cột 'text'. Cột hiện có: {list(df.columns)}")
        sys.exit(1)

    print(f"[INFO] Số dòng ban đầu: {len(df)}")

    # Bỏ trùng
    df = df.drop_duplicates(subset=["text"])
    print(f"[INFO] Sau khi bỏ trùng: {len(df)}")

    # Clean
    df["clean_text"] = df["text"].apply(clean_text)

    # Loại text ngắn (< 10 ký tự sau clean)
    df = df[df["clean_text"].str.len() > 10]
    print(f"[INFO] Sau khi lọc text > 10 ký tự: {len(df)}")

    # Output
    CLEAN_DIR = os.path.join(PROJECT_DIR, "Output", "step0")
    os.makedirs(CLEAN_DIR, exist_ok=True)
    OUTPUT_PATH = os.path.join(CLEAN_DIR, "preprocessed_clean_reddit.csv")

    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
    print(f"✔ DONE: Saved cleaned file → {OUTPUT_PATH}")
