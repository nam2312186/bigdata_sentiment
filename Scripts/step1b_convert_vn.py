import pandas as pd
from pathlib import Path

# === 1. Đường dẫn tới file gốc ===
BASE_DIR = Path(__file__).resolve().parent.parent   # bigdata_sentiment/
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "Output" / "step1"

input_path = DATA_DIR / "clean_vietnamese.csv"

# === 2. Đọc file gốc ===
df = pd.read_csv(input_path)

print("Columns:", df.columns.tolist())

# === 3. Tạo lại cấu trúc giống clean_data.csv ===
df["Text"] = df["Topic"]                # Text = câu tiếng Việt
df["Topic"] = "sentiment_vietnamese"    # Chủ đề chung

df = df[["ID", "Topic", "Sentiment", "Text"]]

# === 4. Tạo thư mục Output nếu chưa có ===
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# === 5. Lưu file mới vào Output/step1 ===
output_path = OUTPUT_DIR / "clean_vietnamese_compat.csv"
df.to_csv(output_path, index=False, encoding="utf-8")

print(f"✅ Đã ghi file mới vào: {output_path}")
