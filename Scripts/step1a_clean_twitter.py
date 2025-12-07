# ===== Step1: Basic Cleaning (Fill missing text only) =====

import pandas as pd
import os

# === 1️⃣ Đường dẫn dữ liệu ===
input_path = "../data/twitter_sentiment.csv"
output_dir = "../Output/step1"
os.makedirs(output_dir, exist_ok=True)

# === 2️⃣ Đọc dữ liệu ===
print("🔹 Đang đọc dữ liệu ...")
df = pd.read_csv(input_path, encoding='utf-8')

# === 3️⃣ Điền 'None' cho các dòng Text bị trống hoặc NaN ===
missing_before = df['Text'].isna().sum()
df['Text'] = df['Text'].fillna("None")

# Nếu Text có giá trị rỗng (""), thay bằng "None"
df.loc[df['Text'].str.strip() == "", 'Text'] = "None"
missing_after = df['Text'].isna().sum()

# === 4️⃣ Lưu dữ liệu đã xử lý ===
output_path = os.path.join(output_dir, "clean_data.csv")
df.to_csv(output_path, index=False, encoding='utf-8')

# === 5️⃣ Ghi log thống kê ===
log_path = os.path.join(output_dir, "clean_log.txt")
with open(log_path, "w", encoding="utf-8") as f:
    f.write("=== Step1 Basic Cleaning Report ===\n\n")
    f.write(f"Missing Text trước khi xử lý: {missing_before}\n")
    f.write(f"Missing Text sau khi xử lý: {missing_after}\n")
    f.write(f"Tổng số dòng sau khi xử lý: {len(df)}\n")

print("\n✅ Đã xử lý dữ liệu thành công!")
print(f"- File dữ liệu sạch: {output_path}")
print(f"- File log thống kê: {log_path}")
