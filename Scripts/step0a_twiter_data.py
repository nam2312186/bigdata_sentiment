import pandas as pd
import os

# === 1️⃣ Đường dẫn dữ liệu ===
input_path = "../data/twitter_sentiment.csv"
output_dir = "../Output/step0"
os.makedirs(output_dir, exist_ok=True)

print("🔹 Đang đọc dữ liệu...")
df = pd.read_csv(input_path, encoding='utf-8')

# === 3️⃣ Hiển thị thông tin cơ bản ===
print("\n📊 Thông tin dataset:")
print(f"- Số dòng: {len(df)}")
print(f"- Số cột: {len(df.columns)}")
print(f"- Cột: {list(df.columns)}")

# === 4️⃣ Kiểm tra giá trị thiếu ===
missing = df.isnull().sum()
print("\n🔍 Số lượng giá trị thiếu mỗi cột:")
print(missing)

# === 5️⃣ Thống kê sentiment ===
print("\n💬 Phân bố sentiment:")
sentiment_counts = df["Sentiment"].value_counts()
print(sentiment_counts)

# === 6️⃣ Xuất thống kê ra file ===
with open(os.path.join(output_dir, "statistics.txt"), "w", encoding="utf-8") as f:
    f.write("=== Sentiment Dataset Statistics ===\n\n")
    f.write(f"Total Rows: {len(df)}\n")
    f.write(f"Columns: {list(df.columns)}\n\n")
    f.write("Missing Values:\n")
    f.write(str(missing))
    f.write("\n\nSentiment Distribution:\n")
    f.write(str(sentiment_counts))

# === 7️⃣ Lưu mẫu dữ liệu 100 dòng ===
sample_path = os.path.join(output_dir, "preview.csv")
df.head(100).to_csv(sample_path, index=False, encoding='utf-8')

print("\n✅ Đã lưu:")
print(f"- File thống kê: {output_dir}/statistics.txt")
print(f"- File mẫu: {sample_path}")
