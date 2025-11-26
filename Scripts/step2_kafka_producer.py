# ===== Step2: Kafka Producer (Giả lập streaming) =====

from kafka import KafkaProducer
import pandas as pd
import json
import time
import os

# === 1️⃣ Cấu hình đường dẫn và Kafka ===
# input_path = "../Output/step1/clean_data.csv"
input_path = "../Output/step1/clean_vietnamese_compat.csv"
output_dir = "../Output/step2"
os.makedirs(output_dir, exist_ok=True)
log_file = os.path.join(output_dir, "producer_log.txt")

topic = "twitter_stream"
bootstrap_servers = "127.0.0.1:9092"  # Broker Kafka local

# === 2️⃣ Tạo Kafka Producer ===
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# === 3️⃣ Đọc dữ liệu cần gửi ===
df = pd.read_csv(input_path, encoding='utf-8')
total = len(df)
print(f"📄 Đã tải {total} dòng dữ liệu từ {input_path}")

# === 4️⃣ Gửi dữ liệu từng dòng lên Kafka ===
with open(log_file, "w", encoding="utf-8") as log:
    log.write("=== Step2 Kafka Producer Log ===\n\n")

    for i, row in df.iterrows():
        # Tạo message JSON
        data = {
            "ID": str(row["ID"]),
            "Topic": str(row["Topic"]),
            "Sentiment": str(row["Sentiment"]),
            "Text": str(row["Text"]),
            "Timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        # Gửi message
        producer.send(topic, value=data)
        producer.flush()

        # In & ghi log
        msg = f"[{i+1}/{total}] Sent: {data['Sentiment']} | {data['Text'][:60]}"
        print(msg)
        log.write(msg + "\n")

        # Giả lập tốc độ stream: 0.2 giây / dòng (5 dòng/giây)
        time.sleep(0.001)

print("\n✅ Gửi dữ liệu hoàn tất!")
print(f"- Topic: {topic}")
print(f"- Log file: {log_file}")
