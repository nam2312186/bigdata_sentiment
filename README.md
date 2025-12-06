# 🎯 Real-time Sentiment Analysis with Kafka & Spark Streaming

Dự án mô phỏng một pipeline Big Data streaming để phân tích cảm xúc (sentiment) trên mạng xã hội theo thời gian thực:

```
Step0 → Step1 → Step2 → Step3 → Step4
Chuẩn bị dữ liệu → Làm sạch → Đưa lên Kafka → Spark Streaming xử lý → Dashboard Streamlit
```

Sentiment được chuẩn hóa thành 4 lớp:
- **Positive**
- **Neutral**
- **Negative**
- **Irrelevant**

---

## ✅ 1. Yêu cầu môi trường

### Hệ thống
- **Windows 10/11**
- **Java 11+** (khuyến nghị: Temurin 17 hoặc 21)
- **Python 3.11+**
- **Apache Kafka 3.7.0** (Windows)
- **Apache Spark 4.0.1** (kèm Hadoop 3)
- **winutils.exe** (để Spark/Hadoop chạy trên Windows)

### Python packages
Python packages nằm trong: `requirements.txt`

```
D:\Daihoc\Nam3\BIGDATA\BTL\bigdata_sentiment\requirements.txt
```

### Tạo venv và cài thư viện (1 lần):

```cmd
cd D:\Daihoc\Nam3\BIGDATA\BTL\bigdata_sentiment

python -m venv venv
venv\Scripts\activate

pip install -r requirements.txt
```

---

## 🧱 2. Cấu trúc thư mục

```
bigdata_sentiment/
│
├── Scripts/
│   ├── step0_data_preparation.py   # chuẩn bị/gộp dữ liệu thô (Twitter, VN, Reddit, …)
│   ├── step1_cleaning.py           # clean & chuẩn hoá dữ liệu chính
│   ├── step1b_convert_vn.py        # xử lý/convert dữ liệu tiếng Việt
│   ├── step1c_clean_reddit.py      # chuẩn hoá bộ reddit_labeled → clean_reddit.csv
│   ├── step2_kafka_producer.py     # gửi dữ liệu sạch lên Kafka (twitter_stream)
│   ├── step3_spark_streaming.py    # Spark Streaming đọc Kafka → CSV + thống kê
│   └── step4_dashboard.py          # Streamlit dashboard realtime
│
├── Output/
│   ├── step0/                      # output trung gian Step0 (nếu có)
│   ├── step1/                      # dữ liệu sạch: clean_data.csv, clean_reddit.csv, …
│   ├── step2/                      # log Kafka Producer
│   ├── step3/                      # (tuỳ chọn) output khác của Step3
│   ├── step3_stream_output/        # CSV theo từng batch Spark
│   └── checkpoint/                 # checkpoint Spark Streaming
│
└── data/
    ├── twitter_sentiment.csv       # dữ liệu sentiment Twitter
    ├── clean_vietnamese.csv        # dữ liệu tiếng Việt đã gán nhãn
    └── reddit_labeled.csv          # dữ liệu Reddit thô (text, clean_text, sentiment)
```

---

## 🪜 3. Quy trình chạy theo từng Step

### 🟦 Step 0 – Chuẩn bị dữ liệu (Optional)

**Mục đích:** Gom/chuẩn bị dữ liệu thô, tách trường, lọc cột… trước khi clean.

Trong môi trường ảo:

```cmd
cd bigdata_sentiment
venv\Scripts\activate
cd Scripts

python step0_data_preparation.py
```

**Output:** (nếu có) sẽ được ghi vào `Output/step0/` hoặc cập nhật các file trong `data/`.

> **Lưu ý:** Nếu dữ liệu gốc đã ổn, bạn có thể bỏ qua Step0.

---

### 🟩 Step 1 – Làm sạch & chuẩn hoá dữ liệu

**Mục tiêu:** Đưa tất cả nguồn dữ liệu về schema chung:

```
ID, Topic, Sentiment, Text
```

Và chuẩn hoá nhãn cảm xúc về: **Positive** / **Neutral** / **Negative** / **Irrelevant**.

#### 1. Clean dữ liệu chính (Twitter + VN)

```cmd
python step1_cleaning.py
```

#### 2. (Nếu dùng) Convert thêm logic cho dữ liệu tiếng Việt

```cmd
python step1b_convert_vn.py
```

#### 3. Thêm nguồn Reddit (chuẩn hoá như Twitter)

```cmd
python step1c_clean_reddit.py
```

**Script này:**
- Đọc `data/reddit_labeled.csv` (các cột: `text`, `clean_text`, `sentiment`)
- Đặt `Topic = "Reddit"`
- Mapping sentiment về 4 lớp chuẩn
- Đánh ID theo thứ tự
- `Text = clean_text`
- Ghi ra: `Output/step1/clean_reddit.csv`

**Sau Step1, bạn có tối thiểu:**

```
Output/step1/
  ├── clean_data.csv      # dữ liệu sạch chính (Twitter + VN)
  └── clean_reddit.csv    # (nếu chạy Step1c) dữ liệu Reddit đã chuẩn hoá
```

Các file này là đầu vào cho Step2 (Producer Kafka).

---

### 🟥 Step 2 – Kafka: khởi động & gửi dữ liệu

Step2 gồm 2 phần: **(A) Khởi động Kafka**, **(B) Chạy Producer**.

**Giả sử Kafka nằm ở:** `D:\Kafka\kafka_2.13-3.7.0`

#### 2.1 Khởi động Kafka

**CMD #1 – ZooKeeper**

```cmd
cd /d D:\Kafka\kafka_2.13-3.7.0
bin\windows\zookeeper-server-start.bat config\zookeeper.properties
```

**CMD #2 – Kafka Broker**

```cmd
cd /d D:\Kafka\kafka_2.13-3.7.0
bin\windows\kafka-server-start.bat config\server.properties
```

**CMD #3 – Tạo topic `twitter_stream`** (chỉ cần khi chạy lần đầu hoặc sau khi xoá)

```cmd
cd /d D:\Kafka\kafka_2.13-3.7.0
bin\windows\kafka-topics.bat ^
  --create --topic twitter_stream ^
  --bootstrap-server 127.0.0.1:9092 ^
  --partitions 1 --replication-factor 1
```

#### 2.2 Chạy Kafka Producer (`Scripts/step2_kafka_producer.py`)

Mở **CMD #4** (trong venv) tại thư mục Scripts:

```cmd
cd D:\Daihoc\Nam3\BIGDATA\BTL\bigdata_sentiment\Scripts
venv\Scripts\activate

python step2_kafka_producer.py
```

**Script sẽ:**
- Đọc `Output/step1/clean_data.csv` (và/hoặc merge thêm `clean_reddit.csv` tuỳ code)
- Mỗi dòng → JSON `{ID, Topic, Sentiment, Text, Timestamp}`
- Gửi lên topic `twitter_stream`
- Ghi log vào `Output/step2/producer_log.txt`

**Terminal sẽ hiển thị dạng:**

```
[1/74682] Sent: Positive | I'm getting on borderlands and I will murder you all...
[2/74682] Sent: Neutral  | ...
...
```

---

### 🟧 Step 3 – Spark Structured Streaming

**Mục tiêu:** Spark đọc stream từ Kafka, tiền xử lý & ghi ra CSV theo batch.

#### 3.1 Thiết lập biến môi trường PySpark

Mở **CMD #5:**

```cmd
cd D:\Daihoc\Nam3\BIGDATA\BTL\bigdata_sentiment

set PYSPARK_PYTHON=D:\Daihoc\Nam3\BIGDATA\BTL\bigdata_sentiment\venv\Scripts\python.exe
set PYSPARK_DRIVER_PYTHON=D:\Daihoc\Nam3\BIGDATA\BTL\bigdata_sentiment\venv\Scripts\python.exe
```

#### 3.2 Chạy Spark Streaming

```cmd
spark-submit ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 ^
  Scripts\step3_spark_streaming.py
```

**Spark sẽ:**
- Đọc topic `twitter_stream`
- Parse JSON → cột `ID`, `Topic`, `Sentiment`, `Text`, `Timestamp`
- Thêm `IngestedAt`, `TextLength`
- Ghi từng batch vào `Output/step3_stream_output/`
- In ra console:
  - Bảng dữ liệu từng batch
  - Tổng số tweet, độ dài trung bình
  - Phân phối 4 lớp sentiment

Đây chính là nguồn dữ liệu realtime cho Step4.

---

### 🟨 Step 4 – Streamlit Dashboard

Cuối cùng là dashboard để giám sát pipeline.

Mở **CMD #6** (trong venv):

```cmd
cd D:\Daihoc\Nam3\BIGDATA\BTL\bigdata_sentiment
venv\Scripts\activate

streamlit run Scripts\step4_dashboard.py
```

#### Dashboard sẽ:

**Quét `Output/step3_stream_output/` và luôn lấy file CSV mới nhất**

**Hiển thị:**

1. **System Overview:**
   - Tổng số tweet
   - Số Positive/Neutral/Negative/Irrelevant
   - Độ dài trung bình
   - Tên batch mới nhất

2. **Biểu đồ tổng quan:**
   - Sentiment Distribution
   - Top 5 Topics
   - Tweet Length Distribution
   - Realtime Trend

3. **Phân tích chi tiết theo topic:**
   - Metric riêng cho topic
   - Pie chart cảm xúc + histogram độ dài text
   - Bảng Latest Tweets – Topic X + nút tải CSV các tweet mới nhất của topic

4. **Latest Tweets (All Topics)** cuối trang + nút tải CSV các tweet mới nhất

#### Tính năng đặc biệt:

**Sidebar:** Tuỳ chọn "Tự dừng dashboard khi có topic nguy hiểm" (topic có tỷ lệ Negative > 50%)

**Khi phát hiện topic nguy hiểm:**
- Dashboard hiển thị cảnh báo
- Auto-refresh tạm dừng để người vận hành phân tích
- Có nút **"Tôi đã xem và xử lý, tiếp tục realtime"** để chạy lại

---

## 🧹 4. Reset & chạy lại thí nghiệm

Khi muốn xoá topic cũ và chạy lại từ Step0 → Step4:

```cmd
cd /d D:\Kafka\kafka_2.13-3.7.0
bin\windows\kafka-topics.bat ^
  --delete --topic twitter_stream ^
  --bootstrap-server localhost:9092
```

Sau đó thực hiện lại các bước từ Step 2.1 (tạo topic mới) → Step 2.2 → Step 3 → Step 4.

---

## 📊 Tổng kết Pipeline

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Step 0    │───▶│   Step 1    │───▶│   Step 2    │───▶│   Step 3    │───▶│   Step 4    │
│   Data      │    │   Data      │    │   Kafka     │    │   Spark     │    │  Streamlit  │
│ Preparation │    │  Cleaning   │    │  Producer   │    │  Streaming  │    │  Dashboard  │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
     (Optional)         ↓                    ↓                   ↓                   ↓
                   clean_data.csv      twitter_stream      CSV batches         Real-time
                   clean_reddit.csv       (topic)        (step3_stream_output)  Visualization
```

---

## 📝 Ghi chú

- Đảm bảo tất cả các terminal (ZooKeeper, Kafka Broker, Producer, Spark, Dashboard) đều đang chạy đồng thời
- Nếu gặp lỗi với Spark trên Windows, kiểm tra `HADOOP_HOME` và `winutils.exe`
- Dashboard tự động refresh mỗi 5 giây để cập nhật dữ liệu mới nhất
- Checkpoint Spark được lưu tại `Output/checkpoint/` để đảm bảo fault-tolerance

---

## 🔗 Liên hệ & Hỗ trợ

Nếu có vấn đề hoặc câu hỏi, vui lòng tạo issue hoặc liên hệ qua repository.

**Repository:** bigdata_sentiment  
**Owner:** nam2312186  
**Branch:** nam2312186

---

**Happy Streaming! 🚀**
