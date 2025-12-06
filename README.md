# 🎯 Real-time Sentiment Analysis with Kafka & Spark Streaming

### ✅ Install requirements

- **Java 11+** (recommended: Temurin 17 or 21)
- **Python 3.11+**
- **Apache Kafka 3.7.0** (Windows version)
- **Apache Spark 4.0.1** (with Hadoop 3)
- **winutils.exe** (for Windows compatibility with Hadoop)
---

## ⚙️ 2️⃣ Prerequisites

D:\Daihoc\Nam3\BIGDATA\BTL\bigdata_sentiment\requirements.txt

---

## 🧱 3️⃣ Folder Structure
bigdata_sentiment/
│
├── Scripts/
│ ├── step0_data_preparation.py
│ ├── step1_cleaning.py
│ ├── step2_kafka_producer.py
│ ├── step3_spark_streaming.py
│
├── Output/
│ ├── step1/
│ ├── step2/
│ ├── step3/
│ ├── checkpoint/
│
└── data/
└── twitter_sentiment.csv
bigdata_sentiment/
│
├── Scripts/
│ ├── step0_data_preparation.py
│ ├── step1_cleaning.py
│ ├── step2_kafka_producer.py
│ ├── step3_spark_streaming.py
│
├── Output/
│ ├── step1/
│ ├── step2/
│ ├── step3/
│ ├── checkpoint/
│
└── data/
└── twitter_sentiment.csv

---

## 🪄 4️⃣ Setup Kafka on Windows

### 🧩 Step 1 — Start ZooKeeper
Open **CMD #1**:
```bash
cd /d D:\Kafka\kafka_2.13-3.7.0
bin\windows\zookeeper-server-start.bat config\zookeeper.properties
```

⚡ Step 2 — Start Kafka Broker

Open **CMD #2**:
```bash
cd /d D:\Kafka\kafka_2.13-3.7.0
bin\windows\kafka-server-start.bat config\server.properties
```

🧩 Step 3 — Create a Kafka Topic

Open **CMD #3**:
```bash
cd /d D:\Kafka\kafka_2.13-3.7.0
bin\windows\kafka-topics.bat --create --topic twitter_stream --bootstrap-server 127.0.0.1:9092 --partitions 1 --replication-factor 1
```

# run code step2 : (venv)
```bash
python step2_kafka_producer.py
```



# run code step3
```
set PYSPARK_PYTHON=D:\Daihoc\Nam3\BIGDATA\BTL\bigdata_sentiment\venv\Scripts\python.exe
set PYSPARK_DRIVER_PYTHON=D:\Daihoc\Nam3\BIGDATA\BTL\bigdata_sentiment\venv\Scripts\python.exe

```
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 Scripts\step3_spark_streaming.py
```

```bash
streamlit run Scripts\step4_dashboard.py
```


# Sau đó xóa topic cũ ()
```bash
bin\windows\kafka-topics.bat --delete --topic twitter_stream --bootstrap-server localhost:9092
```