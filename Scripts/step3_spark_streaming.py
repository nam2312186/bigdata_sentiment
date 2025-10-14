from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import (
    from_json, col, length, count, avg, desc, current_timestamp, date_format
)
import os

# === 1️⃣ Setup checkpoint & output directories ===
checkpoint_dir = "Output/checkpoint"
output_dir = "Output/step3_stream_output"
os.makedirs(checkpoint_dir, exist_ok=True)
os.makedirs(output_dir, exist_ok=True)

# === 2️⃣ Create SparkSession ===
spark = (
    SparkSession.builder
    .appName("TwitterSentimentStreaming")
    .config("spark.sql.streaming.checkpointLocation", checkpoint_dir)
    .config("spark.sql.shuffle.partitions", 4)
    .config("spark.streaming.backpressure.enabled", True)
    .config("spark.streaming.kafka.maxRatePerPartition", 20000)
    .getOrCreate()
)

spark.sparkContext.setSystemProperty("hadoop.home.dir", "D:\\FILEC\\hadoop")
spark.sparkContext.setSystemProperty("hadoop.native.lib", "false")
spark.sparkContext.setLogLevel("WARN")

# === 3️⃣ Define schema ===
schema = (
    StructType()
    .add("ID", StringType())
    .add("Topic", StringType())
    .add("Sentiment", StringType())
    .add("Text", StringType())
    .add("Timestamp", StringType())
)

# === 4️⃣ Read from Kafka topic ===
print(" Connecting to Kafka topic: twitter_stream ...")

df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribe", "twitter_stream")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 10000)
    .option("failOnDataLoss", "false")
    .load()
)

# === 5️⃣ Parse JSON messages ===
df = (
    df_raw
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
)

# Thêm thời điểm Spark nhận dữ liệu
df = df.withColumn("IngestedAt", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("TextLength", length(col("Text")))

# === 6️⃣ Output 1: Log tất cả tweet ra file và console ===
print("\n Spark Structured Streaming is running ...")
print("   Reading from Kafka topic: twitter_stream")
print("   Processing interval: 10 seconds per batch")
print("   Output: raw tweets + batch analysis summary\n")

# --- Console log raw tweets ---
query_console = (
    df.writeStream
    .format("console")
    .option("truncate", False)
    .option("numRows", 20)
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start()
)

# --- Ghi file CSV ---
query_file = (
    df.writeStream
    .outputMode("append")
    .format("csv")
    .option("path", output_dir)
    .option("checkpointLocation", os.path.join(checkpoint_dir, "file_output"))
    .option("header", True)
    .trigger(processingTime="10 seconds")
    .start()
)

# === 7️⃣ Output 2: Real-time Aggregations ===
# Tổng số tweet
agg_total = df.groupBy().agg(count("*").alias("Total_Tweets"))

# Phân phối cảm xúc
agg_sentiment = df.groupBy("Sentiment").agg(count("*").alias("Count"))

# Top 5 chủ đề
agg_top_topics = (
    df.groupBy("Topic")
    .agg(count("*").alias("Count"))
    .orderBy(desc("Count"))
    .limit(5)
)

# Độ dài trung bình text
agg_text_len = df.agg(avg("TextLength").alias("Avg_Text_Length"))

# --- In kết quả phân tích ra terminal ---
query_stats_sentiment = (
    agg_sentiment.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .trigger(processingTime="10 seconds")
    .start()
)

query_stats_topics = (
    agg_top_topics.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .trigger(processingTime="10 seconds")
    .start()
)

query_stats_total = (
    agg_total.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .trigger(processingTime="10 seconds")
    .start()
)

query_stats_avg = (
    agg_text_len.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .trigger(processingTime="10 seconds")
    .start()
)

# === 8️⃣ Await all queries ===
query_console.awaitTermination()
query_file.awaitTermination()
query_stats_sentiment.awaitTermination()
query_stats_topics.awaitTermination()
query_stats_total.awaitTermination()
query_stats_avg.awaitTermination()
