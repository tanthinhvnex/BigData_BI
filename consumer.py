import findspark
import sys
import traceback
from pyspark.ml.feature import StringIndexerModel # Giữ lại để kiểm tra loại stage
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

# --- Hàm chính ---
if __name__ == "__main__":
    findspark.init()

    # --- Cấu hình ---
    path_to_model = 'D:/HK242/BigData_BI/BigData_BI/pre_trained_model'
    KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
    APP_NAME = "TwitterSentimentAnalysis_MultiTopic_MongoDB_MoreFields" # Đổi tên App
    DRIVER_MEMORY = "8g"

    TOPIC_COLLECTION_MAP = [
        {'topic': 'gpt-2', 'collection': 'gpt_predictions_2'},
        {'topic': 'copilot-2', 'collection': 'copilot_predictions_2'},
        {'topic': 'gemini-2', 'collection': 'gemini_predictions_2'},
    ]
    KAFKA_TOPICS = ",".join([item['topic'] for item in TOPIC_COLLECTION_MAP])

    SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1"
    MONGO_SPARK_PACKAGE = "org.mongodb.spark:mongo-spark-connector_2.13:10.3.0"
    SPARK_PACKAGES = f"{SPARK_KAFKA_PACKAGE},{MONGO_SPARK_PACKAGE}"

    MONGO_URI = "mongodb://localhost:27017"
    MONGO_DATABASE = "BigData"

    spark = None
    try:
        print("Khoi tao Spark Session...")
        spark = SparkSession \
            .builder \
            .master("local[*]") \
            .appName(APP_NAME) \
            .config("spark.jars.packages", SPARK_PACKAGES) \
            .config("spark.driver.memory", DRIVER_MEMORY) \
            .config("spark.mongodb.write.connection.uri", MONGO_URI) \
            .config("spark.mongodb.write.database", MONGO_DATABASE) \
            .getOrCreate()

        sc = spark.sparkContext
        sc.setLogLevel('WARN')
        print("Spark Session da san sang.")

        # --- CẬP NHẬT SCHEMA: Thêm trường 'url' ---
        tweet_schema = StructType([
            StructField("id", StringType(), True),
            StructField("url", StringType(), True), # <<< THÊM URL Ở ĐÂY
            StructField("text", StringType(), True),
            StructField("lang", StringType(), True),
            StructField("createdAt", StringType(), True),
            StructField("author", StructType([
                StructField("id", StringType(), True),
                StructField("userName", StringType(), True),
                StructField("name", StringType(), True),
                StructField("isBlueVerified", BooleanType(), True)
                # Bạn có thể thêm các trường khác của author nếu producer gửi chúng
            ]), True),
            StructField("retweetCount", LongType(), True),
            StructField("replyCount", LongType(), True),
            StructField("likeCount", LongType(), True),
            StructField("quoteCount", LongType(), True),
            StructField("bookmarkCount", LongType(), True), # Giữ lại nếu producer gửi
            StructField("viewCount", LongType(), True),
            StructField("isReply", BooleanType(), True),
            StructField("conversationId", StringType(), True)
        ])
        print("Schema tweet chi tiet da duoc cap nhat (them 'url').")

        print(f"Dang tai mo hinh tu: {path_to_model}")
        pipeline_model = PipelineModel.load(path_to_model)
        print("Mo hinh Pipeline da duoc tai thanh cong.")

        print(f"Dang doc du lieu tu Kafka topics: '{KAFKA_TOPICS}'...")
        df_kafka_raw = spark \
            .read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_TOPICS) \
            .option("startingOffsets", "earliest") \
            .load()
        print(f"Da tai {df_kafka_raw.count()} ban ghi tho tu Kafka.")

        if df_kafka_raw.count() > 0:
            df_parsed_all = df_kafka_raw \
                .select(col("key").cast("string"),
                        col("value").cast("string").alias("value_str"),
                        col("topic")) \
                .withColumn("tweet_data", from_json(col("value_str"), tweet_schema)) \
                .select("key", "topic", "tweet_data.*") # Giữ lại cột topic

            print("Da phan tich cu phap JSON cho tat ca topics.")

            for config in TOPIC_COLLECTION_MAP:
                current_topic = config['topic']
                current_collection = config['collection']

                print(f"\n{'='*15} BAT DAU XU LY CHO TOPIC: '{current_topic}' -> COLLECTION: '{current_collection}' {'='*15}")

                df_topic_specific = df_parsed_all.filter(col("topic") == current_topic)
                record_count_topic = df_topic_specific.count()

                if record_count_topic > 0:
                    print(f"Tim thay {record_count_topic} ban ghi cho topic '{current_topic}'.")

                    # --- CHUẨN BỊ DỮ LIỆU: Giữ lại các cột cần thiết ---
                    # Chọn tất cả các cột muốn giữ lại + cột 'text' cho model
                    # Đổi tên 'text' thành 'tweet' để khớp với input của model (nếu cần)
                    df_for_prediction = df_topic_specific.select(
                         col("id"),
                         col("text").alias("tweet"), # Cột input cho pipeline model
                         # Các cột muốn giữ lại trong kết quả cuối cùng:
                         col("createdAt"),
                         col("author"), # Giữ cả struct author
                         col("likeCount"),
                         col("quoteCount"),
                         col("replyCount"),
                         col("retweetCount"),
                         col("viewCount"),
                         col("conversationId"),
                         col("url")
                     ).filter(col("tweet").isNotNull() & (col("tweet") != "")) # Lọc text rỗng

                    print("Da chuan bi du lieu (bao gom cac cot can giu lai) cho model.")

                    if df_for_prediction.count() == 0:
                        print(f"Khong co du lieu hop le (non-null tweet) cho model trong topic '{current_topic}'. Bo qua.")
                        continue

                    # --- Áp dụng Model (Pipeline) ---
                    # Pipeline sẽ chỉ sử dụng cột 'tweet' và các cột trung gian nó tạo ra,
                    # các cột khác sẽ được truyền qua DataFrame kết quả.
                    print("*"*10 + f" BAT DAU AP DUNG PIPELINE MODEL CHO TOPIC '{current_topic}' " + "*"*10)
                    predictions_df = pipeline_model.transform(df_for_prediction).cache() # Apply và cache kết quả
                    print(f"Da ap dung model va cache ket qua. So luong ban ghi du doan: {predictions_df.count()}")
                    # predictions_df.printSchema() # Bỏ comment để xem schema sau khi transform

                    # --- Chọn các cột cuối cùng để lưu ---
                    # Bao gồm ID, text gốc, dự đoán và các cột đã giữ lại
                    df_final_output = predictions_df.select(
                        col("id").alias("tweet_id"), # Đổi tên id
                        col("cleaned_text_for_tokenizer").alias("processed_text"), # Text gốc
                        col("prediction").cast(StringType()).alias("sentiment_prediction"), # Dự đoán
                        # Các cột đã giữ lại từ df_for_prediction:
                        col("createdAt"),
                        col("author"), # Lưu cả struct author
                        # col("author.userName").alias("author_username"), # HOẶC chỉ lưu username
                        col("likeCount"),
                        col("quoteCount"),
                        col("replyCount"),
                        col("retweetCount"),
                        col("viewCount"),
                        col("conversationId"),
                        col("url")
                    )

                    print(f"Da chon cac cot cuoi cung de ghi vao MongoDB.")
                    # df_final_output.show(5, truncate=False) # Xem thử kết quả

                    # --- Ghi kết quả vào MongoDB ---
                    print(f"Dang ghi {df_final_output.count()} ket qua vao MongoDB Collection: '{current_collection}'...")

                    df_final_output.write \
                        .format("mongodb") \
                        .mode("append") \
                        .option("database", MONGO_DATABASE) \
                        .option("collection", current_collection) \
                        .save()

                    print(f"Da ghi thanh cong ket qua vao collection '{current_collection}'.")
                    predictions_df.unpersist() # Giải phóng cache sau khi ghi
                    print(f"Da unpersist cache cho topic '{current_topic}'.")

                else:
                    print(f"Khong tim thay ban ghi nao cho topic '{current_topic}' trong du lieu da doc tu Kafka.")

            print(f"\n{'='*15} HOAN TAT XU LY CHO TAT CA CAC TOPICS {'='*15}")

        else:
            print("Khong co du lieu nao duoc doc tu Kafka de xu ly.")

    except Exception as e:
        print(f"\n--- DA XAY RA LOI TRONG QUA TRINH XU LY CHINH ---", file=sys.stderr)
        print(f"Chi tiet loi: {e}", file=sys.stderr)
        traceback.print_exc()
        print("--------------------------------------------", file=sys.stderr)

    finally:
        if spark:
            print("\nDang dung Spark Session...")
            spark.stop()
            print("Spark Session da dung.")
    print("Hoan tat xu ly Batch.")