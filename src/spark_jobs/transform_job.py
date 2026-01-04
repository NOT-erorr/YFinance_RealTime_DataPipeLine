from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, avg, stddev, current_timestamp
from pyspark.sql.window import Window
import os
import sys

# Setup đường dẫn để import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.utils.db_connector import load_config

def create_spark_session():
    """Khởi tạo Spark Session có kèm JDBC Driver"""
    return SparkSession.builder \
        .appName("StockDataTransformation") \
        .config("spark.jars", "/opt/jars/postgresql-42.6.0.jar") \
        .config("spark.driver.extraClassPath", "/opt/jars/postgresql-42.6.0.jar") \
        .getOrCreate()

def run_job():
    # 1. Load Config DB
    # Lưu ý: Spark chạy trong Docker nên host là 'postgres' chứ không phải localhost
    # Ta dùng trick os.getenv để lấy cấu hình từ biến môi trường Docker
    db_host = os.getenv("DB_HOST", "postgres")
    db_user = os.getenv("POSTGRES_USER", "user")
    db_pass = os.getenv("POSTGRES_PASSWORD", "password")
    db_name = os.getenv("POSTGRES_DB", "stockdb")
    
    jdbc_url = f"jdbc:postgresql://{db_host}:5432/{db_name}"
    db_properties = {
        "user": db_user,
        "password": db_pass,
        "driver": "org.postgresql.Driver"
    }

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN") # Giảm bớt log rác

    print("⏳ Reading data from Raw Table...")
    
    # 2. EXTRACT: Đọc dữ liệu từ bảng raw
    # pushdown_query dùng để chỉ lấy dữ liệu mới nhất (ví dụ) hoặc filter trước
    raw_df = spark.read.jdbc(url=jdbc_url, table="raw_stock_data", properties=db_properties)

    # 3. TRANSFORM
    print("🔄 Transforming data...")
    
    # a. Ép kiểu dữ liệu (String -> Timestamp/Double)
    df_clean = raw_df.withColumn("event_time", to_timestamp(col("record_time"))) \
                     .withColumn("close_price", col("close_price").cast("double")) \
                     .withColumn("volume", col("volume").cast("long"))

    # b. Tạo Window Spec để tính toán theo từng mã chứng khoán, sắp xếp theo thời gian
    # Window này dùng để tính Moving Average (MA)
    window_spec = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(-4, 0) # 5 dòng gần nhất
    window_spec_20 = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(-19, 0) # 20 dòng gần nhất

    # c. Tính toán chỉ số kỹ thuật
    final_df = df_clean.withColumn("sma_5", avg("close_price").over(window_spec)) \
                       .withColumn("sma_20", avg("close_price").over(window_spec_20)) \
                       .withColumn("volatility", stddev("close_price").over(window_spec)) \
                       .withColumn("processed_at", current_timestamp())

    # Chọn các cột cần thiết để lưu
    final_output = final_df.select(
        "symbol", "event_time", "close_price", "volume", 
        "sma_5", "sma_20", "volatility", "processed_at"
    )

    # 4. LOAD: Ghi vào bảng mới 'processed_stocks'
    print("💾 Writing to Processed Table...")
    
    final_output.write.jdbc(
        url=jdbc_url,
        table="processed_stocks",
        mode="append", # append: nối thêm, overwrite: ghi đè
        properties=db_properties
    )
    
    print("✅ Job Finished Successfully!")
    spark.stop()

if __name__ == "__main__":
    run_job()