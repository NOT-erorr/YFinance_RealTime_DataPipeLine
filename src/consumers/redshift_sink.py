import json
import time
import logging
import psycopg2
from psycopg2 import extras
from kafka import KafkaConsumer
import sys
import os

# Import các hàm cấu hình từ utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.utils.db_connector import load_config, get_kafka_config

# --- CẤU HÌNH ---
KAFKA_TOPIC = "stock_market_data"
BATCH_SIZE = 100
FLUSH_INTERVAL = 10

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RedshiftSink")

class RedshiftSink:
    def __init__(self):
        # 1. LOAD CONFIG TỰ ĐỘNG (Sẽ ưu tiên biến môi trường KAFKA_BOOTSTRAP_SERVER)
        kafka_conf = get_kafka_config('consumer')
        db_config = load_config(filename='config/database.ini', section='postgresql')

        logger.info(f"⚙️ Kafka Config: {kafka_conf}")
        logger.info(f"⚙️ DB Config Host: {db_config.get('host')}")

        # 2. Kết nối Kafka Consumer
        try:
            # Truyền thẳng config vào KafkaConsumer (**kafka_conf)
            # Lưu ý: value_deserializer cần define riêng vì nó là hàm lambda
            self.consumer = KafkaConsumer(
                KAFKA_TOPIC,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                **kafka_conf 
            )
            logger.info("✅ Kafka Consumer connected successfully.")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Kafka: {e}")
            raise
        
        # 3. Kết nối Database
        try:
            self.conn = psycopg2.connect(**db_config)
            self.conn.autocommit = True
            self.cursor = self.conn.cursor()
            logger.info("✅ Connected to Database successfully.")
            self.create_table_if_not_exists()
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise

    def create_table_if_not_exists(self):
        create_query = """
        CREATE TABLE IF NOT EXISTS raw_stock_data (
            symbol VARCHAR(20),
            record_time VARCHAR(50), 
            open_price FLOAT,
            high_price FLOAT,
            low_price FLOAT,
            close_price FLOAT,
            volume BIGINT,
            ingest_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        self.cursor.execute(create_query)

    def flush_to_db(self, buffer):
        if not buffer:
            return

        insert_query = """
            INSERT INTO raw_stock_data 
            (symbol, record_time, open_price, high_price, low_price, close_price, volume)
            VALUES %s
        """
        
        values = [
            (
                msg['symbol'],
                msg['timestamp'],
                msg['open'],
                msg['high'],
                msg['low'],
                msg['close'],
                msg['volume']
            )
            for msg in buffer
        ]

        try:
            extras.execute_values(self.cursor, insert_query, values)
            logger.info(f"💾 Flushed {len(values)} records to Database.")
        except Exception as e:
            logger.error(f"❌ Error inserting data: {e}")

    def run(self):
        logger.info("🚀 Starting Redshift Sink Consumer...")
        buffer = []
        last_flush_time = time.time()

        try:
            # Vòng lặp chính để nhận tin nhắn
            for message in self.consumer:
                data = message.value
                buffer.append(data)

                current_time = time.time()
                is_batch_full = len(buffer) >= BATCH_SIZE
                is_time_up = (current_time - last_flush_time) >= FLUSH_INTERVAL

                if (is_batch_full or is_time_up) and buffer:
                    self.flush_to_db(buffer)
                    buffer = [] 
                    last_flush_time = current_time

        except KeyboardInterrupt:
            logger.info("🛑 Stopping sink...")
            if buffer:
                self.flush_to_db(buffer)
            self.cursor.close()
            self.conn.close()

if __name__ == "__main__":
    # Thêm delay nhỏ để chờ Kafka khởi động xong hẳn
    time.sleep(10) 
    sink = RedshiftSink()
    sink.run()