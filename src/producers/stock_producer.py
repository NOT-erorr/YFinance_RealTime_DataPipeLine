import time
import os
import json
import logging
import yfinance as yf # Import chuẩn hơn
from kafka import KafkaProducer
import pandas as pd

# Cấu hình Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("StockProducer")

import sys
# Import utils
# Import utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.utils.db_connector import get_kafka_config

class StockProducer:
    def __init__(self, topic='stock_market_data'):
        self.topic = topic
        
        # --- LOAD CONFIG TỰ ĐỘNG ---
        kafka_conf = get_kafka_config('producer')
        logger.info(f"⚙️ Loaded Kafka Config: {kafka_conf}")

        try:
            # Truyền thẳng dict config vào KafkaProducer (**kwargs)
            self.producer = KafkaProducer(
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                **kafka_conf 
            )
            logger.info("✅ Kafka Producer initialized.")
        except Exception as e:
            logger.error(f"❌ Failed to init Kafka: {e}")
            raise

    def fetch_stock_data(self, symbol):
        try:
            # Tạo Ticker object
            stock = yf.Ticker(symbol)
            
            # Lấy dữ liệu 1 ngày qua, interval 1 phút
            # Quan trọng: yfinance tự động xử lý session, nhưng nếu lỗi nhiều cần thêm proxy
            data = stock.history(period="1d", interval="1m")
            
            if not data.empty:
                latest_data = data.iloc[-1] # Lấy dòng mới nhất
                
                stock_info = {
                    'symbol': symbol,
                    'timestamp': str(latest_data.name), # Convert timestamp to string
                    'open': float(latest_data['Open']),
                    'high': float(latest_data['High']),
                    'low': float(latest_data['Low']),
                    'close': float(latest_data['Close']),
                    'volume': int(latest_data['Volume'])
                }
                return stock_info
            else:
                logger.warning(f"⚠️ No data found for {symbol}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error fetching {symbol}: {e}")
            return None

    def run(self, symbol_file='SP500_symbol.txt'):
        # 1. Đọc danh sách mã trước
        if not os.path.exists(symbol_file):
            logger.error(f"File {symbol_file} not found!")
            return

        with open(symbol_file, 'r') as f:
            symbols = [line.strip() for line in f.readlines() if line.strip()]

        logger.info(f"🚀 Starting producer for {len(symbols)} symbols...")

        # 2. Vòng lặp vô tận nằm ở ngoài cùng (Theo thời gian)
        try:
            # tạo testtime 30s
            test_time = 300  # Thời gian chạy thử nghiệm trong giây (ví dụ: 300 giây = 5 phút)
            start_test = time.time()
            while True:
                if time.time() - start_test > test_time:
                    logger.info("🛑 Test time reached. Stopping producer...")
                    break
                start_time = time.time()
                logger.info("--- Starting new fetch cycle ---")
                
                # 3. Quét qua từng mã trong danh sách
                for symbol in symbols:
                    stock_data = self.fetch_stock_data(symbol)
                    
                    if stock_data:
                        # Gửi vào Kafka
                        self.producer.send(self.topic, value=stock_data)
                        logger.info(f"Sent: {symbol} at {stock_data['timestamp']}")
                    
                    # Ngủ cực ngắn giữa các request để tránh bị Yahoo coi là DDOS
                    time.sleep(1) 

                # Tính thời gian đã chạy
                elapsed = time.time() - start_time
                logger.info(f"--- Cycle finished in {elapsed:.2f}s ---")
                
                # Nếu quét xong nhanh hơn 60s thì ngủ cho đủ phút rồi mới quét lại
                # Nếu quét lâu hơn 60s thì chạy tiếp luôn
                sleep_time = max(0, 60 - elapsed)
                if sleep_time > 0:
                    logger.info(f"Sleeping for {sleep_time:.2f}s before next cycle...")
                    time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("🛑 Stopping Stock Producer...")
            self.producer.close()

if __name__ == "__main__":
    # Khởi tạo và chạy
    # Đảm bảo bạn đã có file SP500_symbol.txt cùng thư mục
    producer = StockProducer(topic='stock_market_data') 
    producer.run(symbol_file='SP500_symbol.txt')