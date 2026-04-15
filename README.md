# 📈 Module Producer: Yahoo Finance Real-Time Data Streamer

Module này đóng vai trò là **Producer** trong hệ thống Data Pipeline. Nhiệm vụ chính là thu thập dữ liệu giá cổ phiếu thời gian thực từ Yahoo Finance và đẩy vào **Kafka topic** để các module khác (như Spark hoặc DuckDB) xử lý.

## 🚀 Tính năng chính
* **Tự động hóa:** Lấy dữ liệu giá S&P 500 theo chu kỳ (mặc định 60 giây).
* **Xử lý bất đồng bộ (Async):** Sử dụng `aiokafka` để gửi dữ liệu hiệu suất cao, không gây nghẽn luồng.
* **Xử lý lỗi thông minh (Mock Fallback):** Nếu Yahoo Finance gặp lỗi hoặc không trả về dữ liệu, hệ thống tự động sinh dữ liệu giả lập (mock data) để đảm bảo pipeline không bị ngắt quãng.
* **Tối ưu hóa Batch:** Chia nhỏ danh sách cổ phiếu thành các batch (mặc định 50 mã) để tránh bị API giới hạn.

## 🛠 Yêu cầu hệ thống
Để chạy được module này, bạn cần cài đặt các thư viện Python sau:

```bash
pip install yfinance aiokafka asyncio