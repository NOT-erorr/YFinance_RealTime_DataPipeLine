# Sử dụng Python 3.9 Slim (Nhẹ, ổn định cho Data Engineering)
FROM python:3.10-slim

# Thiết lập biến môi trường
# PYTHONDONTWRITEBYTECODE=1: Không sinh ra file .pyc (rác)
# PYTHONUNBUFFERED=1: Log in ra ngay lập tức (quan trọng để debug trong Docker)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Cài đặt các thư viện hệ thống cần thiết
# gcc & libpq-dev: Bắt buộc để cài thư viện psycopg2 (kết nối Postgres)
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    iputils-ping \
    && rm -rf /var/lib/apt/lists/*

# Thiết lập thư mục làm việc bên trong container
WORKDIR /app

# 1. Copy file requirements trước để tận dụng Docker Cache
COPY requirements.txt .

# 2. Cài đặt thư viện Python
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copy toàn bộ mã nguồn vào container
COPY . .

# Lệnh mặc định (sẽ bị ghi đè bởi docker-compose)
CMD ["python", "src/producers/stock_producer.py"]