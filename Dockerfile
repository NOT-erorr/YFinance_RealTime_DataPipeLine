# Dùng Debian Bookworm để hỗ trợ Java 17 ổn định
FROM python:3.10-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 1. Cài đặt Java và công cụ
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    iputils-ping \
    openjdk-17-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2. Tạo thư mục chứa Jar driver (Nằm ở /opt để không bị code đè mất)
RUN mkdir -p /opt/jars

# 3. Tải JDBC Driver (Lưu ý: Lưu đúng vào /opt/jars)
RUN curl -o /opt/jars/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "src/producers/stock_producer.py"]