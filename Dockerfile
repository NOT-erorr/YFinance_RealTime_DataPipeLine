FROM python:3.12-slim

# Set working dir
WORKDIR /app

# Copy requirements trước để tận dụng cache
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Run app
CMD ["python", "producer/fetch_data.py"]