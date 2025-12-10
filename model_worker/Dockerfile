FROM python:3.10

WORKDIR /app

# 1. Install system dependencies (rarely change) -> CACHED
RUN apt-get update && apt-get install -y \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# 2. Install Python dependencies (change sometimes) -> CACHED
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copy Application Code (changes often)
COPY model_worker.py .

CMD ["python", "model_worker.py"]
