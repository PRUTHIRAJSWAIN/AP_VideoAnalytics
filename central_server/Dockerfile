    FROM python:3.10

    WORKDIR /app

    COPY requirements.txt .
    RUN pip install --no-cache-dir -r requirements.txt

    COPY central_consumer.py .
    RUN mkdir central_server

    CMD ["python", "central_consumer.py"]
