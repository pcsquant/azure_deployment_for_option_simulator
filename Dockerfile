FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOST=0.0.0.0 \
    PORT=8000 \
    PARQUET_BASE_PATH=/app/data \
    OPTION_PARQUET_BASE_PATH=/app/data \
    SHARED_OPTION_CACHE_DIR=/app/shared_cache/option_chain

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/data /app/shared_cache/option_chain

EXPOSE 8000

CMD ["gunicorn", "-w", "2", "--threads", "2", "--timeout", "180", "-b", "0.0.0.0:8000", "simulator:app"]
