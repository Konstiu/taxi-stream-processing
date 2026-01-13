FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bulk_producer.py .
ENTRYPOINT [ "python", "/app/bulk_producer.py" ]

ENV BOOTSTRAP=broker:29092 \
	TOPIC=taxi-locations \
	GLOB=/data/**/*.txt \
	SPEEDUP=0 \
	COMPRESSION=gzip \
	LINGER_MS=50 \
	BATCH_BYTES=65536 \
	MAX_FILES=0

