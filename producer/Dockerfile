FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
RUN pip install --no-cache-dir "kafka-python>=2.1" prometheus-client
COPY gpu_telemetry_producer.py /app/
CMD ["python", "/app/gpu_telemetry_producer.py"]
