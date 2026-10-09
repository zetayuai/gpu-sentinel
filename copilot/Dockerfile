FROM python:3.12-slim
RUN pip install --no-cache-dir streamlit clickhouse-connect trino openai pandas requests
COPY app.py senso.py /app/
COPY playbooks /app/playbooks
EXPOSE 8501
CMD ["streamlit", "run", "/app/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
