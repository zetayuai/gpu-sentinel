# Build context is the repo root (see docker-compose.yml) so playbooks/ is shared.
FROM python:3.12-slim
RUN pip install --no-cache-dir streamlit clickhouse-connect trino "openai<2" "httpx<0.28" pandas requests
COPY copilot/app.py copilot/senso.py /app/
COPY playbooks /app/playbooks
EXPOSE 8501
CMD ["streamlit", "run", "/app/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
