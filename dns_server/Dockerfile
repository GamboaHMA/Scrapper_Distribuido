FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY dns_server.py .
COPY utils/ ./utils/

CMD ["python3", "dns_server.py"]