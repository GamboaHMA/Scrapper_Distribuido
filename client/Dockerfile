FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements

COPY client.py .

CMD ["python3", "client.py"]