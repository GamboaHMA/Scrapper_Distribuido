FROM python:3.11-slim

WORKDIR /app

# Copiar código del registry
COPY registry.py .
COPY requirements.txt .

# Instalar dependencias si las hay
RUN pip install --no-cache-dir -r requirements.txt || true

# Exponer puerto del registry (DNS)
EXPOSE 5353

# Variables de entorno
ENV PYTHONUNBUFFERED=1
ENV NETWORK_RANGE=192.168.1.0/24
ENV SCAN_PORT=8080
ENV REGISTRY_PORT=5353

# Comando para ejecutar el registry
CMD ["python3", "registry.py"]
