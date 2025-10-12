FROM python:3.11-slim

WORKDIR /app

# Copiar primero los requisitos para aprovechar la caché de Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código de la aplicación
COPY client.py .

# Definir variables de entorno
ENV PYTHONUNBUFFERED=1
ENV SERVER_HOST=mi-scrp-server
ENV SERVER_PORT=8080

# Comando para ejecutar la aplicación
CMD ["python3", "client.py"]