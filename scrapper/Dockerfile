FROM python:3.11-slim

WORKDIR /app

# Copiar primero los requisitos para aprovechar la caché de Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código de la aplicación
COPY scrapper_main.py scrapper.py node_connection.py ./

# Definir variables de entorno
ENV PYTHONUNBUFFERED=1
# Dejar SERVER_HOST sin definir para activar el autodescubrimiento
# ENV SERVER_HOST=mi-scrp-server
ENV SERVER_PORT=8080
ENV BROADCAST_PORT=8081
ENV AUTO_DISCOVERY=true

# Comando para ejecutar la aplicación
CMD ["python3", "scrapper_main.py"]