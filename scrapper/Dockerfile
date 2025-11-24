FROM python:3.11-slim

WORKDIR /app

# Copiar primero los requisitos para aprovechar la caché de Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código de la aplicación
COPY scrapper_main.py scrapper.py ./

# Definir variables de entorno
ENV PYTHONUNBUFFERED=1
# Dejar COORDINATOR_HOST sin definir para activar el autodescubrimiento
# ENV COORDINATOR_HOST=mi-coordinator
ENV COORDINATOR_PORT=8080
ENV BROADCAST_PORT=8081
ENV AUTO_DISCOVERY=true

# Comando para ejecutar la aplicación
CMD ["python3", "scrapper_main.py"]