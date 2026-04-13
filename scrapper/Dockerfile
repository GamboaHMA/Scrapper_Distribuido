FROM python:3.11-slim

WORKDIR /app

# Copiar primero los requisitos para aprovechar la caché de Docker
COPY scrapper/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# El código fuente se monta como volumen en tiempo de ejecución.
# Definir variables de entorno
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
# Dejar SERVER_HOST sin definir para activar el autodescubrimiento
# ENV SERVER_HOST=mi-scrp-server
ENV SERVER_PORT=8080
ENV BROADCAST_PORT=8081
ENV AUTO_DISCOVERY=true

# Comando para ejecutar la aplicación
CMD ["python3", "-m", "scrapper.scrapper_main"]