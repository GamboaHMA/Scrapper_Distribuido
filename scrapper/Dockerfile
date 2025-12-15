FROM python:3.11-slim

WORKDIR /app

# Copiar primero los requisitos para aprovechar la caché de Docker
COPY scrapper/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copiar la estructura de directorios necesaria para imports
# ScrapperNode hereda de base_node.node.Node
COPY base_node ./base_node

# Copiar el código del scrapper
COPY scrapper/scrapper_main.py scrapper/scrapper.py ./scrapper/
COPY scrapper/utils ./scrapper/utils

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