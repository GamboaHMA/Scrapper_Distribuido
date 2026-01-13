FROM python:3.11-slim

WORKDIR /app

# Copiar archivos necesarios
COPY base_node/ /app/base_node/
COPY client/client.py /app/client/client.py

# Variables de entorno (pueden sobrescribirse al ejecutar)
ENV ROUTER_HOST=router
ENV ROUTER_PORT=7070
ENV LOG_LEVEL=INFO

# Punto de entrada
CMD ["python", "-u", "/app/client/interactive_client.py"]
