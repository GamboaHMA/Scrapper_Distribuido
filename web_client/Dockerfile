FROM python:3.11-slim

WORKDIR /app

# Copiar código base compartido
COPY base_node/ /app/base_node/

# Copiar cliente web
COPY web_client/ /app/web_client/

WORKDIR /app/web_client

# Puerto del servidor web
EXPOSE 8080

# Variables de entorno por defecto
ENV ROUTER_HOST=router
ENV ROUTER_PORT=7070
ENV WEB_PORT=8080

# Comando por defecto
CMD ["python3", "web_client.py"]
