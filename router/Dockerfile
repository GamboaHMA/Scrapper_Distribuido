# Dockerfile para Router Node
FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copiar base_node (desde raíz del proyecto - incluye utils)
COPY base_node ./base_node

# Copiar archivos del router
COPY router/router_main.py ./router/router_main.py
COPY router/requirements.txt ./router/requirements.txt

# Instalar dependencias
RUN pip install --no-cache-dir -r ./router/requirements.txt

# Variables de entorno
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV LOG_LEVEL=INFO

# Puerto del router
EXPOSE 9090

# Comando para ejecutar el router
CMD ["python3", "-m", "router.router_main"]
