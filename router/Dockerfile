# Dockerfile para Router Node
FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias del sistema
COPY router/requirements.txt ./router/requirements.txt
# Instalar dependencias
RUN pip install --no-cache-dir -r ./router/requirements.txt

# El código fuente se monta como volumen en tiempo de ejecución.
# Variables de entorno
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV LOG_LEVEL=INFO

# Puerto del router
EXPOSE 9090

# Comando para ejecutar el router
CMD ["python3", "-m", "router.router_main"]
