# Dockerfile para DataBase Node
FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias del sistema
COPY database/requirements.txt ./database/requirements.txt
# Instalar dependencias
RUN pip install --no-cache-dir -r ./database/requirements.txt

# Copiar base_node (desde raíz del proyecto - incluye utils)
COPY base_node ./base_node

# Copiar archivos del router
COPY database/database.py ./router/database.py

# Variables de entorno
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV LOG_LEVEL=INFO

# Puerto del router
EXPOSE 7070

# Comando para ejecutar el router
CMD ["python3", "-m", "database.database"]
