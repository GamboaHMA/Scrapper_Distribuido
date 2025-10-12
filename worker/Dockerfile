FROM python:3.11-slim

WORKDIR /app

# Copiar primero los requisitos para aprovechar la caché de Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código de la aplicación
COPY worker.py .

# Exponer el puerto en el que se ejecutará Flask
EXPOSE 5000

# Definir variables de entorno
ENV PYTHONUNBUFFERED=1

# Comando para ejecutar la aplicación
CMD ["python", "worker.py"]