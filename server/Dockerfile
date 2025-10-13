FROM python:3.11-slim

WORKDIR /app

# Crear un archivo requirements.txt (vacío por ahora)
RUN touch requirements.txt && pip install --no-cache-dir -r requirements.txt

# Copiar el código del servidor
COPY server.py .

# Exponer el puerto en el que escuchará el servidor
EXPOSE 8080

# Definir variables de entorno
ENV PYTHONUNBUFFERED=1

# Comando para ejecutar el servidor
CMD ["python3", "server.py"]