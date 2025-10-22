FROM python:3.11-slim

WORKDIR /app

# Crear un archivo requirements.txt (vacío por ahora)
RUN touch requirements.txt && pip install --no-cache-dir -r requirements.txt

# Copiar el código del servidor
COPY server.py .

# Exponer los puertos en los que escuchará el servidor
EXPOSE 8080
EXPOSE 8081/udp

# Definir variables de entorno
ENV PYTHONUNBUFFERED=1
ENV BROADCAST_PORT=8081

# Comando para ejecutar el servidor
CMD ["python3", "server.py"]