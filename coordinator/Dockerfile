FROM python:3.11-slim

WORKDIR /app

# Crear un archivo requirements.txt (vacío por ahora)
RUN touch requirements.txt && pip install --no-cache-dir -r requirements.txt

# Copiar el código del coordinator
COPY coordinator.py .
COPY gateway.py .
COPY requirements.txt .

# Exponer los puertos en los que escuchará el coordinator
EXPOSE 8080
EXPOSE 8081/udp
EXPOSE 8082

# Definir variables de entorno
ENV PYTHONUNBUFFERED=1
ENV BROADCAST_PORT=8081

# Comando para ejecutar el coordinator
CMD ["python3", "coordinator.py"]