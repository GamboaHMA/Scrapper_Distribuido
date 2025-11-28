# Dockerfile para DNS Service
FROM python:3.11-alpine

WORKDIR /app

# Instalar dependencias del sistema
RUN apk add --no-cache gcc musl-dev

# Copiar archivo de requisitos
COPY requirements.txt .

# Instalar dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código del DNS
COPY dns.py .

# Exponer puerto del DNS
EXPOSE 5353

# Variables de entorno por defecto
# ENV DNS_HOST=0.0.0.0
ENV DNS_PORT=5353
ENV SCAN_PORT=8080
# ENV NETWORK_RANGE=

# Comando de inicio
CMD ["python", "dns.py"]