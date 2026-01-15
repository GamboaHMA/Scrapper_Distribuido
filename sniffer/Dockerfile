FROM python:3.11-slim

# instalar tcdump y socat para redireccion
RUN apt-get update && apt-get install -y tcpdump iproute2 net-tools && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY sniffer/sniffer.py .

COPY /certs/ ./certs/

RUN ls -la certs/ && echo "certificados copiados correctamente"

EXPOSE 7070