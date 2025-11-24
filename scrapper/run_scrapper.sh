#!/bin/bash

echo "Instalando dependencias..."
pip install -r requirements.txt

echo "Ejecutando el cliente de scraping..."
# Puedes configurar estas variables para conectarte a un servidor específico
export SERVER_HOST="0.0.0.0"
export SERVER_PORT=8080

python client.py