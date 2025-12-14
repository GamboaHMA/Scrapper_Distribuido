#!/bin/bash

echo "Instalando dependencias..."
pip install -r requirements.txt

echo "Ejecutando el scrapper de scraping..."
# Puedes configurar estas variables para conectarte a un servidor específico
export COORDINATOR_HOST="0.0.0.0"
export COORDINATOR_PORT=8080

python scrapper_main.py