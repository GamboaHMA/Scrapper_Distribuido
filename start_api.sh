#!/bin/bash

# Script para iniciar la API REST del scrapper distribuido

echo "Iniciando API REST para Scrapper Distribuido..."

# Verificar si Python está instalado
if ! command -v python3 &> /dev/null; then
    echo "Error: Python3 no está instalado"
    exit 1
fi

# Verificar si pip está instalado
if ! command -v pip3 &> /dev/null; then
    echo "Error: pip3 no está instalado"
    exit 1
fi

# Instalar dependencias si es necesario
echo "Verificando dependencias..."
pip3 install -r requirements_api.txt

# Configurar variables de entorno si no están definidas
export API_HOST=${API_HOST:-"0.0.0.0"}
export API_PORT=${API_PORT:-"8082"}
export SERVER_HOST=${SERVER_HOST:-"localhost"}
export SERVER_PORT=${SERVER_PORT:-"8080"}

echo "Configuración:"
echo "  - API escuchando en: $API_HOST:$API_PORT"
echo "  - Servidor principal en: $SERVER_HOST:$SERVER_PORT"
echo ""

# Iniciar la API
echo "Iniciando API REST..."
python3 server/api_server.py