#!/bin/bash

# Script para abrir el dashboard web del scrapper distribuido

API_HOST="localhost"
API_PORT="8082"
DASHBOARD_URL="http://$API_HOST:$API_PORT/dashboard"

echo "=== Dashboard Scrapper Distribuido ==="
echo "URL del dashboard: $DASHBOARD_URL"
echo ""

# Verificar si la API está corriendo
echo "Verificando si la API está disponible..."
if curl -s "$DASHBOARD_URL" > /dev/null 2>&1; then
    echo "✅ API disponible en $API_HOST:$API_PORT"
    echo ""
    
    # Intentar abrir el navegador
    if command -v xdg-open > /dev/null 2>&1; then
        echo "🌐 Abriendo dashboard en el navegador..."
        xdg-open "$DASHBOARD_URL"
    elif command -v open > /dev/null 2>&1; then
        echo "🌐 Abriendo dashboard en el navegador..."
        open "$DASHBOARD_URL"
    else
        echo "📋 Abrir manualmente en el navegador:"
        echo "   $DASHBOARD_URL"
    fi
else
    echo "❌ La API no está disponible en $API_HOST:$API_PORT"
    echo ""
    echo "Asegúrate de que la API esté corriendo:"
    echo "   ./start_api.sh"
    echo ""
    echo "Luego ejecuta este script nuevamente."
fi