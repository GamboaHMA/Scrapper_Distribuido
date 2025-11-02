#!/bin/bash

# Script para probar la API REST del scrapper distribuido

API_HOST="localhost"
API_PORT="8082"
BASE_URL="http://$API_HOST:$API_PORT"

echo "=== Probando API REST del Scrapper Distribuido ==="
echo "URL base: $BASE_URL"
echo ""

# Test 1: Health check
echo "1. Probando health check..."
curl -s "$BASE_URL/api/health" | python3 -m json.tool
echo ""

# Test 2: Estado de la API
echo "2. Probando estado de la API..."
curl -s "$BASE_URL/api/status" | python3 -m json.tool
echo ""

# Test 3: Información básica
echo "3. Probando endpoint raíz..."
curl -s "$BASE_URL/" | python3 -m json.tool
echo ""

# Test 4: Enviar una URL para scraping
echo "4. Enviando URL para scraping..."
curl -s -X POST "$BASE_URL/api/scrape" \
  -H "Content-Type: application/json" \
  -d '{"url": "https://www.animeflv.net"}' | python3 -m json.tool
echo ""

# Test 5: Enviar múltiples URLs
echo "5. Enviando múltiples URLs para scraping..."
curl -s -X POST "$BASE_URL/api/scrape/batch" \
  -H "Content-Type: application/json" \
  -d '{
    "urls": [
      "https://www.python.org",
      "https://www.animeflv.net",
      "https://www.docker.com"
    ]
  }' | python3 -m json.tool
echo ""

echo "=== Pruebas completadas ==="