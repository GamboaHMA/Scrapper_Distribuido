#!/bin/bash

# Script de demostración completa del sistema Scrapper Distribuido

echo "🕷️  DEMO: Sistema Scrapper Distribuido con API REST + Dashboard Web"
echo "=================================================================="
echo ""

# Verificar dependencias
echo "📋 Verificando dependencias..."

if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 no encontrado. Instalar Python 3.x"
    exit 1
fi

if ! python3 -c "import flask" &> /dev/null; then
    echo "⚠️  Flask no encontrado. Instalando..."
    pip3 install flask requests
fi

echo "✅ Dependencias verificadas"
echo ""

# Función para verificar si un puerto está en uso
check_port() {
    netstat -tlnp 2>/dev/null | grep -q ":$1 "
}

# Verificar puertos
echo "🔍 Verificando puertos..."
if check_port 8080; then
    echo "⚠️  Puerto 8080 en uso (posiblemente servidor ya corriendo)"
else
    echo "✅ Puerto 8080 disponible"
fi

if check_port 8082; then
    echo "⚠️  Puerto 8082 en uso (posiblemente API ya corriendo)"
else
    echo "✅ Puerto 8082 disponible"
fi
echo ""

# Mostrar URLs importantes
echo "🌐 URLs del sistema:"
echo "   • Dashboard Web: http://localhost:8082/dashboard"
echo "   • API Health:    http://localhost:8082/api/health"
echo "   • API Status:    http://localhost:8082/api/status"
echo ""

# Instrucciones de uso
echo "📝 Para usar el sistema completo:"
echo ""
echo "1️⃣  SERVIDOR PRINCIPAL (Terminal 1):"
echo "   cd $(pwd)"
echo "   python3 server/server.py"
echo ""
echo "2️⃣  API REST (Terminal 2):"
echo "   cd $(pwd)"
echo "   ./start_api.sh"
echo ""
echo "3️⃣  DASHBOARD WEB (Navegador):"
echo "   ./open_dashboard.sh"
echo "   # O abrir manualmente: http://localhost:8082/dashboard"
echo ""

# Ejemplo de uso de API
echo "🔧 Ejemplos de uso de la API:"
echo ""
echo "   # Health check"
echo "   curl http://localhost:8082/api/health"
echo ""
echo "   # Enviar URL simple"
echo "   curl -X POST http://localhost:8082/api/scrape \\"
echo "        -H 'Content-Type: application/json' \\"
echo "        -d '{\"url\": \"https://httpbin.org/html\"}'"
echo ""
echo "   # Enviar múltiples URLs"
echo "   curl -X POST http://localhost:8082/api/scrape/batch \\"
echo "        -H 'Content-Type: application/json' \\"
echo "        -d '{\"urls\": [\"https://example.com\", \"https://httpbin.org/json\"]}'"
echo ""

# Test automático
echo "🧪 ¿Ejecutar test automático? (y/n)"
read -r response

if [[ "$response" =~ ^[Yy]$ ]]; then
    echo ""
    echo "🚀 Ejecutando test automático..."
    echo ""
    
    # Verificar si la API está corriendo
    if curl -s http://localhost:8082/api/health > /dev/null 2>&1; then
        echo "✅ API disponible - ejecutando tests..."
        ./test_api.sh
    else
        echo "❌ API no disponible. Ejecutar primero:"
        echo "   ./start_api.sh"
    fi
else
    echo "📋 Tests saltados. Para ejecutar manualmente:"
    echo "   ./test_api.sh"
fi

echo ""
echo "🎯 RESUMEN:"
echo "   • Servidor principal:  Puerto 8080 (TCP + UDP 8081)"
echo "   • API REST:           Puerto 8082"
echo "   • Dashboard Web:      http://localhost:8082/dashboard"
echo "   • Documentación API:  README_API.md"
echo "   • Documentación Web:  README_DASHBOARD.md"
echo ""
echo "¡Sistema listo para usar! 🚀"