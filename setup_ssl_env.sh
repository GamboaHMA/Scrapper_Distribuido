#!/bin/bash

# Ejemplo de uso de certificados SSL con tus nodos
# Ejecuta este script antes de iniciar tus nodos

# 1. Generar certificados si no existen
if [ ! -f "./certs/server.crt" ] || [ ! -f "./certs/server.key" ]; then
    echo "Generando certificados SSL..."
    ./setup_certs.sh
fi

# 2. Exportar variables de entorno
export SSL_CERTFILE="./certs/server.crt"
export SSL_KEYFILE="./certs/server.key"
export SSL_VERIFY_MODE=NONE

echo "Variables SSL configuradas:"
echo "SSL_CERTFILE=$SSL_CERTFILE"
echo "SSL_KEYFILE=$SSL_KEYFILE"
echo "SSL_VERIFY_MODE=$SSL_VERIFY_MODE"

# 3. Verificar que los certificados son válidos
echo ""
echo "Verificando certificados..."
openssl x509 -in "$SSL_CERTFILE" -noout -dates
openssl rsa -in "$SSL_KEYFILE" -noout -check

echo ""
echo "✅ Configuración SSL completa. Ahora puedes iniciar tus nodos con cifrado TLS."