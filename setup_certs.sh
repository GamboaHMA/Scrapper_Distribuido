#!/bin/bash

# Script para generar certificados SSL autofirmados para pruebas locales
# Uso: ./setup_certs.sh [directorio_destino]

set -e

# Directorio donde crear los certificados (por defecto: ./certs)
CERT_DIR="${1:-./certs}"

echo " Generando certificados SSL en: $CERT_DIR"

# Crear directorio si no existe
mkdir -p "$CERT_DIR"

# Generar clave privada y certificado autofirmado
openssl req -x509 -newkey rsa:4096 -nodes -keyout "$CERT_DIR/server.key" -out "$CERT_DIR/server.crt" -days 365 \
  -subj "/C=MX/ST=Estado/L=Ciudad/O=Universidad/CN=localhost"

# Verificar que se crearon los archivos
if [ -f "$CERT_DIR/server.crt" ] && [ -f "$CERT_DIR/server.key" ]; then
    echo " Certificados generados exitosamente:"
    echo "   - Certificado: $CERT_DIR/server.crt"
    echo "   - Clave privada: $CERT_DIR/server.key"
    echo ""
    echo " Para usar en tu aplicación:"
    echo "   export SSL_CERTFILE=\"$CERT_DIR/server.crt\""
    echo "   export SSL_KEYFILE=\"$CERT_DIR/server.key\""
    echo "   export SSL_VERIFY_MODE=NONE"
    echo ""
    echo " Verificar certificado:"
    echo "   openssl x509 -in $CERT_DIR/server.crt -text -noout"
else
    echo " Error: No se pudieron generar los certificados"
    exit 1
fi