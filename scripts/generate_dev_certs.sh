#!/usr/bin/env bash
set -euo pipefail

# Small helper to generate a CA and a server certificate signed by that CA.
# The generated files are placed under ./certs:
# - certs/ca.key (CA private key)
# - certs/ca.crt (CA certificate)
# - certs/server.key (server private key)
# - certs/server.crt (server certificate signed by CA)
# Use certs/ca.crt as the TLS CA on clients and server.crt/server.key for servers.

OUT_DIR="$(dirname "$0")/../certs"
mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

echo "Generating CA key and certificate..."
openssl genrsa -out ca.key 2048
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 -subj "/CN=ScrapperLocalCA" -out ca.crt

echo "Generating server key and CSR..."
openssl genrsa -out server.key 2048
openssl req -new -key server.key -subj "/CN=localhost" -out server.csr

echo "Signing server certificate with CA..."
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 3650 -sha256

# Cleanup
rm -f server.csr ca.srl

echo "Certificates generated in: $OUT_DIR"
echo "- CA cert: $OUT_DIR/ca.crt"
echo "- Server cert: $OUT_DIR/server.crt"
echo "- Server key: $OUT_DIR/server.key"

echo "To run nodes with TLS, pass the paths to the Node constructor, e.g. in Python:" 
echo "  node = Node('router', use_tls=True, tls_certfile='certs/server.crt', tls_keyfile='certs/server.key', tls_cafile='certs/ca.crt')"

exit 0
