# SSL/TLS Configuration for Distributed Scraper

Este proyecto ahora soporta cifrado SSL/TLS para todas las comunicaciones entre nodos.

## 🚀 Configuración Rápida

### 1. Generar certificados
```bash
./setup_certs.sh
```

Esto crea:
- `certs/server.crt` - Certificado público
- `certs/server.key` - Clave privada

### 2. Configurar entorno SSL
```bash
./setup_ssl_env.sh
```

O manualmente:
```bash
export SSL_CERTFILE="./certs/server.crt"
export SSL_KEYFILE="./certs/server.key"
export SSL_VERIFY_MODE=NONE
```

### 3. Iniciar nodos
```bash
# Con SSL habilitado
python scrapper/scrapper_main.py

# Sin SSL (por defecto si no hay certificados)
python scrapper/scrapper_main.py
```

## 🔧 Variables de Entorno

| Variable | Descripción | Valores |
|----------|-------------|---------|
| `SSL_CERTFILE` | Ruta al certificado público | Ruta absoluta o relativa |
| `SSL_KEYFILE` | Ruta a la clave privada | Ruta absoluta o relativa |
| `SSL_CAFILE` | Archivo CA para verificación (opcional) | Ruta absoluta o relativa |
| `SSL_VERIFY_MODE` | Modo de verificación de certificados | `NONE`, `REQUIRED` |

## 📋 Comportamiento

- **Con SSL configurado**: Todas las conexiones usan TLS con logging de cipher usado
- **Sin SSL**: Funciona normalmente con sockets TCP planos
- **Certificados inválidos**: El sistema cae a modo sin SSL con warning

## 🔍 Verificación

Verificar que SSL está activo en los logs:
```
🔐 SSL habilitado: certfile=./certs/server.crt, verify_mode=NONE
🔐 SSL server handshake aceptado desde 192.168.1.100:54321 cipher=TLS_AES_256_GCM_SHA384
🔐 SSL client handshake completado con 192.168.1.100 cipher=TLS_AES_256_GCM_SHA384
```

## 🛠️ Troubleshooting

### Error: "SSL no configurado"
- Verifica que `SSL_CERTFILE` y `SSL_KEYFILE` estén exportados
- Asegúrate de que los archivos existan y sean legibles

### Error: "No se pudo inicializar SSL"
- Verifica que los certificados sean válidos: `openssl x509 -in certs/server.crt -text`
- Verifica que la clave sea correcta: `openssl rsa -in certs/server.key -check`

### Conexiones fallan
- Para pruebas locales usa `SSL_VERIFY_MODE=NONE`
- Para producción configura una CA real y usa `SSL_VERIFY_MODE=REQUIRED`