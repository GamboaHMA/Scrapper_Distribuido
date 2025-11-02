# API REST para Scrapper Distribuido

API REST simple que se conecta al servidor principal de scrapping y permite enviar URLs para procesar.

## Configuración de Puertos

- **8080**: Servidor principal + comunicación con clientes
- **8081**: Puerto UDP para broadcast del servidor
- **8082**: API REST (nuevo)

## Instalación y Uso

### 1. Instalar dependencias
```bash
pip3 install -r requirements_api.txt
```

### 2. Iniciar la API
```bash
./start_api.sh
```

O manualmente:
```bash
python3 server/api_server.py
```

### 3. Probar la API
```bash
./test_api.sh
```

## Endpoints Disponibles

### GET `/api/health`
Health check de la API
```bash
curl http://localhost:8082/api/health
```

### GET `/api/status`
Estado de la API y conexión al servidor
```bash
curl http://localhost:8082/api/status
```

### POST `/api/scrape`
Enviar una URL para scrapping
```bash
curl -X POST http://localhost:8082/api/scrape \
  -H "Content-Type: application/json" \
  -d '{"url": "https://example.com"}'
```

### POST `/api/scrape/batch`
Enviar múltiples URLs para scrapping
```bash
curl -X POST http://localhost:8082/api/scrape/batch \
  -H "Content-Type: application/json" \
  -d '{
    "urls": [
      "https://example.com",
      "https://httpbin.org/html"
    ]
  }'
```

### GET `/`
Información básica de la API
```bash
curl http://localhost:8082/
```

## Variables de Entorno

- `API_HOST`: IP donde escucha la API (default: 0.0.0.0)
- `API_PORT`: Puerto de la API (default: 8082)
- `SERVER_HOST`: IP del servidor principal (default: localhost)
- `SERVER_PORT`: Puerto del servidor principal (default: 8080)

## Ejemplo de Uso Programático

### Python
```python
import requests

# Enviar una URL
response = requests.post('http://localhost:8082/api/scrape', 
                        json={'url': 'https://example.com'})
print(response.json())

# Enviar múltiples URLs
response = requests.post('http://localhost:8082/api/scrape/batch',
                        json={
                            'urls': [
                                'https://example.com',
                                'https://httpbin.org/html'
                            ]
                        })
print(response.json())
```

### JavaScript/Node.js
```javascript
const axios = require('axios');

// Enviar una URL
async function scrapeUrl(url) {
    try {
        const response = await axios.post('http://localhost:8082/api/scrape', {
            url: url
        });
        console.log(response.data);
    } catch (error) {
        console.error('Error:', error.response.data);
    }
}

scrapeUrl('https://example.com');
```

### curl
```bash
# Scraping simple
curl -X POST http://localhost:8082/api/scrape \
  -H "Content-Type: application/json" \
  -d '{"url": "https://example.com"}'

# Batch scraping
curl -X POST http://localhost:8082/api/scrape/batch \
  -H "Content-Type: application/json" \
  -d '{"urls": ["https://example.com", "https://test.com"]}'
```

## Arquitectura

```
API Client → API REST (8082) → Servidor Principal (8080) → Clientes de Scrapping
```

La API REST actúa como un proxy/gateway que:
1. Recibe requests HTTP
2. Se conecta al servidor principal via TCP
3. Envía comandos usando el protocolo existente del servidor
4. Retorna respuestas en formato JSON

## Notas

- La API se conecta al servidor principal como si fuera un cliente especial
- Usa el mismo protocolo de comunicación que los clientes de scrapping
- Se reconecta automáticamente si pierde la conexión
- Todas las respuestas están en formato JSON
- Incluye validación básica de URLs (deben empezar con http:// o https://)

## Troubleshooting

### Error de conexión al servidor
- Verificar que el servidor principal esté ejecutándose en puerto 8080
- Verificar que no haya firewall bloqueando las conexiones

### Puerto 8082 en uso
- Cambiar la variable de entorno `API_PORT` a otro puerto disponible
- Usar `netstat -tlnp | grep 8082` para verificar qué está usando el puerto