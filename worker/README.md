# Worker de Scraping

Este componente es parte de un proyecto de scraper distribuido. El worker es responsable de descargar el contenido HTML de una URL proporcionada, respetando el archivo robots.txt y los límites de velocidad para cada dominio.

## Características

- Respeta robots.txt
- Implementa límites de velocidad por dominio
- Extrae enlaces de las páginas HTML
- API REST con Flask para comunicación entre nodos
- Validación de URLs y detección de contenido no HTML
- Reintentos con backoff exponencial en caso de errores

## Requisitos

- Python 3.11 o superior
- Docker (opcional, para ejecución en contenedor)

## Instalación

### Con Docker (recomendado)

1. Construir la imagen:
```bash
docker build -t scraper-worker .
```

2. Ejecutar el contenedor:
```bash
docker run -p 5000:5000 scraper-worker
```

### Sin Docker

1. Crear un entorno virtual (opcional pero recomendado):
```bash
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate
```

2. Instalar dependencias:
```bash
pip install -r requirements.txt
```

3. Ejecutar el worker:
```bash
python worker.py
```

## Uso

El worker expone dos endpoints HTTP:

### 1. `/health` (GET)

Comprueba si el servicio está funcionando correctamente.

**Ejemplo:**
```bash
curl http://localhost:5000/health
```

**Respuesta:**
```json
{"status": "OK"}
```

### 2. `/scrape` (POST)

Realiza el scraping de una URL.

**Ejemplo:**
```bash
curl -X POST \
  http://localhost:5000/scrape \
  -H "Content-Type: application/json" \
  -d '{"url": "https://www.python.org/"}'
```

**Respuesta exitosa:**
```json
{
  "url": "https://www.python.org/",
  "html": "<!DOCTYPE html>...</html>",
  "links": [
    "https://www.python.org/about/",
    "https://www.python.org/downloads/",
    ...
  ]
}
```

**Respuesta de error:**
```json
{
  "error": "URL no permitida por robots.txt: https://example.com/private"
}
```

## Cliente de prueba

Se incluye un cliente de prueba (`test_client.py`) que muestra cómo otro nodo puede conectarse al worker:

```bash
python test_client.py https://www.python.org/
```

## Integración con otros nodos

Para integrar este worker con otros nodos del sistema distribuido, los nodos pueden realizar solicitudes HTTP POST al endpoint `/scrape` con una URL para procesar. El worker devolverá el HTML y los enlaces encontrados en la página.

## Configuración

Las siguientes variables en `worker.py` pueden ajustarse según sea necesario:

- `HEADERS`: Headers HTTP para las solicitudes
- `DELAY`: Tiempo mínimo entre solicitudes al mismo dominio (en segundos)
- `MAX_RETRIES`: Número máximo de reintentos en caso de error
- `TIMEOUT`: Tiempo máximo de espera para cada solicitud (en segundos)