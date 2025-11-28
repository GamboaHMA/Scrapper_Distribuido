# Cliente de Scraping Distribuido

Este cliente forma parte de un sistema distribuido para hacer scraping de páginas web. Se conecta a un servidor central que asigna URLs para procesar y devuelve los resultados del scraping.

## Características

- Scraping de páginas web respetando robots.txt
- Implementa límites de velocidad por dominio
- Extrae enlaces de las páginas HTML
- Se conecta a un servidor central para recibir tareas
- Validación de URLs y detección de contenido no HTML
- Reintentos con backoff exponencial en caso de errores
- Dockerizado para fácil despliegue

## Requisitos

- Python 3.11 o superior
- Docker (opcional, para ejecución en contenedor)
- Dependencias: requests, beautifulsoup4

## Instalación

### Instalación Local

1. Instalar dependencias:
```bash
pip install -r requirements.txt
```

2. Configurar variables de entorno (opcional):
```bash
export SERVER_HOST=ip_del_servidor
export SERVER_PORT=8080
```

3. Ejecutar:
```bash
python client.py
```

### Con Docker

1. Construir la imagen:
```bash
docker build -t scraper-client .
```

2. Ejecutar el contenedor:
```bash
docker run -e SERVER_HOST=ip_del_servidor -e SERVER_PORT=8080 scraper-client
```

## Funcionamiento

1. El cliente se conecta al servidor central y solicita tareas
2. Cuando recibe una tarea con una URL, realiza el scraping
3. Extrae el contenido HTML y los enlaces de la página
4. Envía los resultados de vuelta al servidor
5. Si pierde la conexión, intenta reconectarse automáticamente

## Protocolo de comunicación

- **Conexión inicial**: El cliente se conecta al servidor y recibe un ID
- **Heartbeat**: El cliente envía señales periódicas para indicar que sigue activo
- **Recepción de tareas**: El servidor envía tareas con URLs para procesar
- **Envío de resultados**: El cliente envía los resultados del scraping al servidor

## Configuración

Las siguientes variables pueden ajustarse en el código:

- `HEADERS`: Headers HTTP para las solicitudes
- `DELAY`: Tiempo mínimo entre solicitudes al mismo dominio (en segundos)
- `MAX_RETRIES`: Número máximo de reintentos en caso de error
- `TIMEOUT`: Tiempo máximo de espera para cada solicitud (en segundos)

También puedes configurar la conexión al servidor mediante variables de entorno:

- `SERVER_HOST`: La dirección IP o hostname del servidor central
- `SERVER_PORT`: El puerto del servidor central