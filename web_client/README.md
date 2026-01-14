# Cliente Web de Scrapping

Cliente web simple para el sistema distribuido de scrapping. **NO usa frameworks como Flask, Streamlit o FastAPI**, solo Python estándar con `http.server` y HTML/CSS/JavaScript puro.

## Características

- ✅ **Sin frameworks externos**: Solo usa `http.server` de Python estándar
- 🔄 **Reconexión automática**: Se reconecta automáticamente si el router se cae
- 🎨 **Interfaz moderna**: HTML/CSS/JavaScript puro con diseño responsive
- 📊 **Resultados en tiempo real**: Polling automático de resultados
- 🔍 **Fácil de usar**: Interfaz intuitiva para hacer scrapping de URLs

## Arquitectura

```
web_client/
├── web_client.py   # Servidor HTTP y lógica de conexión (Python)
├── index.html      # Interfaz de usuario
├── style.css       # Estilos CSS
├── script.js       # Lógica del frontend (JavaScript)
└── Dockerfile      # Contenedor Docker
```

### Componentes

1. **web_client.py**: 
   - Servidor HTTP usando `http.server.HTTPServer`
   - Mantiene conexión persistente con el Router usando `NodeConnection`
   - Reconexión automática si se pierde la conexión
   - API REST simple para:
     - `GET /`: Página principal
     - `GET /api/status`: Estado de la conexión
     - `POST /api/scrape`: Enviar petición de scrapping
     - `GET /api/result/<task_id>`: Obtener resultado

2. **Frontend (HTML/CSS/JS)**:
   - Interfaz limpia y moderna
   - Actualización automática de estado cada 5 segundos
   - Polling de resultados cada 5 segundos
   - Visualización de enlaces encontrados

## Uso

### Con Docker (recomendado)

```bash
# Construir imagen
make build-web-client

# Ejecutar cliente web
make run-web-client

# Acceder en el navegador
http://localhost:8080
```

### Manual

```bash
cd web_client
python3 web_client.py
```

Variables de entorno:
- `ROUTER_HOST`: Hostname del router (default: `router`)
- `ROUTER_PORT`: Puerto del router (default: `7070`)
- `WEB_PORT`: Puerto del servidor web (default: `8080`)

## Cómo funciona

### 1. Conexión al Router

El cliente:
1. Resuelve IPs de routers usando DNS de Docker
2. Encuentra el router jefe enviando mensajes de identificación
3. Establece conexión persistente usando `NodeConnection`
4. Monitorea la conexión cada 10 segundos

### 2. Reconexión Automática

Si la conexión se pierde:
1. El monitor detecta la desconexión
2. Fuerza re-descubrimiento de routers (busca nuevo jefe)
3. Establece nueva conexión automáticamente
4. El usuario puede seguir usando la interfaz sin interrupciones

### 3. Peticiones de Scrapping

Flujo:
1. Usuario ingresa URL en la interfaz
2. JavaScript envía POST a `/api/scrape`
3. Servidor envía petición al Router con `NodeConnection`
4. JavaScript hace polling cada 5s a `/api/result/<task_id>`
5. Cuando llega el resultado, se muestra en la interfaz

### 4. Mensajes del Sistema

El cliente maneja:
- **IDENTIFICATION**: Identificación con el router
- **REQUEST**: Envío de peticiones de scrapping
- **RESULT**: Recepción de resultados

## Ventajas vs otros frameworks

### vs Streamlit
- ✅ Más control sobre la UI
- ✅ Mejor para APIs REST
- ✅ Más liviano
- ❌ Menos componentes pre-hechos

### vs Flask
- ✅ No requiere dependencias externas
- ✅ Más simple para casos básicos
- ❌ Menos features avanzados

### vs FastAPI
- ✅ Sin dependencias
- ✅ Python estándar únicamente
- ❌ Sin documentación automática (OpenAPI)

## Ejemplo de uso

1. Abrir http://localhost:8080
2. Ingresar URL (ej: https://example.com)
3. Click en "🔍 Scrapear"
4. Ver resultado en tiempo real cuando esté listo

## Estado de la interfaz

La barra de estado muestra:
- **Estado**: Conectado/Desconectado al router
- **Router**: IP del router jefe actual
- **Pendientes**: Peticiones en proceso
- **Completadas**: Peticiones finalizadas

## Desarrollo

Para modificar la interfaz:
1. Editar `index.html`, `style.css` o `script.js`
2. Recargar página (no necesitas reiniciar servidor)

Para modificar la lógica:
1. Editar `web_client.py`
2. Reiniciar servidor

## Troubleshooting

**Problema**: No se conecta al router
- Verificar que estés en la red `scrapper-network`
- Verificar que haya al menos un router corriendo
- Ver logs: `docker logs web-client`

**Problema**: Resultados no aparecen
- Verificar que haya scrappers disponibles
- El sistema puede tardar si todos los scrappers están ocupados
- Timeout por defecto: 5 minutos

**Problema**: Página no carga
- Verificar que el puerto 8080 no esté en uso
- Revisar logs del contenedor
