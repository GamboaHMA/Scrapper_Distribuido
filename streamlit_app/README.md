# Interfaz Visual Streamlit - Scrapper Distribuido

Interfaz web interactiva para el sistema de scraping distribuido.

## Características

- 🎯 **Dashboard en tiempo real** con métricas del sistema
- 🕷️ **Envío de tareas** de scraping mediante formulario web
- 📊 **Visualización de resultados** en tiempo real
- 🔌 **Gestión de conexión** con el router
- 📋 **Historial de tareas** con filtros
- 🎨 **Interfaz moderna** y responsive

## Uso Local

```bash
# Instalar dependencias
pip install -r requirements.txt

# Ejecutar aplicación
streamlit run streamlit_app.py
```

La aplicación estará disponible en http://localhost:8501

## Uso con Docker

```bash
# Construir imagen
docker build -t streamlit-app -f streamlit_app/Dockerfile .

# Ejecutar contenedor
docker run -p 8501:8501 \
  --network scrapper-network \
  --name streamlit-app \
  -e ROUTER_IP=router-node \
  streamlit-app
```

Accede a http://localhost:8501 desde tu navegador.

## Configuración

Variables de entorno:

- `ROUTER_IP`: IP o nombre del servicio del router (default: `router-node`)
- `ROUTER_PORT`: Puerto del router (default: `7070`)

## Funcionalidades

### Dashboard
- Total de tareas procesadas
- Tareas exitosas/fallidas
- Tareas pendientes en tiempo real

### Envío de Tareas
- Formulario simple para ingresar URLs
- Validación de URLs
- Feedback inmediato

### Resultados
- Lista de todas las tareas procesadas
- Filtros por estado (exitosas/fallidas)
- Detalles de cada resultado:
  - URL scrapeada
  - Título de la página
  - Tamaño del contenido
  - Timestamp
  - Errores (si los hay)

### Conexión
- Conexión/desconexión manual
- Indicador de estado de conexión
- Información del cliente (ID único)
