# 🕷️ Sistema de Scraping Distribuido

Sistema distribuido para scraping web con arquitectura jerárquica de nodos Router, Scrapper y Base de Datos.

## 🎯 Características

- **Arquitectura distribuida** con nodos Router, Scrapper y BD
- **Elección de líderes** automática para alta disponibilidad
- **Balanceo de carga** Round-Robin entre nodos Scrapper
- **Interfaz web** moderna con Streamlit
- **Cliente CLI** interactivo
- **Docker Swarm** para despliegue multi-nodo
- **Heartbeat monitoring** para detección de fallos

## 🚀 Inicio Rápido

### Construcción

```bash
# Crear red Docker
make network

# Construir todas las imágenes
make build-all
```

### Ejecución

```bash
# Opción 1: Sistema completo con interfaz web
make run-all

# La interfaz estará en http://localhost:8501
```

```bash
# Opción 2: Solo componentes específicos
make run-scrappers  # 4 nodos scrapper
make run-routers    # 4 nodos router  
make run-streamlit  # Interfaz web
```

### Uso de la Interfaz Web

1. Abre http://localhost:8501 en tu navegador
2. Haz clic en **"Conectar"** en la barra lateral
3. Ingresa una URL en el formulario (ej: https://www.python.org)
4. Haz clic en **"Scrapear"**
5. Ve los resultados en tiempo real

### Limpieza

```bash
# Limpiar todos los contenedores
make clean-all

# Limpiar red
make network-clean
```

## 📋 Comandos Disponibles

Ver todos los comandos:
```bash
make help
```

### Construcción
- `make build-scrapper` - Construir imagen de Scrapper
- `make build-router` - Construir imagen de Router
- `make build-streamlit` - Construir interfaz Streamlit
- `make build-all` - Construir todo

### Ejecución
- `make run-scrappers` - Ejecutar 4 nodos Scrapper
- `make run-routers` - Ejecutar 4 nodos Router
- `make run-streamlit` - Ejecutar interfaz web
- `make run-all` - Ejecutar sistema completo

### Logs
- `make logs-scrapper` - Ver logs del primer Scrapper
- `make logs-router` - Ver logs del primer Router
- `make logs-streamlit` - Ver logs de la interfaz

### Limpieza
- `make clean-scrappers` - Limpiar nodos Scrapper
- `make clean-routers` - Limpiar nodos Router
- `make clean-streamlit` - Limpiar interfaz
- `make clean-all` - Limpiar todo

## 🏗️ Arquitectura

```
┌─────────────────┐
│  Streamlit UI   │ ← Interfaz web (puerto 8501)
└────────┬────────┘
         │
         v
┌─────────────────┐
│  Router Jefe    │ ← Coordina tareas
└────────┬────────┘
         │
         v
┌─────────────────┐
│ Scrapper Jefe   │ ← Asigna tareas
└────────┬────────┘
         │
         v
┌─────────────────┐
│ Scrapper Worker │ ← Ejecuta scraping
└─────────────────┘
```

### Componentes

1. **Router Nodes**: Reciben peticiones de clientes, coordinan con Scrappers y BD
2. **Scrapper Nodes**: Ejecutan tareas de scraping web
3. **BD Nodes**: Almacenan resultados (en desarrollo)
4. **Streamlit UI**: Interfaz web para usuarios
5. **Cliente CLI**: Cliente interactivo de línea de comandos

## 🎨 Interfaz Streamlit

### Características

- ✅ **Dashboard en tiempo real** - Métricas de tareas procesadas
- 🕷️ **Formulario de scraping** - Envío simple de URLs
- 📊 **Visualización de resultados** - Lista con filtros
- 🔌 **Gestión de conexión** - Conectar/desconectar del router
- ⏳ **Tareas pendientes** - Ver estado en tiempo real
- 📋 **Historial completo** - Todos los resultados con detalles

### Capturas

La interfaz muestra:
- Total de tareas procesadas
- Tareas exitosas vs fallidas  
- Tareas pendientes
- Formulario para enviar URLs
- Resultados con título, tamaño y timestamp

## 🐳 Docker

### Imágenes

- `scrapper_node` - Nodo Scrapper
- `router_node` - Nodo Router
- `streamlit-app` - Interfaz web
- `interactive_client` - Cliente CLI

### Red

Todos los contenedores se ejecutan en la red `scrapper-network` (overlay).

## 🔧 Desarrollo

### Requisitos

- Docker
- Make
- Python 3.11+ (para desarrollo local)

### Estructura del Proyecto

```
.
├── base_node/          # Código base compartido
├── router/             # Nodo Router
├── scrapper/           # Nodo Scrapper
├── database/           # Nodo BD (en desarrollo)
├── client/             # Cliente CLI
├── streamlit_app/      # Interfaz Streamlit
├── common/             # Utilidades compartidas
├── Makefile            # Comandos de automatización
└── README.md           # Este archivo
```

## 📝 Notas

- Los nodos eligen automáticamente un líder usando Bully Algorithm
- El sistema soporta caídas de nodos y reelección de líderes
- La interfaz Streamlit se actualiza automáticamente cada segundo
- Los resultados se muestran en orden cronológico inverso

## 🤝 Contribuir

1. Fork el proyecto
2. Crea una rama para tu feature
3. Commit tus cambios
4. Push a la rama
5. Abre un Pull Request

## 📄 Licencia

Este proyecto es parte de un proyecto académico de Sistemas Distribuidos.

