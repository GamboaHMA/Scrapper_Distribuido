# Router Node

## Descripción

El **RouterNode** es el nodo coordinador que actúa como intermediario entre clientes externos y los servicios internos del sistema distribuido (BD y Scrapper).

## Arquitectura

```
Cliente → Router Jefe → BD (consulta)
                     ↓
                  Scrapper (si no existe en BD)
                     ↓
                  Cliente (respuesta)
```

## Responsabilidades

### Router Jefe
- **Recibir peticiones de clientes** para scrapping de URLs
- **Consultar BD** para verificar si la información ya existe
- **Delegar a Scrapper** si la información no existe o BD no disponible
- **Responder a clientes** con los resultados
- **Mantener conexiones persistentes** con jefes BD y Scrapper
- **Gestionar cola de tareas** pendientes

### Router Subordinado
- Seguir al jefe mediante elecciones Bully
- Estar listo para convertirse en jefe si el actual falla

## Flujo de Procesamiento

1. **Cliente envía petición** (conexión temporal)
   - `CLIENT_REQUEST` con `task_id` y `url`

2. **Router agrega a cola** de tareas pendientes

3. **Router procesa tarea**:
   - Si BD disponible → envía `BD_QUERY`
   - Si BD no disponible → delega a Scrapper

4. **BD responde**:
   - Si existe → `BD_QUERY_RESPONSE` con resultado
   - Si no existe → Router delega a Scrapper

5. **Scrapper procesa**:
   - Ejecuta scrapping
   - Envía `SCRAPPER_RESULT` con datos

6. **Router responde al cliente**
   - `TASK_RESULT` con información solicitada

## Tipos de Mensaje

### Recibe (Temporal - Clientes)
- `CLIENT_REQUEST`: Petición de scrapping
  ```json
  {
    "type": "client_request",
    "data": {
      "task_id": "uuid",
      "url": "https://example.com"
    }
  }
  ```

### Envía/Recibe (Persistente - BD)
- `BD_QUERY`: Consulta si URL existe
- `BD_QUERY_RESPONSE`: Respuesta con datos o indicación de no existencia

### Envía/Recibe (Persistente - Scrapper)
- `NEW_TASK`: Nueva tarea de scrapping
- `SCRAPPER_RESULT`: Resultado del scrapping

### Envía (Respuesta - Clientes)
- `TASK_RESULT`: Resultado final de la petición

## Conexiones

### Conexiones Entrantes (Temporales)
- **Clientes**: Peticiones de scrapping

### Conexiones Salientes (Persistentes)
- **Jefe BD**: Consultas de información
- **Jefe Scrapper**: Delegación de tareas
- **Subordinados Router**: Gestión de cluster (si es jefe)
- **Jefe Router**: Seguimiento (si es subordinado)

## Cola de Tareas

### Estados de Tarea
1. **Pending**: En cola, esperando procesamiento
2. **In Progress**: 
   - `querying_bd`: Consultando a BD
   - `delegating_scrapper`: Delegando a Scrapper
3. **Completed**: Procesada y respondida al cliente

## Configuración

### Variables de Entorno
- `LOG_LEVEL`: Nivel de logging (INFO/DEBUG)
- Puerto: **9090** (por defecto)

### Docker
```bash
# Construir imagen
make build-router-node

# Ejecutar un router
make run-router-node

# Ejecutar múltiples routers
make run-router-nodes NUM=3

# Ver logs
make logs-router-node

# Limpiar
make clean-router-nodes
```

## Elecciones Bully

Usa el mismo algoritmo que ScrapperNode:
- **Criterio**: IP más alta gana
- **Timeout**: 5 segundos para respuestas
- **Heartbeat**: Cada 30 segundos
- **Detección de fallas**: Timeout de 90 segundos

## Tolerancia a Fallos

- **Jefe BD cae**: Router delega directamente a Scrapper
- **Jefe Scrapper cae**: Router responde con error al cliente
- **Jefe Router cae**: Subordinado con IP más alta se convierte en jefe
- **Reconexión**: Intenta reconectar automáticamente con servicios externos

## Ejemplo de Uso

```python
from router.router_main import RouterNode

# Crear router
router = RouterNode(
    node_type="router",
    port=9090,
    log_level="INFO"
)

# Iniciar
router.start()
```

## Dependencias

- **base_node.Node**: Clase base con elecciones Bully
- **scrapper.utils.MessageProtocol**: Protocolo de mensajes
- **scrapper.utils.NodeConnection**: Gestión de conexiones
