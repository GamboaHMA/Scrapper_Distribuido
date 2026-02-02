# Correcciones Aplicadas

## Problemas Solucionados

### 1. Heartbeats causando "Mensaje desconocido" y desconexiones inmediatas
**Problema**: Los heartbeats llegaban como conexiones temporales antes de que la conexión persistente se estableciera, causando warnings y desconexiones.

**Solución**:
- ✅ Agregado handler para ignorar heartbeats en conexiones temporales (`base_node/node.py`)
- ✅ Timeout de heartbeat aumentado de 40s a 60s (`base_node/utils/node_connection.py`)
- ✅ Delay inicial de heartbeat reducido de 15s a 2s (suficiente para evitar conexiones temporales pero no causar timeout)

### 2. Tablas no cargaban en interfaz (timeout de 10s)
**Problema**: El router generaba un nuevo `request_id` en lugar de preservar el del cliente, causando que las respuestas no se reconocieran.

**Solución**:
- ✅ Router ahora preserva el `request_id` original del cliente (`router/router_main.py`)

### 3. Subordinados desconectados no se eliminan
**Problema**: Los subordinados desconectados permanecían en `self.subordinates`, causando errores al intentar enviarles mensajes.

**Solución**:
- ✅ Agregado monitor de conexiones que ejecuta `_cleanup_dead_nodes()` cada 10s (`base_node/node.py`)
- ✅ Mejorado filtrado al replicar info de jefes externos (solo envía a conectados)
- ✅ Agregados logs detallados en asignación de tareas para debugging

### 4. Tareas no se asignan a subordinados
**Problema**: No había visibilidad de por qué las tareas no se asignaban.

**Solución**:
- ✅ Agregados logs detallados mostrando:
  - Subordinados totales vs conectados vs disponibles
  - Estado del jefe (ocupado/disponible)
  - Tareas pendientes al recibir nueva tarea

## IMPORTANTE: Reconstruir Contenedores

Los cambios están en el código pero **los contenedores Docker deben reconstruirse** para aplicarlos:

```bash
# Detener contenedores actuales
make stop

# Reconstruir imágenes (forzar rebuild)
docker-compose build --no-cache

# O si usan Makefile
make build

# Reiniciar todo el sistema
make run-all
```

## Verificación Post-Reconstrucción

Después de reconstruir, deberían ver estos logs:

### Al iniciar un nodo:
```
INFO - Iniciando monitoreo de conexiones (intervalo: 10s)
INFO - Thread de monitoreo de conexiones iniciado
```

### Al recibir una tarea (scrapper jefe):
```
INFO - Nueva tarea recibida del router: task-xxxxx
DEBUG - Estado actual - Subordinados: X, Jefe ocupado: False, Tareas pendientes: 1
DEBUG - Intentando asignar tareas. Pendientes: 1
DEBUG - Subordinados: X totales, Y conectados, Z ocupados, W disponibles
DEBUG - Jefe disponible: True (is_busy=False)
INFO - Asignando tareas con W trabajadores disponibles
```

### Al replicar info de jefes:
```
INFO - Información de 1 jefes externos replicada a X/Y subordinados
```
(X = enviados exitosamente, Y = total de subordinados)

### Heartbeats funcionando:
- NO deberían ver "WARNING - Mensaje desconocido de X: heartbeat"
- NO deberían ver desconexiones cada 60s por timeout
- NO deberían ver "Connection reset by peer" inmediatamente después de conectar

## Archivos Modificados

1. `base_node/node.py`:
   - Línea 224-236: Filtrado de subordinados en replicación
   - Línea 1028-1050: Loop de monitor de conexiones
   - Línea 1052-1120: `_cleanup_dead_nodes()` simplificado
   - Línea 1314-1319: Handler para heartbeats en conexiones temporales
   - Línea 1645-1658: Inicio del monitor de conexiones

2. `base_node/utils/node_connection.py`:
   - Línea 63: Timeout 60s (era 40s)
   - Línea 151-163: Delay de 2s antes del primer heartbeat (era 15s)

3. `router/router_main.py`:
   - Línea 910-949: Preserva `request_id` original del cliente

4. `scrapper/scrapper_main.py`:
   - Línea 420-434: Logs de estado al recibir tarea
   - Línea 497-545: Logs detallados en asignación de tareas

## Próximos Pasos

1. **Reconstruir contenedores** (CRÍTICO)
2. Verificar que aparezcan los nuevos logs
3. Probar que:
   - Las tablas cargan sin timeout
   - Los subordinados desconectados se eliminan automáticamente
   - Las tareas se asignan correctamente
   - No hay desconexiones cíclicas por timeout de heartbeat
