# Cliente Interactivo

Cliente interactivo con conexión persistente al Router Jefe para el sistema de scrapping distribuido.

## Características

- ✅ Descubrimiento automático del Router Jefe
- ✅ Usa DNS de Docker para encontrar todos los routers
- ✅ Identifica al jefe mediante mensajes de identificación
- ✅ Conexión persistente con el Router Jefe
- ✅ Envío de peticiones de scrapping
- ✅ Recepción asíncrona de resultados
- ✅ Consulta de estado del sistema
- ✅ Interfaz de línea de comandos con colores
- ✅ Tracking de peticiones pendientes e historial

## Uso con Docker

### Construir la imagen

```bash
make build-client
```

### Ejecutar el cliente

```bash
make run-client
```

**Proceso de conexión:**
1. El cliente consulta al DNS de Docker por el hostname `router`
2. DNS devuelve TODAS las IPs de contenedores con network-alias `router`
3. El cliente envía mensaje de identificación a cada router
4. El router que responda con `is_boss=True` es el jefe
5. Se establece conexión persistente con el Router Jefe

## Comandos Disponibles

| Comando | Descripción |
|---------|-------------|
| `scrape <url>` | Solicita scrapping de una URL |
| `status` | Muestra estado del sistema (BD, Scrapper, tareas) |
| `pending` | Lista peticiones pendientes |
| `history` | Muestra historial de peticiones completadas |
| `clear` | Limpia la pantalla |
| `help` | Muestra ayuda de comandos |
| `exit` o `quit` | Cierra el cliente |

## Ejemplos de Uso

### Enviar petición de scrapping

```
> scrape https://example.com
[10:30:15] INFO: 📤 Petición enviada: https://example.com
[10:30:15] INFO:    Task ID: 550e8400-e29b-41d4-a716-446655440000
[10:30:17] SUCCESS: ✓ Resultado recibido para 'https://example.com' (2.15s)

============================================================
📄 RESULTADO DEL SCRAPPING:
============================================================
  title: Example Domain
  content: This domain is for use in illustrative...
  links: ['https://www.iana.org/domains/example']
============================================================
```

### Ver estado del sistema

```
> status
[10:31:20] INFO: 📤 Solicitando estado del sistema...
[10:31:20] RESULT: 📊 Estado del Sistema:
{
  "router": {
    "pending_tasks": 2,
    "in_progress": 3,
    "completed": 45
  },
  "bd_available": true,
  "scrapper_available": true
}
```

### Ver peticiones pendientes

```
> pending

📋 Peticiones Pendientes:
--------------------------------------------------------------------------------
  • https://example1.com
    Task ID: 550e8400-e29b-41d4-a716-446655440001
    Tiempo: 5.3s

  • https://example2.com
    Task ID: 550e8400-e29b-41d4-a716-446655440002
    Tiempo: 2.1s
```

## Configuración

El cliente usa variables de entorno para su configuración:

| Variable | Por Defecto | Descripción |
|----------|-------------|-------------|
| `ROUTER_HOST` | `router` | Hostname del Router (network-alias de Docker) |
| `ROUTER_PORT` | `7070` | Puerto del Router |
| `LOG_LEVEL` | `INFO` | Nivel de logging (DEBUG, INFO, WARNING, ERROR) |

### Cambiar configuración

```bash
docker run -it --rm \
  --name client \
  --network scrapper-network \
  -e ROUTER_HOST=mi-router \
  -e LOG_LEVEL=DEBUG \
  client_interactive
```

## Arquitectura

```
┌─────────────────┐
│  Cliente        │
│  Interactivo    │
└────────┬────────┘
         │ TCP Persistente
         │ Puerto 7070
         ▼
┌─────────────────┐
│     Router      │◄─────────► BD
│     (Jefe)      │
└────────┬────────┘
         │
         ▼
    Scrapper(s)
```

### Descubrimiento del Router Jefe

El cliente usa la misma estrategia que los nodos para encontrar jefes:

```
1. DNS Query (getaddrinfo)
   ├─> router: [10.0.1.10, 10.0.1.11, 10.0.1.12]
   └─> Múltiples IPs retornadas

2. Consulta de Identificación (para cada IP)
   Client ──IDENTIFICATION(temporary)──> Router 1
                                        └─> {is_boss: false}
   
   Client ──IDENTIFICATION(temporary)──> Router 2  
                                        └─> {is_boss: true} ✓
   
   Client ──IDENTIFICATION(temporary)──> Router 3
                                        └─> {is_boss: false}

3. Conexión Persistente
   Client ═══════════════════════════> Router 2 (Jefe)
```

### Flujo de Mensajes

1. **Cliente → Routers**: Identificación temporal a todos
2. **Router Jefe → Cliente**: Respuesta con `is_boss=true`
3. **Cliente → Router Jefe**: Conexión persistente establecida
4. **Cliente → Router Jefe**: Petición de scrapping (`CLIENT_REQUEST`)
5. **Router Jefe → BD**: Consulta si existe
6. **Router Jefe → Scrapper**: Delegación de tarea
7. **Scrapper → Router Jefe**: Resultado (`SCRAPPER_RESULT`)
8. **Router Jefe → Cliente**: Resultado final (`TASK_RESULT`)

## Troubleshooting

### Error: "No se encontró un router jefe"

**Problema**: Hay routers en la red pero ninguno es jefe.

**Soluciones**:
1. Espera unos segundos - puede que esté en proceso de elección
2. Verifica logs del router: `docker logs router-node-1`
3. Fuerza elecciones reiniciando un router

### Error: "Error resolviendo 'router'"

**Problema**: No hay routers en la red con ese network-alias.

**Solución**: Asegúrate de que:
1. El cliente esté en la red correcta: `--network scrapper-network`
2. Hay al menos un router corriendo con `--network-alias router`
3. El hostname sea el correcto (por defecto `router`)

```bash
# Verificar routers en la red
docker network inspect scrapper-network | grep -A 10 router
```

### "Timeout consultando routers"

**Problema**: Los routers no responden a identificación.

**Soluciones**:
1. Verifica que los routers estén escuchando: `docker logs router-node-1 | grep "Escuchando"`
2. Prueba conectividad: `docker exec client telnet router 7070`

### No se reciben resultados

**Problema**: Las peticiones se envían pero no llegan resultados.

**Soluciones**:
1. Verifica que hay Scrappers disponibles: `docker ps | grep scrapper`
2. Revisa logs del Router: `docker logs router-node-1`
3. Usa `LOG_LEVEL=DEBUG` para ver más detalles

## Desarrollo

### Ejecutar sin Docker

```bash
# Asegúrate de tener acceso a la red de Docker
python3 client/client.py
```

### Probar conectividad

```bash
# Desde el contenedor
docker exec -it client bash
ping router
telnet router 7070
```

## Próximas Funcionalidades

- [ ] Reintentos automáticos en caso de desconexión
- [ ] Guardar historial en archivo
- [ ] Exportar resultados a JSON/CSV
- [ ] Modo batch (leer URLs desde archivo)
- [ ] Estadísticas de rendimiento
- [ ] Autocompletado de comandos
