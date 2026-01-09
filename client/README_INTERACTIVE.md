# Cliente Interactivo - Sistema de Scrapping Distribuido

Cliente con conexión persistente al Router que permite enviar peticiones de scrapping y recibir resultados de forma asíncrona.

## Características

- ✅ Conexión persistente con el Router
- ✅ Recepción asíncrona de resultados
- ✅ Tracking de peticiones pendientes y completadas
- ✅ Solicitud de estado del sistema
- ✅ Interfaz interactiva con comandos
- ✅ Colores para mejor visualización

## Uso Rápido

### Con Docker (Recomendado)

```bash
# 1. Construir imagen del cliente
make build-client

# 2. Ejecutar cliente (requiere que el sistema esté corriendo)
make run-client
```

### Sin Docker

```bash
# Desde el directorio raíz del proyecto
python3 client/interactive_client.py
```

## Comandos Disponibles

| Comando | Descripción | Ejemplo |
|---------|-------------|---------|
| `scrape <url>` | Solicitar scrapping de una URL | `scrape https://example.com` |
| `status` | Ver estado del sistema | `status` |
| `pending` | Mostrar peticiones pendientes | `pending` |
| `completed` | Mostrar peticiones completadas | `completed` |
| `clear` | Limpiar historial de completadas | `clear` |
| `help` | Mostrar ayuda de comandos | `help` |
| `exit` o `quit` | Salir del cliente | `exit` |

## Flujo de Trabajo Típico

1. **Iniciar el cliente:**
   ```bash
   make run-client
   ```

2. **Solicitar scrapping:**
   ```
   > scrape https://example.com
   ```

3. **Ver peticiones pendientes:**
   ```
   > pending
   ```

4. **Recibir resultado:**
   El resultado se mostrará automáticamente cuando esté listo

5. **Ver estado del sistema:**
   ```
   > status
   ```

6. **Salir:**
   ```
   > exit
   ```

## Ejemplo de Sesión

```
╔══════════════════════════════════════════════════════════════════╗
║              CLIENTE INTERACTIVO - Sistema de Scrapping          ║
╚══════════════════════════════════════════════════════════════════╝

[10:30:15] INFO: 🔍 Buscando Router en DNS...
[10:30:15] SUCCESS: ✓ Router jefe encontrado: 10.0.1.5
[10:30:15] INFO: 🔌 Conectando con Router 10.0.1.5...
[10:30:15] SUCCESS: ✓ Conectado al Router

> scrape https://example.com
[10:30:20] INFO: → Petición enviada: https://example.com
[10:30:20] INFO:   Task ID: a1b2c3d4-e5f6-...

[10:30:25] SUCCESS: ✓ Resultado recibido para: https://example.com
======================================================================
[10:30:25] RESULT: RESULTADO: https://example.com
======================================================================
  title: Example Domain
  description: Example website for documentation
  content: This domain is for use in illustrative...
======================================================================

> status
[10:30:30] INFO: → Solicitando estado del sistema...

======================================================================
[10:30:30] RESULT: ESTADO DEL SISTEMA
======================================================================
  router:
    ip: 10.0.1.5
    is_boss: True
    bd_connected: True
    scrapper_connected: True
  tasks:
    pending: 0
    in_progress: 0
    completed: 1
  timestamp: 2025-12-23T10:30:30
======================================================================

> exit
[10:30:35] INFO: Cerrando cliente...
[10:30:35] INFO: Conexión cerrada
```

## Estructura de Mensajes

### Petición de Scrapping
```json
{
  "type": "client_request",
  "sender_id": "interactive-client",
  "data": {
    "task_id": "uuid-unico",
    "url": "https://example.com"
  }
}
```

### Resultado de Scrapping
```json
{
  "type": "task_result",
  "data": {
    "task_id": "uuid-unico",
    "result": {
      "title": "...",
      "content": "..."
    },
    "success": true
  }
}
```

### Solicitud de Estado
```json
{
  "type": "status_request",
  "sender_id": "interactive-client",
  "data": {}
}
```

### Respuesta de Estado
```json
{
  "type": "status_response",
  "data": {
    "router": {
      "ip": "10.0.1.5",
      "is_boss": true,
      "bd_connected": true,
      "scrapper_connected": true
    },
    "tasks": {
      "pending": 0,
      "in_progress": 1,
      "completed": 5
    },
    "timestamp": "2025-12-23T10:30:30"
  }
}
```

## Troubleshooting

### No se encuentra el Router
- Verificar que el DNS esté corriendo: `docker ps | grep dns`
- Verificar que el Router esté corriendo: `docker ps | grep router`
- Verificar que estés en la misma red Docker: `docker network inspect scrapper-network`

### Conexión cerrada inesperadamente
- El Router puede haberse caído
- Verificar logs del Router: `make logs-router-node`
- Reiniciar el cliente

### No llegan resultados
- Verificar que los Scrappers estén corriendo: `docker ps | grep scrapper`
- Usar comando `pending` para ver si la petición está en cola
- Verificar logs del Router y Scrapper

## Integración con el Sistema

El cliente se conecta al Router jefe y:

1. **Descubrimiento:** Consulta DNS para encontrar Routers disponibles
2. **Verificación:** Identifica cuál es el jefe
3. **Conexión:** Establece conexión persistente con el jefe
4. **Comunicación:** Envía peticiones y recibe resultados asíncronamente

```
┌─────────┐      ┌──────┐      ┌────────┐      ┌──────┐      ┌──────────┐
│ Cliente │─────▶│ DNS  │─────▶│ Router │─────▶│  BD  │      │ Scrapper │
│Interact.│◀─────│Server│      │  Jefe  │      │      │      │          │
└─────────┘      └──────┘      └────────┘      └──────┘      └──────────┘
                                    │                              ▲
                                    │                              │
                                    └──────────────────────────────┘
                                         Delegación de tareas
```

## Desarrollo

Para modificar el cliente:

1. Editar `client/interactive_client.py`
2. Reconstruir imagen: `make build-client`
3. Probar cambios: `make run-client`

## Notas

- El cliente usa colores ANSI, mejor visualización en terminales modernas
- La conexión es persistente, se mantiene hasta que el usuario salga
- Los resultados se reciben asíncronamente en un thread separado
- El tracking de peticiones se mantiene en memoria (se pierde al cerrar)
