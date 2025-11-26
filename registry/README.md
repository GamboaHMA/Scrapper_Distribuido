# Registry/DNS Service

El **Registry** es el servicio de descubrimiento de servicios (DNS) para el sistema de Scrapper Distribuido.

## 🎯 Funcionalidad

- **Escaneo activo de red**: Recorre cada IP del rango especificado buscando servicios
- **No usa broadcast**: Conexión directa IP por IP para mayor confiabilidad
- **Registro automático**: Identifica y registra coordinators, scrappers y gateways
- **Consultas DNS**: API para que otros servicios encuentren servicios disponibles
- **Heartbeat automático**: Elimina servicios que no responden

## 🚀 Uso

### Ejecutar con Docker:
```bash
make run-registry
```

### Ejecutar manualmente:
```bash
python3 registry/registry.py
```

### Configuración con variables de entorno:
```bash
docker run -d --name scrapper-registry \
  --network host \
  -e NETWORK_RANGE=192.168.1.0/24 \
  -e SCAN_PORT=8080 \
  -e REGISTRY_PORT=5353 \
  scrapper-registry
```

## 🔍 Consultar el Registry

### Listar todos los servicios:
```bash
python3 registry/registry_client.py list
```

### Buscar por tipo:
```bash
python3 registry/registry_client.py type coordinator
python3 registry/registry_client.py type scrapper
```

### Buscar por ID:
```bash
python3 registry/registry_client.py id coordinator_192.168.1.10_8080
```

## 📡 API del Registry

El registry escucha en el puerto **5353** y acepta consultas JSON:

### Consulta: Listar todos
```json
{
  "query_type": "list_all"
}
```

### Consulta: Por tipo
```json
{
  "query_type": "find_by_type",
  "service_type": "coordinator"
}
```

### Consulta: Por ID
```json
{
  "query_type": "find_by_id",
  "service_id": "coordinator_192.168.1.10_8080"
}
```

## 🔧 Configuración

- `NETWORK_RANGE`: Rango de IPs a escanear (default: `192.168.1.0/24`)
- `SCAN_PORT`: Puerto donde buscar servicios (default: `8080`)
- `REGISTRY_PORT`: Puerto del registry (default: `5353`)

## 🏗️ Integración con Servicios

Los servicios (coordinator, scrapper, gateway) deben responder al mensaje `identify`:

```json
{
  "type": "identify",
  "timestamp": "2025-11-23T12:00:00"
}
```

Respuesta esperada:
```json
{
  "service_type": "coordinator",
  "service_id": "coordinator_192.168.1.10_8080",
  "metadata": {
    "host": "192.168.1.10",
    "port": 8080,
    "total_clients": 5
  }
}
```

## 📊 Ejemplo de salida

```json
{
  "services": [
    {
      "service_id": "coordinator_192.168.1.10_8080",
      "type": "coordinator",
      "ip": "192.168.1.10",
      "port": 8080,
      "last_seen": "2025-11-23T12:30:00",
      "metadata": {
        "total_clients": 5,
        "total_tasks": 12
      }
    },
    {
      "service_id": "scrapper_192.168.1.15_8080",
      "type": "scrapper",
      "ip": "192.168.1.15",
      "port": 8080,
      "last_seen": "2025-11-23T12:29:55",
      "metadata": {}
    }
  ],
  "count": 2
}
```

## 🔄 Flujo de Operación

1. **Inicio**: Registry escanea el rango de red configurado
2. **Discovery**: Por cada IP, intenta conectarse al puerto configurado
3. **Identificación**: Envía mensaje `identify` a servicios que responden
4. **Registro**: Almacena servicios identificados con su información
5. **Limpieza**: Elimina servicios que no responden tras timeout
6. **Consultas**: Otros servicios pueden consultar el registry para encontrar servicios

## ⚙️ Futuras Mejoras

- [ ] Soporte para múltiples puertos por servicio
- [ ] Balanceo de carga automático
- [ ] Persistencia de servicios en disco
- [ ] Clustering de registries para alta disponibilidad
- [ ] Métricas de salud de servicios
