# Sistema de Autodescubrimiento para Scrapper Distribuido

Este documento explica cómo funciona el sistema de autodescubrimiento implementado para el proyecto Scrapper Distribuido, que permite que los clientes y servidores se conecten automáticamente dentro de una red Docker Swarm sin depender del DNS de Docker Swarm o la API de Docker.

## Funcionamiento

El sistema utiliza comunicación UDP broadcast para permitir que los servidores anuncien su presencia y los clientes los descubran:

1. **Servidor**: 
   - Al iniciar, el servidor envía periódicamente (cada 5 segundos) mensajes de broadcast UDP al puerto 8081 en la dirección 255.255.255.255.
   - Estos mensajes contienen información sobre la dirección IP y puerto del servidor.

2. **Cliente**:
   - Al iniciar en modo autodescubrimiento, el cliente escucha en el puerto UDP 8081.
   - Cuando recibe un mensaje de broadcast de un servidor, extrae la información de conexión y se conecta automáticamente.
   - Si la conexión falla, continuará escuchando otros mensajes de broadcast.

3. **Persistencia**:
   - Si un cliente pierde la conexión, intentará reconectarse primero al último servidor conocido.
   - Si esto falla, volverá al modo de autodescubrimiento.

## Configuración

### Variables de Entorno

#### Servidor:
- `SERVER_PORT`: Puerto TCP para la comunicación normal (predeterminado: 8080)
- `BROADCAST_PORT`: Puerto UDP para enviar señales de broadcast (predeterminado: 8081)

#### Cliente:
- `SERVER_HOST`: Si está definido, el cliente intentará conectarse directamente a este servidor. Si no está definido o está vacío, se activará el autodescubrimiento.
- `SERVER_PORT`: Puerto TCP para conectarse al servidor (predeterminado: 8080)
- `BROADCAST_PORT`: Puerto UDP para escuchar señales de broadcast (predeterminado: 8081)
- `AUTO_DISCOVERY`: Si es "true" (predeterminado), activa el modo autodescubrimiento cuando SERVER_HOST no está definido.

## Despliegue

Se proporciona un script `deploy_autodiscovery.sh` para desplegar el sistema en Docker Swarm:

```bash
./deploy_autodiscovery.sh
```

Este script:
1. Crea una red overlay con soporte de broadcast
2. Construye las imágenes del servidor y cliente
3. Despliega el servicio del servidor
4. Despliega servicios de cliente (escalables)

## Escalado

Para escalar los clientes:

```bash
docker service scale scrapper-clients=<número>
```

Los nuevos clientes automáticamente descubrirán y se conectarán al servidor.

## Consideraciones

- Este sistema está diseñado para funcionar dentro de una red Docker Swarm overlay, que permite broadcast UDP.
- La dirección de broadcast utilizada (255.255.255.255) funciona dentro de la red Docker, pero no se filtrará fuera de ella.
- Para mayor seguridad en un entorno de producción, considera implementar autenticación o cifrado de las comunicaciones.