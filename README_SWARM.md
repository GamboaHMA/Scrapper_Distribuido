# Scrapper Distribuido con Docker Swarm

Este proyecto implementa un sistema de scraping distribuido utilizando Docker Swarm para orquestación. El sistema consta de un servidor central que coordina las tareas y múltiples clientes que realizan el scraping de páginas web.

## Arquitectura

- **Servidor**: Coordina la asignación de tareas a los clientes y recopila los resultados.
- **Clientes**: Realizan el scraping de las páginas web asignadas, respetando robots.txt y límites de velocidad.

## Requisitos

- Docker instalado (versión 19.03 o superior)
- Permisos para iniciar un clúster de Docker Swarm

## Configuración de Docker Swarm

El proyecto incluye scripts para configurar fácilmente un clúster de Docker Swarm:

1. **Desplegar los servicios**:

```bash
./deploy_swarm.sh
```

Este script realizará las siguientes acciones:
- Inicializar Docker Swarm si no está ya inicializado
- Crear una red overlay para la comunicación entre servicios
- Construir las imágenes del servidor y del cliente
- Desplegar el servidor con 1 réplica
- Desplegar los clientes con 3 réplicas

2. **Despliegue optimizado para múltiples nodos**:

```bash
./deploy_multi_node.sh
```

Este script está diseñado específicamente para escenarios multi-nodo, con funcionalidades adicionales para distribuir servicios entre nodos manager y worker.

3. **Escalar el número de clientes**:

```bash
./scale_clients.sh <número_de_réplicas>
```

Ejemplo para escalar a 5 clientes:
```bash
./scale_clients.sh 5
```

4. **Detener y limpiar los servicios**:

```bash
./cleanup_swarm.sh
```

## Monitoreo

Para monitorear los servicios, puedes usar los siguientes comandos:

### Ver los servicios en ejecución:
```bash
docker service ls
```

### Ver los logs del servidor:
```bash
docker service logs scrapper-server
```

### Ver los logs de los clientes:
```bash
docker service logs scrapper-client
```

## Modo de desarrollo

Para trabajar en modo de desarrollo, puedes iniciar los servicios individualmente:

### Servidor:
```bash
cd server
python3 server.py
```

### Cliente:
```bash
cd client
export SERVER_HOST=localhost
export SERVER_PORT=8080
python3 client.py
```

## Escalar a múltiples nodos físicos

Para añadir más nodos físicos al clúster:

1. En los nodos adicionales, ejecuta el siguiente comando (obtenido del resultado de `docker swarm init` en el nodo principal):

```bash
docker swarm join --token <token> <ip-del-nodo-principal>:2377
```

2. En el nodo principal, verifica los nodos del clúster:

```bash
docker node ls
```

3. Los servicios se distribuirán automáticamente entre los nodos disponibles.

## Despliegue en múltiples computadoras

Para probar el sistema distribuido entre dos o más computadoras (por ejemplo, tu laptop y la de un compañero), puedes utilizar el script `deploy_multi_node.sh`.

### En el nodo principal (primera laptop)

1. Asegúrate de que ambas laptops estén conectadas a la misma red (WiFi o LAN)

2. Inicia el despliegue:

```bash
sudo ./deploy_multi_node.sh
```

3. El script inicializará Docker Swarm y mostrará un comando para unir otros nodos

### En el nodo secundario (laptop del compañero)

1. Instala Docker si aún no está instalado

2. Ejecuta el comando de unión que se mostró en el nodo principal:

```bash
sudo docker swarm join --token SWMTKN-1-abcdefg... <IP-DEL-NODO-PRINCIPAL>:2377
```

### Verificación del despliegue

Una vez que ambos nodos estén unidos al clúster, el script `deploy_multi_node.sh` desplegará:

- El servidor en el nodo manager (tu laptop)
- Los clientes en el nodo worker (laptop del compañero)

Esto permite probar el sistema en un entorno distribuido real.

### Solución de problemas comunes

- **Problemas de conexión**: Asegúrate de que los firewalls permitan la comunicación en los puertos 2377, 7946 y 4789
- **Direcciones IP**: Si hay problemas de conexión, verifica que estés usando la dirección IP correcta de la red local

## Notas importantes

- El servidor está configurado para escuchar en el puerto 8080.
- Los clientes utilizan las variables de entorno SERVER_HOST y SERVER_PORT para conectarse al servidor.
- En un despliegue en Swarm, los clientes se conectan al servidor utilizando el nombre del servicio (scrapper-server).
- La comunicación entre servicios se realiza a través de la red overlay creada por Docker Swarm.
- Al usar múltiples nodos físicos, el servidor se despliega preferentemente en el nodo manager y los clientes en los nodos worker.