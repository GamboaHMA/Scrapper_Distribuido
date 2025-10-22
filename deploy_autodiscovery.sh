#!/bin/bash

# Desplegar una red overlay para Docker Swarm con soporte de broadcast
echo "Creando red overlay scrapper-net..."
docker network create --driver overlay --attachable scrapper-network

# Construir las imágenes
echo "Construyendo imagen del servidor..."
docker build -t scrapper-server:latest ./server/

echo "Construyendo imagen del cliente..."
docker build -t scrapper-client:latest ./client/

# Desplegar el servicio del servidor
echo "Desplegando servicio del servidor..."
docker service create \
  --name scrapper-server \
  --network scrapper-net \
  --publish 8080:8080 \
  --publish 8081:8081/udp \
  scrapper-server:latest

# Esperar a que el servidor esté listo
echo "Esperando a que el servidor esté listo..."
sleep 5

# Desplegar los servicios de cliente (escalables)
echo "Desplegando servicios de cliente..."
docker service create \
  --name scrapper-client \
  --network scrapper-net \
  --replicas 2 \
  scrapper-client:latest

echo "Despliegue completo. Los clientes deberían autodescubrir y conectarse al servidor automáticamente."
echo "Puedes escalar los clientes con: docker service scale scrapper-clients=<número>"
echo "Para verificar el estado de los servicios: docker service ls"