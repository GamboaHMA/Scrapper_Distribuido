#!/bin/bash

# Script para desplegar los servicios en un clúster Docker Swarm multi-nodo

# Colores para una mejor visualización
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}====== Configuración de Docker Swarm para Scrapper Distribuido ======${NC}"

# Verificar si Docker está instalado
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Docker no está instalado. Por favor, instala Docker antes de continuar.${NC}"
    exit 1
fi

# Obtener el directorio base del proyecto
BASE_DIR="$(dirname "$(readlink -f "$0")")"
SERVER_DIR="$BASE_DIR/server"
CLIENT_DIR="$BASE_DIR/client"

# Verificar si ya estamos en modo Swarm
SWARM_STATUS=$(docker info --format '{{.Swarm.LocalNodeState}}')

# Inicializar Swarm si es necesario
if [ "$SWARM_STATUS" != "active" ]; then
    # Obtener la IP del host para advertise-addr
    HOST_IP=$(hostname -I | awk '{print $1}')
    
    echo -e "${YELLOW}Inicializando Docker Swarm con IP: $HOST_IP${NC}"
    docker swarm init --advertise-addr $HOST_IP
    
    echo -e "${GREEN}Swarm inicializado. Usa el comando anterior en otras computadoras para unirlas al clúster.${NC}"
    echo -e "${YELLOW}Espera a que otros nodos se unan antes de continuar...${NC}"
    
    # Dar tiempo para que se unan otros nodos
    read -p "¿Presiona Enter cuando estés listo para continuar o Ctrl+C para cancelar..."
    
    # Mostrar los nodos conectados
    echo -e "${YELLOW}Nodos actualmente en el clúster:${NC}"
    docker node ls
else
    echo -e "${GREEN}El nodo ya está en modo Swarm.${NC}"
    
    # Mostrar los nodos conectados
    echo -e "${YELLOW}Nodos actualmente en el clúster:${NC}"
    docker node ls
fi

# Crear una red overlay para la comunicación entre servicios
echo -e "${YELLOW}Creando red overlay 'scrapper-network'...${NC}"
docker network create --driver overlay --attachable scrapper-network || echo -e "${YELLOW}La red ya existe o no se pudo crear.${NC}"

# Verificar si hay múltiples nodos
NODE_COUNT=$(docker node ls | grep -c "Ready")

if [ "$NODE_COUNT" -gt 1 ]; then
    echo -e "${GREEN}Detectados múltiples nodos ($NODE_COUNT). Configurando para distribución entre nodos.${NC}"
    DISTRIBUTE_SERVICES=true
else
    echo -e "${YELLOW}Solo se detectó un nodo. Todos los servicios se ejecutarán en este nodo.${NC}"
    DISTRIBUTE_SERVICES=false
fi

# Construir las imágenes
echo -e "${YELLOW}Construyendo imagen del servidor...${NC}"
if [ -d "$SERVER_DIR" ]; then
    echo -e "${GREEN}Directorio del servidor encontrado: $SERVER_DIR${NC}"
    docker build -t scrapper-server "$SERVER_DIR"
else
    echo -e "${RED}Error: Directorio del servidor no encontrado: $SERVER_DIR${NC}"
    echo -e "${RED}Estructura de directorios actual:${NC}"
    ls -la "$BASE_DIR"
    exit 1
fi

echo -e "${YELLOW}Construyendo imagen del cliente...${NC}"
if [ -d "$CLIENT_DIR" ]; then
    echo -e "${GREEN}Directorio del cliente encontrado: $CLIENT_DIR${NC}"
    docker build -t scrapper-client "$CLIENT_DIR"
else
    echo -e "${RED}Error: Directorio del cliente no encontrado: $CLIENT_DIR${NC}"
    echo -e "${RED}Estructura de directorios actual:${NC}"
    ls -la "$BASE_DIR"
    exit 1
fi

# Desplegar el servicio del servidor (preferiblemente en el nodo manager)
echo -e "${YELLOW}Desplegando servicio del servidor...${NC}"
if [ "$DISTRIBUTE_SERVICES" = true ]; then
    docker service create \
        --name scrapper-server \
        --network scrapper-network \
        --replicas 1 \
        --constraint 'node.role == manager' \
        --publish 8080:8080 \
        scrapper-server
else
    docker service create \
        --name scrapper-server \
        --network scrapper-network \
        --replicas 1 \
        --publish 8080:8080 \
        scrapper-server
fi

# Esperar a que el servidor esté listo
echo -e "${YELLOW}Esperando a que el servidor esté listo...${NC}"
echo -e "${YELLOW}Verificando el estado del servicio del servidor...${NC}"
docker service ls | grep scrapper-server
sleep 10

# Desplegar el servicio del cliente (preferiblemente en los nodos worker si hay múltiples nodos)
echo -e "${YELLOW}Desplegando servicio del cliente...${NC}"
if [ "$DISTRIBUTE_SERVICES" = true ]; then
    docker service create \
        --name scrapper-client \
        --network scrapper-network \
        --replicas 3 \
        --constraint 'node.role == worker' \
        --env SERVER_HOST=scrapper-server \
        --env SERVER_PORT=8080 \
        scrapper-client
    
    echo -e "${GREEN}Clientes desplegados en nodos worker.${NC}"
else
    docker service create \
        --name scrapper-client \
        --network scrapper-network \
        --replicas 3 \
        --env SERVER_HOST=scrapper-server \
        --env SERVER_PORT=8080 \
        scrapper-client
    
    echo -e "${YELLOW}Todos los clientes se ejecutan en el único nodo disponible.${NC}"
fi

echo -e "${GREEN}====== Servicios desplegados en Docker Swarm ======${NC}"
echo -e "${GREEN}Servidor desplegado como 'scrapper-server' (1 réplica)${NC}"
echo -e "${GREEN}Cliente desplegado como 'scrapper-client' (3 réplicas)${NC}"

echo -e "${YELLOW}Verificando el estado de los servicios y su distribución...${NC}"
docker service ls
echo -e "${YELLOW}Distribución del servicio del servidor:${NC}"
docker service ps scrapper-server
echo -e "${YELLOW}Distribución del servicio del cliente:${NC}"
docker service ps scrapper-client

echo -e "${BLUE}Para escalar el número de clientes, ejecuta:${NC}"
echo -e "docker service scale scrapper-client=<número_de_réplicas>"

echo -e "${BLUE}Para ver los logs del servidor:${NC}"
echo -e "docker service logs scrapper-server"

echo -e "${BLUE}Para ver los logs de los clientes:${NC}"
echo -e "docker service logs scrapper-client"

echo -e "${RED}Para eliminar los servicios:${NC}"
echo -e "docker service rm scrapper-server scrapper-client"