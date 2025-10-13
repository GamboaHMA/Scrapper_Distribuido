#!/bin/bash

# Script para detener y limpiar los servicios de Docker Swarm

# Colores para una mejor visualización
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${RED}====== Eliminando servicios de Docker Swarm ======${NC}"

# Eliminar los servicios
echo -e "${YELLOW}Eliminando servicio del cliente...${NC}"
docker service rm scrapper-client || echo -e "${YELLOW}El servicio scrapper-client no existe o ya fue eliminado.${NC}"

echo -e "${YELLOW}Eliminando servicio del servidor...${NC}"
docker service rm scrapper-server || echo -e "${YELLOW}El servicio scrapper-server no existe o ya fue eliminado.${NC}"

# Eliminar la red
echo -e "${YELLOW}Eliminando red scrapper-network...${NC}"
docker network rm scrapper-network || echo -e "${YELLOW}La red scrapper-network no existe o ya fue eliminada.${NC}"

echo -e "${GREEN}====== Limpieza completada ======${NC}"
echo -e "${YELLOW}Si deseas salir completamente del modo Swarm, ejecuta:${NC}"
echo -e "docker swarm leave --force"