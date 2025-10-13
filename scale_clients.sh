#!/bin/bash

# Script para escalar el número de clientes en Docker Swarm

# Colores para una mejor visualización
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}====== Escalado de clientes en Docker Swarm ======${NC}"

# Verificar si se proporcionó un número de réplicas
if [ -z "$1" ]; then
    echo -e "${RED}Error: Debes proporcionar el número de réplicas.${NC}"
    echo -e "${YELLOW}Uso: $0 <número_de_réplicas>${NC}"
    exit 1
fi

# Verificar si el argumento es un número
if ! [[ "$1" =~ ^[0-9]+$ ]]; then
    echo -e "${RED}Error: El argumento debe ser un número entero positivo.${NC}"
    exit 1
fi

# Escalar el servicio del cliente
echo -e "${YELLOW}Escalando el servicio scrapper-client a $1 réplicas...${NC}"
docker service scale scrapper-client=$1

# Verificar el estado del servicio
echo -e "${YELLOW}Verificando el estado del servicio...${NC}"
docker service ls | grep scrapper-client

echo -e "${GREEN}====== Escalado completado ======${NC}"