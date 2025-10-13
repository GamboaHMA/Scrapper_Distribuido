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

# Opciones para manejar imágenes en múltiples nodos
echo -e "${BLUE}====== Gestión de imágenes Docker entre nodos ======${NC}"
echo -e "${YELLOW}En un entorno multi-nodo, las imágenes deben estar disponibles en todos los nodos.${NC}"
echo -e "${YELLOW}Selecciona una opción para manejar las imágenes:${NC}"
echo -e "1) Construir imágenes localmente en este nodo y exportarlas para importar en otros nodos"
echo -e "2) Construir imágenes en cada nodo individualmente (requiere código fuente en ambas computadoras)"
echo -e "3) Usar registro Docker (requiere un registro Docker configurado)"
echo -e "4) Continuar sin gestión especial (las imágenes se descargarán cuando sea necesario si es posible)"

read -p "Selecciona una opción (1-4): " IMAGE_OPTION

case $IMAGE_OPTION in
    1)
        # Opción 1: Construir y exportar imágenes
        echo -e "${YELLOW}Construyendo y exportando imágenes...${NC}"
        
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
        
        # Guardar las imágenes como archivos tar
        echo -e "${YELLOW}Guardando imágenes como archivos tar...${NC}"
        docker save -o "$BASE_DIR/scrapper-server.tar" scrapper-server
        docker save -o "$BASE_DIR/scrapper-client.tar" scrapper-client
        
        echo -e "${GREEN}Imágenes guardadas en:${NC}"
        echo -e "- $BASE_DIR/scrapper-server.tar"
        echo -e "- $BASE_DIR/scrapper-client.tar"
        
        echo -e "${YELLOW}====== INSTRUCCIONES PARA OTROS NODOS ======${NC}"
        echo -e "${YELLOW}1. Copia los archivos .tar a los otros nodos${NC}"
        echo -e "${YELLOW}2. En cada nodo, ejecuta:${NC}"
        echo -e "   docker load -i scrapper-server.tar"
        echo -e "   docker load -i scrapper-client.tar"
        
        read -p "¿Has importado las imágenes en todos los nodos? (s/n): " IMAGES_IMPORTED
        if [[ $IMAGES_IMPORTED != "s" && $IMAGES_IMPORTED != "S" ]]; then
            echo -e "${RED}Por favor, importa las imágenes en todos los nodos antes de continuar.${NC}"
            exit 1
        fi
        ;;
        
    2)
        # Opción 2: Construir en cada nodo individualmente
        echo -e "${YELLOW}Construyendo imágenes localmente...${NC}"
        
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
        
        echo -e "${YELLOW}====== INSTRUCCIONES PARA OTROS NODOS ======${NC}"
        echo -e "${YELLOW}En cada nodo worker, ejecuta:${NC}"
        echo -e "   cd /ruta/al/proyecto"
        echo -e "   docker build -t scrapper-server ./server"
        echo -e "   docker build -t scrapper-client ./client"
        
        read -p "¿Has construido las imágenes en todos los nodos? (s/n): " IMAGES_BUILT
        if [[ $IMAGES_BUILT != "s" && $IMAGES_BUILT != "S" ]]; then
            echo -e "${RED}Por favor, construye las imágenes en todos los nodos antes de continuar.${NC}"
            exit 1
        fi
        ;;
        
    3)
        # Opción 3: Usar registro Docker
        echo -e "${YELLOW}Usando registro Docker...${NC}"
        
        read -p "Introduce la dirección de tu registro Docker (ej. localhost:5000, mirepo.com): " REGISTRY
        
        # Construir las imágenes
        echo -e "${YELLOW}Construyendo imagen del servidor...${NC}"
        if [ -d "$SERVER_DIR" ]; then
            echo -e "${GREEN}Directorio del servidor encontrado: $SERVER_DIR${NC}"
            docker build -t "$REGISTRY/scrapper-server" "$SERVER_DIR"
        else
            echo -e "${RED}Error: Directorio del servidor no encontrado: $SERVER_DIR${NC}"
            echo -e "${RED}Estructura de directorios actual:${NC}"
            ls -la "$BASE_DIR"
            exit 1
        fi
        
        echo -e "${YELLOW}Construyendo imagen del cliente...${NC}"
        if [ -d "$CLIENT_DIR" ]; then
            echo -e "${GREEN}Directorio del cliente encontrado: $CLIENT_DIR${NC}"
            docker build -t "$REGISTRY/scrapper-client" "$CLIENT_DIR"
        else
            echo -e "${RED}Error: Directorio del cliente no encontrado: $CLIENT_DIR${NC}"
            echo -e "${RED}Estructura de directorios actual:${NC}"
            ls -la "$BASE_DIR"
            exit 1
        fi
        
        # Subir las imágenes al registro
        echo -e "${YELLOW}Subiendo imágenes al registro...${NC}"
        docker push "$REGISTRY/scrapper-server"
        docker push "$REGISTRY/scrapper-client"
        
        # Actualizar los nombres de las imágenes para usar en los servicios
        SCRAPPER_SERVER_IMAGE="$REGISTRY/scrapper-server"
        SCRAPPER_CLIENT_IMAGE="$REGISTRY/scrapper-client"
        ;;
        
    4|*)
        # Opción 4 o por defecto: Continuar sin gestión especial
        echo -e "${YELLOW}Continuando con la construcción local de imágenes...${NC}"
        
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
        
        echo -e "${YELLOW}ADVERTENCIA: Es posible que los nodos worker necesiten descargar o construir las imágenes.${NC}"
        SCRAPPER_SERVER_IMAGE="scrapper-server"
        SCRAPPER_CLIENT_IMAGE="scrapper-client"
        ;;
esac

# Si no se establecieron los nombres de las imágenes en el caso 3, establecerlos ahora
if [ -z "$SCRAPPER_SERVER_IMAGE" ]; then
    SCRAPPER_SERVER_IMAGE="scrapper-server"
fi

if [ -z "$SCRAPPER_CLIENT_IMAGE" ]; then
    SCRAPPER_CLIENT_IMAGE="scrapper-client"
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
        $SCRAPPER_SERVER_IMAGE
else
    docker service create \
        --name scrapper-server \
        --network scrapper-network \
        --replicas 1 \
        --publish 8080:8080 \
        $SCRAPPER_SERVER_IMAGE
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
        $SCRAPPER_CLIENT_IMAGE
    
    echo -e "${GREEN}Clientes desplegados en nodos worker.${NC}"
else
    docker service create \
        --name scrapper-client \
        --network scrapper-network \
        --replicas 3 \
        --env SERVER_HOST=scrapper-server \
        --env SERVER_PORT=8080 \
        $SCRAPPER_CLIENT_IMAGE
    
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

# Opciones interactivas
echo -e "${BLUE}====== Opciones Interactivas ======${NC}"
echo -e "${YELLOW}¿Qué deseas hacer ahora?${NC}"
echo -e "1) Ver logs del servidor en tiempo real"
echo -e "2) Ver logs de un cliente específico en tiempo real"
echo -e "3) Abrir shell interactivo en un contenedor del servidor"
echo -e "4) Abrir shell interactivo en un contenedor cliente"
echo -e "5) Escalar el número de clientes"
echo -e "6) Monitorear todos los servicios"
echo -e "7) Salir sin hacer nada más"

read -p "Selecciona una opción (1-7): " INTERACTIVE_OPTION

case $INTERACTIVE_OPTION in
    1)
        echo -e "${YELLOW}Mostrando logs del servidor en tiempo real (Ctrl+C para salir):${NC}"
        docker service logs scrapper-server --follow
        ;;
    2)
        echo -e "${YELLOW}Contenedores cliente disponibles:${NC}"
        CLIENT_CONTAINERS=$(docker ps --filter name=scrapper-client --format "{{.Names}}")
        
        if [ -z "$CLIENT_CONTAINERS" ]; then
            echo -e "${RED}No se encontraron contenedores cliente.${NC}"
        else
            echo "$CLIENT_CONTAINERS"
            echo ""
            read -p "Ingresa el nombre del contenedor cliente para ver sus logs: " SELECTED_CLIENT
            echo -e "${YELLOW}Mostrando logs del cliente $SELECTED_CLIENT en tiempo real (Ctrl+C para salir):${NC}"
            docker logs $SELECTED_CLIENT --follow
        fi
        ;;
    3)
        echo -e "${YELLOW}Buscando contenedor del servidor...${NC}"
        SERVER_CONTAINER=$(docker ps --filter name=scrapper-server --format "{{.Names}}" | head -n 1)
        
        if [ -z "$SERVER_CONTAINER" ]; then
            echo -e "${RED}No se encontró el contenedor del servidor.${NC}"
        else
            echo -e "${GREEN}Abriendo shell interactivo en el contenedor $SERVER_CONTAINER...${NC}"
            echo -e "${YELLOW}Para salir del contenedor escribe 'exit'${NC}"
            docker exec -it $SERVER_CONTAINER /bin/bash || docker exec -it $SERVER_CONTAINER /bin/sh
        fi
        ;;
    4)
        echo -e "${YELLOW}Contenedores cliente disponibles:${NC}"
        CLIENT_CONTAINERS=$(docker ps --filter name=scrapper-client --format "{{.Names}}")
        
        if [ -z "$CLIENT_CONTAINERS" ]; then
            echo -e "${RED}No se encontraron contenedores cliente.${NC}"
        else
            echo "$CLIENT_CONTAINERS"
            echo ""
            read -p "Ingresa el nombre del contenedor cliente para abrir shell: " SELECTED_CLIENT
            echo -e "${GREEN}Abriendo shell interactivo en el contenedor $SELECTED_CLIENT...${NC}"
            echo -e "${YELLOW}Para salir del contenedor escribe 'exit'${NC}"
            docker exec -it $SELECTED_CLIENT /bin/bash || docker exec -it $SELECTED_CLIENT /bin/sh
        fi
        ;;
    5)
        read -p "Ingresa el número de clientes que deseas ejecutar: " CLIENT_COUNT
        echo -e "${YELLOW}Escalando servicio de clientes a $CLIENT_COUNT réplicas...${NC}"
        docker service scale scrapper-client=$CLIENT_COUNT
        ;;
    6)
        echo -e "${YELLOW}Iniciando monitoreo de servicios (Ctrl+C para salir)...${NC}"
        watch -n 2 'echo "==== SERVICIOS ====" && docker service ls && echo "\n==== SERVIDOR ====" && docker service ps scrapper-server && echo "\n==== CLIENTES ====" && docker service ps scrapper-client'
        ;;
    7|*)
        echo -e "${GREEN}Finalizado. Los servicios continúan ejecutándose en segundo plano.${NC}"
        ;;
esac

echo -e "${BLUE}\n====== Comandos Útiles ======${NC}"
echo -e "${BLUE}Para escalar el número de clientes:${NC}"
echo -e "docker service scale scrapper-client=<número_de_réplicas>"

echo -e "${BLUE}Para ver los logs del servidor:${NC}"
echo -e "docker service logs scrapper-server"

echo -e "${BLUE}Para ver los logs de los clientes:${NC}"
echo -e "docker service logs scrapper-client"

echo -e "${BLUE}Para interactuar con un contenedor específico:${NC}"
echo -e "docker exec -it <nombre_contenedor> /bin/bash"

echo -e "${BLUE}Para ejecutar este menú interactivo nuevamente:${NC}"
echo -e "./interactive_menu.sh"

echo -e "${RED}Para eliminar los servicios:${NC}"
echo -e "docker service rm scrapper-server scrapper-client"