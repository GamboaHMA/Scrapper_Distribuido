# Makefile para Scrapper Distribuido

# Variables
NETWORK_NAME = scrapper-network
SCRAPPER_NODE_IMAGE = scrapper_node
ROUTER_NODE_IMAGE = router_node
DATABASE_NODE_IMAGE = db_node
CLIENT_IMAGE = client

# Colores
GREEN = \033[0;32m
YELLOW = \033[1;33m
RED = \033[0;31m
NC = \033[0m

.PHONY: help network network-inspect network-clean build build-all run run-all clean clean-all

# =============================================================================
# AYUDA
# =============================================================================

help: ## Mostrar todos los comandos disponibles
	@echo "$(GREEN)╔════════════════════════════════════════════════════════════════╗$(NC)"
	@echo "$(GREEN)║      Scrapper Distribuido - Comandos Disponibles               ║$(NC)"
	@echo "$(GREEN)╚════════════════════════════════════════════════════════════════╝$(NC)"
	@echo ""
	@echo "$(YELLOW)Red Docker:$(NC)"
	@grep -E '^network.*:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-25s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(YELLOW)Build (Construcción de Imágenes):$(NC)"
	@grep -E '^build.*:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-25s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(YELLOW)Run (Ejecutar Nodos):$(NC)"
	@grep -E '^run.*:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-25s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(YELLOW)Clean (Limpiar):$(NC)"
	@grep -E '^clean.*:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-25s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(YELLOW)Logs y Debug:$(NC)"
	@grep -E '^(logs|exec).*:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-25s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(YELLOW)Docker Swarm:$(NC)"
	@grep -E '^swarm.*:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-25s$(NC) %s\n", $$1, $$2}'
	@echo ""

# =============================================================================
# RED DOCKER
# =============================================================================

network: ## Crear red overlay scrapper-network
	@echo "$(YELLOW)Creando red overlay $(NETWORK_NAME)...$(NC)"
	@docker network create --driver overlay --attachable $(NETWORK_NAME) 2>/dev/null || echo "$(GREEN)Red ya existe$(NC)"
	@echo "$(GREEN)✅ Red overlay lista$(NC)"

network-inspect: ## Inspeccionar la red scrapper-network
	@echo "$(GREEN)Información de la red $(NETWORK_NAME):$(NC)"
	@docker network inspect $(NETWORK_NAME)

network-clean: ## Eliminar la red scrapper-network
	@echo "$(YELLOW)Eliminando red $(NETWORK_NAME)...$(NC)"
	-docker network rm $(NETWORK_NAME)
	@echo "$(GREEN)✅ Red eliminada$(NC)"

# =============================================================================
# BUILD - CONSTRUCCIÓN DE IMÁGENES
# =============================================================================

build-scrapper: ## Construir imagen de ScrapperNode
	@echo "$(YELLOW)Construyendo ScrapperNode...$(NC)"
	docker build -t $(SCRAPPER_NODE_IMAGE) -f scrapper/Dockerfile .
	@echo "$(GREEN)✅ ScrapperNode construido$(NC)"

build-router: ## Construir imagen de RouterNode
	@echo "$(YELLOW)Construyendo RouterNode...$(NC)"
	docker build -t $(ROUTER_NODE_IMAGE) -f router/Dockerfile .
	@echo "$(GREEN)✅ RouterNode construido$(NC)"

build-database: ## Construir imagen de DatabaseNode
	@echo "$(YELLOW)Construyendo DatabaseNode...$(NC)"
	docker build -t $(DATABASE_NODE_IMAGE) -f database/Dockerfile .
	@echo "$(GREEN)✅ DatabaseNode construido$(NC)"

build-client: ## Construir imagen del Cliente
	@echo "$(YELLOW)Construyendo Cliente...$(NC)"
	docker build -t $(CLIENT_IMAGE) -f client/Dockerfile .
	@echo "$(GREEN)✅ Cliente construido$(NC)"

build-streamlit: ## Construir imagen de Streamlit UI
	@echo "$(YELLOW)Construyendo Interfaz Streamlit...$(NC)"
	docker build -t streamlit-app -f streamlit_app/Dockerfile .
	@echo "$(GREEN)✅ Streamlit UI construido$(NC)"

build-all: build-scrapper build-router build-database build-client build-streamlit ## Construir todas las imágenes
	@echo "$(GREEN)╔════════════════════════════════════════════════════════════════╗$(NC)"
	@echo "$(GREEN)║  ✅ Todas las imágenes han sido construidas exitosamente       ║$(NC)"
	@echo "$(GREEN)╚════════════════════════════════════════════════════════════════╝$(NC)"

# =============================================================================
# RUN - EJECUTAR NODOS
# =============================================================================

run-scrapper: network ## Ejecutar 1 ScrapperNode
	@echo "$(YELLOW)Iniciando 1 ScrapperNode...$(NC)"
	@NEXT_NUM=$$(docker ps -a --filter name=scrapper-node --format "{{.Names}}" | \
		sed 's/scrapper-node-//' | sort -n | tail -1); \
	NEXT_NUM=$$((NEXT_NUM + 1)); \
	docker run -d --name scrapper-node-$$NEXT_NUM \
		--network $(NETWORK_NAME) \
		--network-alias scrapper \
		-e LOG_LEVEL=INFO \
		$(SCRAPPER_NODE_IMAGE); \
	echo "$(GREEN)✅ ScrapperNode $$NEXT_NUM iniciado$(NC)"

run-scrappers: network ## Ejecutar 4 ScrapperNodes (por defecto)
	@echo "$(YELLOW)Iniciando 4 ScrapperNodes...$(NC)"
	@for i in 1 2 3 4; do \
		docker run -d --name scrapper-node-$$i \
			--network $(NETWORK_NAME) \
			--network-alias scrapper \
			-e LOG_LEVEL=INFO \
			$(SCRAPPER_NODE_IMAGE); \
		echo "$(GREEN)✅ ScrapperNode $$i iniciado$(NC)"; \
	done

run-router: network ## Ejecutar 1 RouterNode
	@echo "$(YELLOW)Iniciando 1 RouterNode...$(NC)"
	@NEXT_NUM=$$(docker ps -a --filter name=router-node --format "{{.Names}}" | \
		sed 's/router-node-//' | sort -n | tail -1); \
	NEXT_NUM=$$((NEXT_NUM + 1)); \
	docker run -d --name router-node-$$NEXT_NUM \
		--network $(NETWORK_NAME) \
		--network-alias router \
		-e LOG_LEVEL=INFO \
		$(ROUTER_NODE_IMAGE); \
	echo "$(GREEN)✅ RouterNode $$NEXT_NUM iniciado$(NC)"

run-routers: network ## Ejecutar 4 RouterNodes (por defecto)
	@echo "$(YELLOW)Iniciando 4 RouterNodes...$(NC)"
	@for i in 1 2 3 4; do \
		docker run -d --name router-node-$$i \
			--network $(NETWORK_NAME) \
			--network-alias router \
			-e LOG_LEVEL=INFO \
			$(ROUTER_NODE_IMAGE); \
		echo "$(GREEN)✅ RouterNode $$i iniciado$(NC)"; \
	done

run-database: network ## Ejecutar 1 DatabaseNode
	@echo "$(YELLOW)Iniciando 1 DatabaseNode...$(NC)"
	@NEXT_NUM=$$(docker ps -a --filter name=db-node --format "{{.Names}}" | \
		sed 's/db-node-//' | sort -n | tail -1); \
	NEXT_NUM=$$((NEXT_NUM + 1)); \
	docker run -d --name db-node-$$NEXT_NUM \
		--network $(NETWORK_NAME) \
		--network-alias bd \
		-e LOG_LEVEL=INFO \
		-v database-data-$$NEXT_NUM:/app/database \
		$(DATABASE_NODE_IMAGE); \
	echo "$(GREEN)✅ DatabaseNode $$NEXT_NUM iniciado$(NC)"

run-databases: network ## Ejecutar 3 DatabaseNodes (por defecto)
	@echo "$(YELLOW)Iniciando 3 DatabaseNodes...$(NC)"
	@for i in 1 2 3; do \
		docker run -d --name db-node-$$i \
			--network $(NETWORK_NAME) \
			--network-alias bd \
			-e LOG_LEVEL=INFO \
			-v database-data-$$i:/app/database \
			$(DATABASE_NODE_IMAGE); \
		echo "$(GREEN)✅ DatabaseNode $$i iniciado$(NC)"; \
	done

run-client: network ## Ejecutar 1 Cliente interactivo
	@echo "$(YELLOW)Iniciando Cliente...$(NC)"
	docker run -d --name client-1 \
		--network $(NETWORK_NAME) \
		--entrypoint /bin/bash \
		$(CLIENT_IMAGE) -c "tail -f /dev/null"
	@echo "$(GREEN)✅ Cliente iniciado$(NC)"
	@echo "$(YELLOW)Para usar el cliente ejecuta:$(NC)"
	@echo "  docker exec -it client-1 python /app/client/client.py"

run-streamlit: network ## Ejecutar interfaz Streamlit (accesible en http://localhost:8501)
	@echo "$(YELLOW)Iniciando interfaz Streamlit...$(NC)"
	docker run -d --name streamlit-app \
		--network $(NETWORK_NAME) \
		-p 8501:8501 \
		-e ROUTER_IP=router-node \
		-e ROUTER_PORT=7070 \
		streamlit-app
	@echo "$(GREEN)✅ Streamlit UI iniciado$(NC)"
	@echo "$(YELLOW)Accede a la interfaz en:$(NC) http://localhost:8501"

run-all: network run-scrappers run-routers run-databases run-client run-streamlit ## Ejecutar todo el sistema (4 scrappers, 4 routers, 3 databases, 1 cliente, UI)
	@echo "$(GREEN)╔════════════════════════════════════════════════════════════════╗$(NC)"
	@echo "$(GREEN)║  ✅ Sistema completo desplegado:                               ║$(NC)"
	@echo "$(GREEN)║     - 4 ScrapperNodes                                          ║$(NC)"
	@echo "$(GREEN)║     - 4 RouterNodes                                            ║$(NC)"
	@echo "$(GREEN)║     - 3 DatabaseNodes                                          ║$(NC)"
	@echo "$(GREEN)║     - 1 Cliente                                                ║$(NC)"
	@echo "$(GREEN)║     - 1 Streamlit UI                                           ║$(NC)"
	@echo "$(GREEN)╚════════════════════════════════════════════════════════════════╝$(NC)"
	@echo "$(YELLOW)Interfaz web:$(NC) http://localhost:8501"
	@echo "$(YELLOW)Para cliente CLI:$(NC) docker exec -it client-1 python /app/client/client.py"

# =============================================================================
# CLEAN - LIMPIEZA
# =============================================================================

clean-scrappers: ## Detener y eliminar todos los ScrapperNodes
	@echo "$(YELLOW)Limpiando ScrapperNodes...$(NC)"
	@containers=$$(docker ps -aq --filter name=scrapper-node 2>/dev/null); \
	if [ -n "$$containers" ]; then \
		docker stop $$containers 2>/dev/null; \
		docker rm $$containers 2>/dev/null; \
	fi
	@echo "$(GREEN)✅ ScrapperNodes limpiados$(NC)"

clean-routers: ## Detener y eliminar todos los RouterNodes
	@echo "$(YELLOW)Limpiando RouterNodes...$(NC)"
	@containers=$$(docker ps -aq --filter name=router-node 2>/dev/null); \
	if [ -n "$$containers" ]; then \
		docker stop $$containers 2>/dev/null; \
		docker rm $$containers 2>/dev/null; \
	fi
	@echo "$(GREEN)✅ RouterNodes limpiados$(NC)"

clean-databases: ## Detener y eliminar todos los DatabaseNodes
	@echo "$(YELLOW)Limpiando DatabaseNodes...$(NC)"
	@containers=$$(docker ps -aq --filter name=db-node 2>/dev/null); \
	if [ -n "$$containers" ]; then \
		docker stop $$containers 2>/dev/null; \
		docker rm $$containers 2>/dev/null; \
	fi
	@echo "$(GREEN)✅ DatabaseNodes limpiados$(NC)"

clean-datasets: ## Eliminar todos los volúmenes de datos de DatabaseNodes
	@echo "$(YELLOW)Eliminando volúmenes database-data...$(NC)"
	-docker volume rm $$(docker volume ls -q --filter name=database-data) 2>/dev/null || echo "$(GREEN)No hay volúmenes database-data$(NC)"
	@echo "$(GREEN)✅ Volúmenes eliminados$(NC)"

clean-clients: ## Detener y eliminar todos los Clientes
	@echo "$(YELLOW)Limpiando Clientes...$(NC)"
	@containers=$$(docker ps -aq --filter name=client 2>/dev/null); \
	if [ -n "$$containers" ]; then \
		docker stop $$containers 2>/dev/null; \
		docker rm $$containers 2>/dev/null; \
	fi
	@echo "$(GREEN)✅ Clientes limpiados$(NC)"

clean-streamlit: ## Detener y eliminar Streamlit UI
	@echo "$(YELLOW)Limpiando Streamlit UI...$(NC)"
	@container=$$(docker ps -aq --filter name=streamlit-app 2>/dev/null); \
	if [ -n "$$container" ]; then \
		docker stop $$container 2>/dev/null; \
		docker rm $$container 2>/dev/null; \
	fi
	@echo "$(GREEN)✅ Streamlit UI limpiado$(NC)"

clean-all: clean-scrappers clean-routers clean-databases clean-clients clean-streamlit ## Limpiar todos los contenedores
	@echo "$(GREEN)╔════════════════════════════════════════════════════════════════╗$(NC)"
	@echo "$(GREEN)║  ✅ Todos los contenedores han sido limpiados                 ║$(NC)"
	@echo "$(GREEN)╚════════════════════════════════════════════════════════════════╝$(NC)"

clean-images: ## Eliminar todas las imágenes del proyecto
	@echo "$(YELLOW)Eliminando imágenes del proyecto...$(NC)"
	@images=""; \
	for img in $(SCRAPPER_NODE_IMAGE) $(ROUTER_NODE_IMAGE) $(DATABASE_NODE_IMAGE) $(CLIENT_IMAGE) streamlit-app; do \
		id=$$(docker images -q $$img 2>/dev/null); \
		if [ -n "$$id" ]; then \
			images="$$images $$id"; \
		fi; \
	done; \
	if [ -n "$$images" ]; then \
		docker rmi -f $$images 2>/dev/null; \
		echo "$(GREEN)✅ Imágenes eliminadas$(NC)"; \
	else \
		echo "$(GREEN)No hay imágenes del proyecto para eliminar$(NC)"; \
	fi

prune: clean-all clean-datasets clean-images ## Limpiar TODO (contenedores, volúmenes e imágenes)
	@echo "$(GREEN)╔════════════════════════════════════════════════════════════════╗$(NC)"
	@echo "$(GREEN)║  ✅ Sistema completamente limpiado                            ║$(NC)"
	@echo "$(GREEN)╚════════════════════════════════════════════════════════════════╝$(NC)"

# =============================================================================
# LOGS Y DEBUG
# =============================================================================

logs-scrapper: ## Ver logs del primer ScrapperNode
	@CONTAINER=$$(docker ps --filter name=scrapper-node --format "{{.Names}}" | head -1); \
	if [ -n "$$CONTAINER" ]; then \
		echo "$(GREEN)Logs de $$CONTAINER:$(NC)"; \
		docker logs -f $$CONTAINER; \
	else \
		echo "$(RED)No hay ScrapperNodes en ejecución$(NC)"; \
	fi

logs-router: ## Ver logs del primer RouterNode
	@CONTAINER=$$(docker ps --filter name=router-node --format "{{.Names}}" | head -1); \
	if [ -n "$$CONTAINER" ]; then \
		echo "$(GREEN)Logs de $$CONTAINER:$(NC)"; \
		docker logs -f $$CONTAINER; \
	else \
		echo "$(RED)No hay RouterNodes en ejecución$(NC)"; \
	fi

logs-database: ## Ver logs del primer DatabaseNode
	@CONTAINER=$$(docker ps --filter name=db-node --format "{{.Names}}" | head -1); \
	if [ -n "$$CONTAINER" ]; then \
		echo "$(GREEN)Logs de $$CONTAINER:$(NC)"; \
		docker logs -f $$CONTAINER; \
	else \
		echo "$(RED)No hay DatabaseNodes en ejecución$(NC)"; \
	fi

logs-client: ## Ver logs del primer Cliente
	@CONTAINER=$$(docker ps --filter name=client --format "{{.Names}}" | head -1); \
	if [ -n "$$CONTAINER" ]; then \
		echo "$(GREEN)Logs de $$CONTAINER:$(NC)"; \
		docker logs -f $$CONTAINER; \
	else \
		echo "$(RED)No hay Clientes en ejecución$(NC)"; \
	fi

logs-streamlit: ## Ver logs de Streamlit UI
	@if docker ps --filter name=streamlit-app --format "{{.Names}}" | grep -q streamlit-app; then \
		echo "$(GREEN)Logs de streamlit-app:$(NC)"; \
		docker logs -f streamlit-app; \
	else \
		echo "$(RED)Streamlit UI no está en ejecución$(NC)"; \
	fi

exec-client: ## Conectar al primer cliente disponible
	@CONTAINER=$$(docker ps --filter name=client --format "{{.Names}}" | head -1); \
	if [ -n "$$CONTAINER" ]; then \
		echo "$(GREEN)Conectando a $$CONTAINER$(NC)"; \
		docker exec -it $$CONTAINER /bin/bash; \
	else \
		echo "$(RED)No hay clientes en ejecución$(NC)"; \
	fi

# =============================================================================
# DOCKER SWARM
# =============================================================================

swarm-init: ## Inicializar Docker Swarm en esta máquina
	@echo "$(YELLOW)Inicializando Docker Swarm...$(NC)"
	@IP=$$(hostname -I | awk '{print $$1}'); \
	docker swarm init --advertise-addr $$IP 2>/dev/null || echo "$(GREEN)Swarm ya está inicializado$(NC)"
	@echo "$(GREEN)✅ Swarm inicializado$(NC)"
	@$(MAKE) network
	@echo ""
	@echo "$(YELLOW)Para unir otros nodos como workers, ejecuta en esas máquinas:$(NC)"
	@echo "  make swarm-join-worker"
	@echo ""
	@echo "$(YELLOW)Para obtener el token manualmente:$(NC)"
	@echo "  make swarm-token-worker"

swarm-token-worker: ## Obtener token para unir workers al swarm
	@echo "$(GREEN)Token para unir Workers:$(NC)"
	@docker swarm join-token worker

swarm-token-manager: ## Obtener token para unir managers al swarm
	@echo "$(GREEN)Token para unir Managers:$(NC)"
	@docker swarm join-token manager

swarm-join-worker: ## Unir esta máquina al swarm como worker (requiere TOKEN y MANAGER_IP)
	@if [ -z "$(TOKEN)" ] || [ -z "$(MANAGER_IP)" ]; then \
		echo "$(RED)Error: Debes especificar TOKEN y MANAGER_IP$(NC)"; \
		echo "$(YELLOW)Uso: make swarm-join-worker TOKEN='SWMTKN-...' MANAGER_IP='192.168.1.100:2377'$(NC)"; \
		exit 1; \
	fi
	@echo "$(YELLOW)Uniéndose al swarm...$(NC)"
	docker swarm join --token $(TOKEN) $(MANAGER_IP)
	@echo "$(GREEN)✅ Unido al swarm exitosamente$(NC)"

swarm-nodes: ## Listar todos los nodos del swarm
	@echo "$(GREEN)Nodos del Swarm:$(NC)"
	@docker node ls

swarm-info: ## Mostrar información detallada del swarm
	@echo "$(GREEN)Información del Swarm:$(NC)"
	@docker info | grep -A 10 "Swarm:"

swarm-leave: ## Salir del swarm (esta máquina)
	@echo "$(YELLOW)Saliendo del swarm...$(NC)"
	@read -p "¿Estás seguro? (s/n): " confirm; \
	if [ "$$confirm" = "s" ]; then \
		docker swarm leave --force; \
		echo "$(GREEN)✅ Has salido del swarm$(NC)"; \
	else \
		echo "$(YELLOW)Cancelado$(NC)"; \
	fi

swarm-promote: ## Promover un nodo worker a manager (requiere NODE_ID)
	@if [ -z "$(NODE_ID)" ]; then \
		echo "$(RED)Error: Debes especificar NODE_ID$(NC)"; \
		echo "$(YELLOW)Uso: make swarm-promote NODE_ID='abc123'$(NC)"; \
		echo "$(YELLOW)Usa 'make swarm-nodes' para ver los IDs$(NC)"; \
		exit 1; \
	fi
	@echo "$(YELLOW)Promoviendo nodo $(NODE_ID) a manager...$(NC)"
	docker node promote $(NODE_ID)
	@echo "$(GREEN)✅ Nodo promovido$(NC)"

swarm-demote: ## Degradar un nodo manager a worker (requiere NODE_ID)
	@if [ -z "$(NODE_ID)" ]; then \
		echo "$(RED)Error: Debes especificar NODE_ID$(NC)"; \
		echo "$(YELLOW)Uso: make swarm-demote NODE_ID='abc123'$(NC)"; \
		echo "$(YELLOW)Usa 'make swarm-nodes' para ver los IDs$(NC)"; \
		exit 1; \
	fi
	@echo "$(YELLOW)Degradando nodo $(NODE_ID) a worker...$(NC)"
	docker node demote $(NODE_ID)
	@echo "$(GREEN)✅ Nodo degradado$(NC)"

swarm-remove-node: ## Remover un nodo del swarm (requiere NODE_ID)
	@if [ -z "$(NODE_ID)" ]; then \
		echo "$(RED)Error: Debes especificar NODE_ID$(NC)"; \
		echo "$(YELLOW)Uso: make swarm-remove-node NODE_ID='abc123'$(NC)"; \
		echo "$(YELLOW)Usa 'make swarm-nodes' para ver los IDs$(NC)"; \
		exit 1; \
	fi
	@echo "$(YELLOW)Removiendo nodo $(NODE_ID) del swarm...$(NC)"
	docker node rm $(NODE_ID) --force
	@echo "$(GREEN)✅ Nodo removido$(NC)"

swarm-update-node: ## Actualizar disponibilidad de un nodo (requiere NODE_ID y AVAILABILITY=active/pause/drain)
	@if [ -z "$(NODE_ID)" ] || [ -z "$(AVAILABILITY)" ]; then \
		echo "$(RED)Error: Debes especificar NODE_ID y AVAILABILITY$(NC)"; \
		echo "$(YELLOW)Uso: make swarm-update-node NODE_ID='abc123' AVAILABILITY='drain'$(NC)"; \
		echo "$(YELLOW)AVAILABILITY puede ser: active, pause, drain$(NC)"; \
		exit 1; \
	fi
	@echo "$(YELLOW)Actualizando nodo $(NODE_ID) a $(AVAILABILITY)...$(NC)"
	docker node update --availability $(AVAILABILITY) $(NODE_ID)
	@echo "$(GREEN)✅ Nodo actualizado$(NC)"


