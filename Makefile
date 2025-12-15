# Makefile para Scrapper Distribuido
# Basado en los scripts .sh existentes del proyecto

# Variables
SERVER_IMAGE = scrapper-server
SCRAPPER_IMAGE = scrapper-scrapper
NETWORK_NAME = scrapper-network

# Colores
GREEN = \033[0;32m
YELLOW = \033[1;33m
RED = \033[0;31m
NC = \033[0m

.PHONY: help build run clean logs status swarm network

help: ## Mostrar comandos disponibles
	@echo "$(GREEN)Scrapper Distribuido - Comandos Docker$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "$(YELLOW)%-20s$(NC) %s\n", $$1, $$2}'

# =============================================================================
# NETWORK
# =============================================================================

network: ## Crear red overlay scrapper-network
	@echo "$(YELLOW)Creando red overlay...$(NC)"
	docker network create --driver overlay --attachable $(NETWORK_NAME) || echo "Red ya existe"
	@echo "$(GREEN)✅ Red overlay creada/verificada$(NC)"

network-inspect: ## Inspeccionar red overlay
	@echo "$(GREEN)Información de la red $(NETWORK_NAME):$(NC)"
	docker network inspect $(NETWORK_NAME)

network-ls: ## Listar todas las redes
	@echo "$(GREEN)Redes disponibles:$(NC)"
	docker network ls

network-clean: ## Eliminar red overlay
	@echo "$(YELLOW)Eliminando red overlay...$(NC)"
	-docker network rm $(NETWORK_NAME)
	@echo "$(GREEN)Red eliminada$(NC)"

# =============================================================================
# BUILD
# =============================================================================

build: ## Construir imágenes del servidor y scrapper
	@echo "$(YELLOW)Construyendo imágenes...$(NC)"
	docker build -t $(SERVER_IMAGE) server/
	docker build -t $(SCRAPPER_IMAGE) scrapper/
	@echo "$(GREEN)✅ Imágenes construidas$(NC)"

# =============================================================================
# RUN (modo standalone)
# =============================================================================

run-server: network ## Ejecutar servidor (puertos 8080 TCP y 8081 UDP)
	@echo "$(YELLOW)Iniciando servidor...$(NC)"
	docker run -d --name scrapper-server \
		--network $(NETWORK_NAME) \
		--publish 8080:8080 \
		--publish 8081:8081/udp \
		$(SERVER_IMAGE)

run-scrapper: network ## Ejecutar scrapper
	docker run -d --name scrapper-scrapper-$(shell date +%s) \
		--network $(NETWORK_NAME) \
		$(SCRAPPER_IMAGE)

run-scrappers: network ## Ejecutar múltiples scrappers (NUM=3 por defecto)
	@for i in $$(seq 1 $(or $(NUM),3)); do \
		docker run -d --name scrapper-scrapper-$$i \
			--network $(NETWORK_NAME) \
			$(SCRAPPER_IMAGE); \
	done

start-api: ## Iniciar API REST (puerto 8082)
	@echo "$(YELLOW)Iniciando API...$(NC)"
	python3 server/api_server.py

# =============================================================================
# DOCKER SWARM (basado en deploy_swarm.sh)
# =============================================================================

swarm-init: ## Inicializar Docker Swarm
	@echo "$(YELLOW)Inicializando Docker Swarm...$(NC)"
	docker swarm init --advertise-addr $$(hostname -I | awk '{print $$1}') || echo "Swarm ya inicializado"
	@$(MAKE) network

swarm-deploy: build swarm-init ## Desplegar servicios en Swarm
	@echo "$(YELLOW)Desplegando servicios...$(NC)"
	docker service create \
		--name scrapper-server \
		--network $(NETWORK_NAME) \
		--replicas 1 \
		--publish 8080:8080 \
		$(SERVER_IMAGE)
	docker service create \
		--name scrapper-scrapper \
		--network $(NETWORK_NAME) \
		--replicas 3 \
		--env SERVER_HOST=scrapper-server \
		--env SERVER_PORT=8080 \
		$(SCRAPPER_IMAGE)

swarm-scale: ## Escalar scrappers (REPLICAS=número)
	docker service scale scrapper-scrapper=$(REPLICAS)

swarm-token: ## Obtener token para unir workers
	docker swarm join-token worker

swarm-join: ## Unirse a swarm (TOKEN=... MANAGER_IP=...)
	docker swarm join --token $(TOKEN) $(MANAGER_IP)

swarm-services: ## Listar servicios del swarm
	docker service ls

swarm-logs: ## Ver logs de servicios
	@echo "$(YELLOW)Logs del servidor:$(NC)"
	docker service logs scrapper-server
	@echo "$(YELLOW)Logs de scrappers:$(NC)"
	docker service logs scrapper-scrapper

swarm-cleanup: ## Eliminar servicios y salir del swarm (basado en cleanup_swarm.sh)
	@echo "$(RED)Limpiando servicios...$(NC)"
	-docker service rm scrapper-scrapper
	-docker service rm scrapper-server
	-docker network rm $(NETWORK_NAME)
	@echo "$(YELLOW)Para salir del swarm: docker swarm leave --force$(NC)"

# =============================================================================
# MONITORING & UTILS
# =============================================================================

status: ## Ver estado de containers/servicios
	@echo "$(GREEN)Red overlay:$(NC)"
	@docker network ls --filter name=$(NETWORK_NAME) || echo "Red no encontrada"
	@echo ""
	@echo "$(GREEN)Containers standalone:$(NC)"
	@docker ps --filter ancestor=$(SERVER_IMAGE) --filter ancestor=$(SCRAPPER_IMAGE)
	@echo ""
	@echo "$(GREEN)Servicios swarm:$(NC)"
	@docker service ls 2>/dev/null || echo "No hay servicios de swarm"

logs: ## Ver logs del servidor
	@if docker ps --filter name=scrapper-server -q | grep -q .; then \
		docker logs -f scrapper-server; \
	else \
		docker service logs -f scrapper-server 2>/dev/null || echo "Servidor no encontrado"; \
	fi

ps: ## Mostrar todos los containers del proyecto
	docker ps --filter ancestor=$(SERVER_IMAGE) --filter ancestor=$(SCRAPPER_IMAGE)

inspect-server: ## Inspeccionar container/servicio del servidor
	@if docker ps --filter name=scrapper-server -q | grep -q .; then \
		docker inspect scrapper-server; \
	else \
		docker service inspect scrapper-server 2>/dev/null || echo "Servidor no encontrado"; \
	fi

exec-server: ## Conectar al container del servidor
	docker exec -it scrapper-server /bin/bash

exec-scrapper: ## Conectar al primer container scrapper disponible
	@CONTAINER=$$(docker ps --filter ancestor=$(SCRAPPER_IMAGE) --format "{{.Names}}" | head -1); \
	if [ -n "$$CONTAINER" ]; then \
		echo "$(GREEN)Conectando a $$CONTAINER$(NC)"; \
		docker exec -it $$CONTAINER /bin/bash; \
	else \
		echo "$(RED)No hay contenedores scrapper en ejecución$(NC)"; \
	fi

# =============================================================================
# CLEANUP
# =============================================================================

stop: ## Detener todos los containers
	@echo "$(YELLOW)Deteniendo containers...$(NC)"
	-docker stop $$(docker ps -q --filter ancestor=$(SERVER_IMAGE) --filter ancestor=$(SCRAPPER_IMAGE))

clean: stop ## Limpiar containers, imágenes y red
	@echo "$(YELLOW)Limpiando containers...$(NC)"
	-docker rm $$(docker ps -aq --filter ancestor=$(SERVER_IMAGE) --filter ancestor=$(SCRAPPER_IMAGE))
	@echo "$(YELLOW)Limpiando imágenes...$(NC)"
	-docker rmi $(SERVER_IMAGE) $(SCRAPPER_IMAGE)
	@echo "$(YELLOW)Limpiando red...$(NC)"
	-docker network rm $(NETWORK_NAME)

# =============================================================================
# SHORTCUTS
# =============================================================================

demo: build run-server ## Inicio rápido para desarrollo local
	@sleep 3
	@$(MAKE) run-scrappers NUM=2
	@echo "$(GREEN)Demo iniciado en red overlay $(NETWORK_NAME)$(NC)"
	@echo "$(YELLOW)Comandos útiles:$(NC)"
	@echo "  make status             # Ver estado general"
	@echo "  make start-api          # Iniciar API REST$(NC)"

# =============================================================================
# SCRAPPER NODE (Nueva Arquitectura con Node Base)
# =============================================================================

SCRAPPER_NODE_IMAGE = scrapper_node

build-scrapper-node: ## Construir imagen del nuevo ScrapperNode (hereda de Node)
	@echo "$(YELLOW)Construyendo ScrapperNode (con herencia de Node base)...$(NC)"
	docker build -t $(SCRAPPER_NODE_IMAGE) -f scrapper/Dockerfile .
	@echo "$(GREEN)✅ ScrapperNode construido$(NC)"

run-scrapper-node: network ## Ejecutar un ScrapperNode
	@echo "$(YELLOW)Iniciando ScrapperNode...$(NC)"
	docker run -d --name scrapper-node-$(shell date +%s) \
		--network $(NETWORK_NAME) \
		--network-alias scrapper \
		-e LOG_LEVEL=INFO \
		$(SCRAPPER_NODE_IMAGE)
	@echo "$(GREEN)✅ ScrapperNode iniciado$(NC)"

run-scrapper-node-debug: network ## Ejecutar ScrapperNode en modo DEBUG
	@echo "$(YELLOW)Iniciando ScrapperNode en modo DEBUG...$(NC)"
	docker run -it --rm --name scrapper-node-debug \
		--network $(NETWORK_NAME) \
		--network-alias scrapper \
		-e LOG_LEVEL=DEBUG \
		$(SCRAPPER_NODE_IMAGE)

run-scrapper-nodes: network ## Ejecutar múltiples ScrapperNodes (NUM=3 por defecto)
	@echo "$(YELLOW)Iniciando $(or $(NUM),3) ScrapperNodes...$(NC)"
	@for i in $$(seq 1 $(or $(NUM),3)); do \
		docker run -d --name scrapper-node-$$i \
			--network $(NETWORK_NAME) \
			--network-alias scrapper \
			-e LOG_LEVEL=INFO \
			$(SCRAPPER_NODE_IMAGE); \
		echo "$(GREEN)✅ ScrapperNode $$i iniciado$(NC)"; \
	done

clean-scrapper-nodes: ## Limpiar todos los ScrapperNodes
	@echo "$(YELLOW)Limpiando ScrapperNodes...$(NC)"
	-docker stop $$(docker ps -aq --filter ancestor=$(SCRAPPER_NODE_IMAGE))
	-docker rm $$(docker ps -aq --filter ancestor=$(SCRAPPER_NODE_IMAGE))
	@echo "$(GREEN)✅ ScrapperNodes limpiados$(NC)"

logs-scrapper-node: ## Ver logs del primer ScrapperNode
	@CONTAINER=$$(docker ps --filter ancestor=$(SCRAPPER_NODE_IMAGE) --format "{{.Names}}" | head -1); \
	if [ -n "$$CONTAINER" ]; then \
		echo "$(GREEN)Logs de $$CONTAINER:$(NC)"; \
		docker logs -f $$CONTAINER; \
	else \
		echo "$(RED)No hay ScrapperNodes en ejecución$(NC)"; \
	fi

# =============================================================================
# ROUTER NODE (Nueva Arquitectura con Node Base)
# =============================================================================

ROUTER_NODE_IMAGE = router_node

build-router-node: ## Construir imagen del RouterNode (hereda de Node)
	@echo "$(YELLOW)Construyendo RouterNode (con herencia de Node base)...$(NC)"
	docker build -t $(ROUTER_NODE_IMAGE) -f router/Dockerfile .
	@echo "$(GREEN)✅ RouterNode construido$(NC)"

run-router-node: network ## Ejecutar un RouterNode
	@echo "$(YELLOW)Iniciando RouterNode...$(NC)"
	docker run -d --name router-node-$(shell date +%s) \
		--network $(NETWORK_NAME) \
		--network-alias router \
		-e LOG_LEVEL=INFO \
		$(ROUTER_NODE_IMAGE)
	@echo "$(GREEN)✅ RouterNode iniciado$(NC)"

run-router-node-debug: network ## Ejecutar RouterNode en modo DEBUG
	@echo "$(YELLOW)Iniciando RouterNode en modo DEBUG...$(NC)"
	docker run -it --rm --name router-node-debug \
		--network $(NETWORK_NAME) \
		--network-alias router \
		-e LOG_LEVEL=DEBUG \
		$(ROUTER_NODE_IMAGE)

run-router-nodes: network ## Ejecutar múltiples RouterNodes (NUM=3 por defecto)
	@echo "$(YELLOW)Iniciando $(or $(NUM),3) RouterNodes...$(NC)"
	@for i in $$(seq 1 $(or $(NUM),3)); do \
		docker run -d --name router-node-$$i \
			--network $(NETWORK_NAME) \
			--network-alias router \
			-e LOG_LEVEL=INFO \
			$(ROUTER_NODE_IMAGE); \
		echo "$(GREEN)✅ RouterNode $$i iniciado$(NC)"; \
	done

clean-router-nodes: ## Limpiar todos los RouterNodes
	@echo "$(YELLOW)Limpiando RouterNodes...$(NC)"
	-docker stop $$(docker ps -aq --filter ancestor=$(ROUTER_NODE_IMAGE))
	-docker rm $$(docker ps -aq --filter ancestor=$(ROUTER_NODE_IMAGE))
	@echo "$(GREEN)✅ RouterNodes limpiados$(NC)"

logs-router-node: ## Ver logs del primer RouterNode
	@CONTAINER=$$(docker ps --filter ancestor=$(ROUTER_NODE_IMAGE) --format "{{.Names}}" | head -1); \
	if [ -n "$$CONTAINER" ]; then \
		echo "$(GREEN)Logs de $$CONTAINER:$(NC)"; \
		docker logs -f $$CONTAINER; \
	else \
		echo "$(RED)No hay RouterNodes en ejecución$(NC)"; \
	fi
