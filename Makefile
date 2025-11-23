# Makefile para Scrapper Distribuido
# Basado en los scripts .sh existentes del proyecto

# Variables
SERVER_IMAGE = scrapper-server
CLIENT_IMAGE = scrapper-client
NETWORK_NAME = scrapper-network

# Colores
GREEN = \033[0;32m
YELLOW = \033[1;33m
RED = \033[0;31m
NC = \033[0m

.PHONY: help build run clean logs status swarm

help: ## Mostrar comandos disponibles
	@echo "$(GREEN)Scrapper Distribuido - Comandos Docker$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "$(YELLOW)%-20s$(NC) %s\n", $$1, $$2}'

# =============================================================================
# BUILD
# =============================================================================

build: ## Construir imágenes del servidor y cliente
	@echo "$(YELLOW)Construyendo imágenes...$(NC)"
	docker build -t $(SERVER_IMAGE) server/
	docker build -t $(CLIENT_IMAGE) client/
	@echo "$(GREEN)✅ Imágenes construidas$(NC)"

# =============================================================================
# RUN (modo standalone)
# =============================================================================

run-server: ## Ejecutar servidor (puertos 8080 TCP y 8081 UDP)
	@echo "$(YELLOW)Iniciando servidor...$(NC)"
	docker run -d --name scrapper-server \
		--publish 8080:8080 \
		--publish 8081:8081/udp \
		$(SERVER_IMAGE)

run-client: ## Ejecutar cliente
	docker run -d --name scrapper-client-$(shell date +%s) $(CLIENT_IMAGE)

run-clients: ## Ejecutar múltiples clientes (NUM=3 por defecto)
	@for i in $$(seq 1 $(or $(NUM),3)); do \
		docker run -d --name scrapper-client-$$i $(CLIENT_IMAGE); \
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
	docker network create --driver overlay --attachable $(NETWORK_NAME) || echo "Red ya existe"

swarm-deploy: build swarm-init ## Desplegar servicios en Swarm
	@echo "$(YELLOW)Desplegando servicios...$(NC)"
	docker service create \
		--name scrapper-server \
		--network $(NETWORK_NAME) \
		--replicas 1 \
		--publish 8080:8080 \
		$(SERVER_IMAGE)
	docker service create \
		--name scrapper-client \
		--network $(NETWORK_NAME) \
		--replicas 3 \
		--env SERVER_HOST=scrapper-server \
		--env SERVER_PORT=8080 \
		$(CLIENT_IMAGE)

swarm-scale: ## Escalar clientes (REPLICAS=número)
	docker service scale scrapper-client=$(REPLICAS)

swarm-token: ## Obtener token para unir workers
	docker swarm join-token worker

swarm-join: ## Unirse a swarm (TOKEN=... MANAGER_IP=...)
	docker swarm join --token $(TOKEN) $(MANAGER_IP)

swarm-services: ## Listar servicios del swarm
	docker service ls

swarm-logs: ## Ver logs de servicios
	@echo "$(YELLOW)Logs del servidor:$(NC)"
	docker service logs scrapper-server
	@echo "$(YELLOW)Logs de clientes:$(NC)"
	docker service logs scrapper-client

swarm-cleanup: ## Eliminar servicios y salir del swarm (basado en cleanup_swarm.sh)
	@echo "$(RED)Limpiando servicios...$(NC)"
	-docker service rm scrapper-client
	-docker service rm scrapper-server
	-docker network rm $(NETWORK_NAME)
	@echo "$(YELLOW)Para salir del swarm: docker swarm leave --force$(NC)"

# =============================================================================
# MONITORING & UTILS
# =============================================================================

status: ## Ver estado de containers/servicios
	@echo "$(GREEN)Containers standalone:$(NC)"
	@docker ps --filter ancestor=$(SERVER_IMAGE) --filter ancestor=$(CLIENT_IMAGE)
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
	docker ps --filter ancestor=$(SERVER_IMAGE) --filter ancestor=$(CLIENT_IMAGE)

inspect-server: ## Inspeccionar container/servicio del servidor
	@if docker ps --filter name=scrapper-server -q | grep -q .; then \
		docker inspect scrapper-server; \
	else \
		docker service inspect scrapper-server 2>/dev/null || echo "Servidor no encontrado"; \
	fi

exec-server: ## Conectar al container del servidor
	docker exec -it scrapper-server /bin/bash

# =============================================================================
# CLEANUP
# =============================================================================

stop: ## Detener todos los containers
	@echo "$(YELLOW)Deteniendo containers...$(NC)"
	-docker stop $$(docker ps -q --filter ancestor=$(SERVER_IMAGE) --filter ancestor=$(CLIENT_IMAGE))

clean: stop ## Limpiar containers e imágenes
	@echo "$(YELLOW)Limpiando containers...$(NC)"
	-docker rm $$(docker ps -aq --filter ancestor=$(SERVER_IMAGE) --filter ancestor=$(CLIENT_IMAGE))
	@echo "$(YELLOW)Limpiando imágenes...$(NC)"
	-docker rmi $(SERVER_IMAGE) $(CLIENT_IMAGE)

# =============================================================================
# SHORTCUTS
# =============================================================================

demo: build run-server ## Inicio rápido para desarrollo local
	@sleep 3
	@$(MAKE) run-clients NUM=2
	@echo "$(GREEN)Demo iniciado. Usar 'make start-api' para API REST$(NC)"