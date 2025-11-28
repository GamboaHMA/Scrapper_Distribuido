# Makefile para Scrapper Distribuido
# Basado en los scripts .sh existentes del proyecto

# Variables
COORDINATOR_IMAGE = scrapper-coordinator
SCRAPPER_IMAGE = scrapper-scrapper
GATEWAY_IMAGE = scrapper-gateway
DNS_IMAGE = scrapper-dns
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

# =============================================================================
# BUILD
# =============================================================================

build: ## Construir imágenes del coordinator, scrapper y DNS
	@echo "$(YELLOW)Construyendo imágenes...$(NC)"
	docker build -t $(COORDINATOR_IMAGE) coordinator/
	docker build -t $(SCRAPPER_IMAGE) scrapper/
	docker build -t $(DNS_IMAGE) DNS/
	@echo "$(GREEN)✅ Imágenes construidas$(NC)"

# =============================================================================
# RUN (modo standalone)
# =============================================================================

run-coordinator: network ## Ejecutar coordinator (puertos 8080 TCP y 8081 UDP)
	@echo "$(YELLOW)Iniciando coordinator...$(NC)"
	docker run -d --name scrapper-coordinator \
		--network $(NETWORK_NAME) \
		--publish 8080:8080 \
		--publish 8081:8081/udp \
		$(COORDINATOR_IMAGE)

run-scrapper: network ## Ejecutar scrapper
	docker run -d --name scrapper-scrapper-$(shell date +%s) \
		--network $(NETWORK_NAME) \
		--env COORDINATOR_HOST=scrapper-coordinator \
		--env COORDINATOR_PORT=8080 \
		$(SCRAPPER_IMAGE)

run-scrappers: network ## Ejecutar múltiples scrappers (NUM=3 por defecto)
	@for i in $$(seq 1 $(or $(NUM),3)); do \
		docker run -d --name scrapper-scrapper-$$i \
			--network $(NETWORK_NAME) \
			--env COORDINATOR_HOST=scrapper-coordinator \
			--env COORDINATOR_PORT=8080 \
			$(SCRAPPER_IMAGE); \
	done

start-gateway: ## Iniciar Gateway/API REST (puerto 8082)
	@echo "$(YELLOW)Iniciando gateway...$(NC)"
	python3 coordinator/gateway.py

logs-dns: ## Ver logs del DNS
	@if docker ps --filter name=scrapper-dns -q | grep -q .; then \
		docker logs -f scrapper-dns; \
	else \
		docker service logs -f scrapper-dns 2>/dev/null || echo "DNS no encontrado"; \
	fi

exec-dns: ## Conectar al container del DNS
	docker exec -it scrapper-dns /bin/sh

run-dns: network ## Ejecutar DNS personalizado (puerto 5353)
	@echo "$(YELLOW)Iniciando DNS...$(NC)"
	docker run -d --name scrapper-dns \
		--network $(NETWORK_NAME) \
		--publish 5353:5353 \
		--env DNS_HOST=0.0.0.0 \
		--env DNS_PORT=5353 \
		--env SCAN_PORT=8080 \
		--env NETWORK_RANGE=172.18.0.0/16 \
		$(DNS_IMAGE)

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
		--name scrapper-dns \
		--network $(NETWORK_NAME) \
		--replicas 1 \
		--publish 5353:5353 \
		--env DNS_HOST=0.0.0.0 \
		--env DNS_PORT=5353 \
		--env SCAN_PORT=8080 \
		--env NETWORK_RANGE=172.18.0.0/16 \
		$(DNS_IMAGE)
	docker service create \
		--name scrapper-coordinator \
		--network $(NETWORK_NAME) \
		--replicas 1 \
		--publish 8080:8080 \
		$(COORDINATOR_IMAGE)
	docker service create \
		--name scrapper-scrapper \
		--network $(NETWORK_NAME) \
		--replicas 3 \
		--env COORDINATOR_HOST=scrapper-coordinator \
		--env COORDINATOR_PORT=8080 \
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
	@echo "$(YELLOW)Logs del coordinator:$(NC)"
	docker service logs scrapper-coordinator
	@echo "$(YELLOW)Logs de scrappers:$(NC)"
	docker service logs scrapper-scrapper

swarm-cleanup: ## Eliminar servicios y salir del swarm (basado en cleanup_swarm.sh)
	@echo "$(RED)Limpiando servicios...$(NC)"
	-docker service rm scrapper-scrapper
	-docker service rm scrapper-coordinator
	-docker service rm scrapper-dns
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
	@docker ps --filter ancestor=$(COORDINATOR_IMAGE) --filter ancestor=$(SCRAPPER_IMAGE) --filter ancestor=$(DNS_IMAGE)
	@echo ""
	@echo "$(GREEN)Servicios swarm:$(NC)"
	@docker service ls 2>/dev/null || echo "No hay servicios de swarm"

logs: ## Ver logs del coordinator
	@if docker ps --filter name=scrapper-coordinator -q | grep -q .; then \
		docker logs -f scrapper-coordinator; \
	else \
		docker service logs -f scrapper-coordinator 2>/dev/null || echo "Coordinator no encontrado"; \
	fi

ps: ## Mostrar todos los containers del proyecto
	docker ps --filter ancestor=$(COORDINATOR_IMAGE) --filter ancestor=$(SCRAPPER_IMAGE) --filter ancestor=$(DNS_IMAGE)

inspect-coordinator: ## Inspeccionar container/servicio del coordinator
	@if docker ps --filter name=scrapper-coordinator -q | grep -q .; then \
		docker inspect scrapper-coordinator; \
	else \
		docker service inspect scrapper-coordinator 2>/dev/null || echo "Coordinator no encontrado"; \
	fi

exec-coordinator: ## Conectar al container del coordinator
	docker exec -it scrapper-coordinator /bin/bash

# =============================================================================
# CLEANUP
# =============================================================================

stop: ## Detener todos los containers
	@echo "$(YELLOW)Deteniendo containers...$(NC)"
	-docker stop $$(docker ps -q --filter ancestor=$(COORDINATOR_IMAGE) --filter ancestor=$(SCRAPPER_IMAGE) --filter ancestor=$(DNS_IMAGE))

clean: stop ## Limpiar containers, imágenes y red
	@echo "$(YELLOW)Limpiando containers...$(NC)"
	-docker rm $$(docker ps -aq --filter ancestor=$(COORDINATOR_IMAGE) --filter ancestor=$(SCRAPPER_IMAGE) --filter ancestor=$(DNS_IMAGE))
	@echo "$(YELLOW)Limpiando imágenes...$(NC)"
	-docker rmi $(COORDINATOR_IMAGE) $(SCRAPPER_IMAGE) $(DNS_IMAGE)
	@echo "$(YELLOW)Limpiando red...$(NC)"
	-docker network rm $(NETWORK_NAME)

# =============================================================================
# SHORTCUTS
# =============================================================================

demo: build run-dns run-coordinator ## Inicio rápido para desarrollo local
	@sleep 3
	@$(MAKE) run-scrappers NUM=2
	@echo "$(GREEN)Demo iniciado con DNS personalizado. Usar 'make start-gateway' para Gateway/API$(NC)"