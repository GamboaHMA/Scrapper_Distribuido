#!/usr/bin/env python3
"""
Registry/DNS Service para Scrapper Distribuido
Escanea la red activamente y registra todos los servicios disponibles
"""

import socket
import threading
import json
import time
import logging
import ipaddress
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class ServiceRegistry:
    def __init__(self, network_range="192.168.1.0/24", scan_port=8080, registry_port=5353):
        """
        Inicializa el Registry/DNS
        
        Args:
            network_range: Rango de IPs a escanear (notación CIDR)
            scan_port: Puerto donde los servicios responden
            registry_port: Puerto donde este registry escucha consultas
        """
        self.network_range = network_range
        self.scan_port = scan_port
        self.registry_port = registry_port
        
        # Estructura de datos para servicios registrados
        # {service_id: {type, ip, port, last_seen, metadata}}
        self.services = {}
        self.services_lock = threading.Lock()
        
        # Control de threads
        self.running = False
        self.scan_interval = 30  # Segundos entre escaneos
        self.heartbeat_timeout = 60  # Segundos sin respuesta = servicio muerto
        
        # Socket del servidor
        self.server_socket = None
        
        self.logger = logging.getLogger('Registry')
        
    def start(self):
        """Inicia el registry"""
        self.running = True
        self.logger.info(f"🔍 Registry iniciado - Escaneando {self.network_range}")
        
        # Thread para escaneo de red
        scan_thread = threading.Thread(target=self._scan_network_loop, daemon=True)
        scan_thread.start()
        
        # Thread para limpieza de servicios muertos
        cleanup_thread = threading.Thread(target=self._cleanup_dead_services, daemon=True)
        cleanup_thread.start()

        # Thread para servidor de consultas
        server_thread = threading.Thread(target=self._start_query_server, daemon=True)
        server_thread.start()
        
        self.logger.info(f"📡 Registry escuchando en puerto {self.registry_port}")
        
        # Mantener el programa corriendo
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Registry detenido por usuario")
            self.stop()
    
    def stop(self):
        """Detiene el registry"""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
    
    def _scan_network_loop(self):
        """Loop principal de escaneo de red"""
        while self.running:
            self.logger.info(f"🔎 Iniciando escaneo de red: {self.network_range}")
            start_time = time.time()
            
            discovered = self._scan_network()
            
            elapsed = time.time() - start_time
            self.logger.info(f"✅ Escaneo completado en {elapsed:.2f}s - {discovered} servicios descubiertos")
            
            # Esperar antes del próximo escaneo
            time.sleep(self.scan_interval)
    
    def _scan_network(self) -> int:
        """
        Escanea la red IP por IP buscando servicios
        
        Returns:
            Número de servicios descubiertos
        """
        discovered_count = 0
        network = ipaddress.IPv4Network(self.network_range, strict=False)
        
        # Lista de IPs a escanear
        ips_to_scan = [str(ip) for ip in network.hosts()]
        
        self.logger.info(f"📋 Escaneando {len(ips_to_scan)} IPs...")
        
        # Escanear cada IP
        for ip in ips_to_scan:
            if not self.running:
                break
            
            service = self._probe_service(ip, self.scan_port)
            if service:
                self._register_service(service)
                discovered_count += 1
        
        return discovered_count
    
    def _probe_service(self, ip: str, port: int, timeout=1) -> Optional[Dict]:
        """
        Intenta conectarse a una IP:puerto y determinar si hay un servicio
        
        Args:
            ip: Dirección IP a probar
            port: Puerto a probar
            timeout: Timeout en segundos
            
        Returns:
            Dict con info del servicio o None si no hay servicio
        """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            
            result = sock.connect_ex((ip, port))
            
            if result == 0:
                # Puerto abierto, intentar identificar el servicio
                service_info = self._identify_service(sock, ip, port)
                sock.close()
                return service_info
            
            sock.close()
            return None
            
        except socket.error:
            return None
        except Exception as e:
            self.logger.debug(f"Error probando {ip}:{port} - {e}")
            return None
    
    def _identify_service(self, sock: socket.socket, ip: str, port: int) -> Optional[Dict]:
        """
        Intenta identificar qué tipo de servicio está corriendo
        
        Args:
            sock: Socket conectado
            ip: IP del servicio
            port: Puerto del servicio
            
        Returns:
            Dict con información del servicio
        """
        try:
            # Enviar mensaje de identificación
            identify_msg = {
                'type': 'identify',
                'timestamp': datetime.now().isoformat()
            }
            
            sock.send(json.dumps(identify_msg).encode() + b'\n')
            sock.settimeout(2)
            
            # Recibir respuesta
            response = sock.recv(4096).decode().strip()
            
            if response:
                data = json.loads(response)
                
                service_type = data.get('service_type', 'unknown')
                service_id = data.get('service_id', f"{service_type}_{ip}_{port}")
                
                service_info = {
                    'service_id': service_id,
                    'type': service_type,
                    'ip': ip,
                    'port': port,
                    'last_seen': datetime.now(),
                    'metadata': data.get('metadata', {})
                }
                
                self.logger.debug(f"✓ Servicio identificado: {service_type} en {ip}:{port}")
                return service_info
            
            return None
            
        except socket.timeout:
            # Si no responde al identify, asumimos que es un servicio genérico
            return {
                'service_id': f"unknown_{ip}_{port}",
                'type': 'unknown',
                'ip': ip,
                'port': port,
                'last_seen': datetime.now(),
                'metadata': {}
            }
        except Exception as e:
            self.logger.debug(f"Error identificando servicio en {ip}:{port} - {e}")
            return None
    
    def _register_service(self, service_info: Dict):
        """
        Registra o actualiza un servicio en el registry
        
        Args:
            service_info: Información del servicio
        """
        with self.services_lock:
            service_id = service_info['service_id']
            
            if service_id not in self.services:
                self.logger.info(f"➕ Nuevo servicio registrado: {service_info['type']} ({service_id}) en {service_info['ip']}:{service_info['port']}")
            
            self.services[service_id] = service_info
    
    def _cleanup_dead_services(self):
        """Elimina servicios que no responden hace tiempo"""
        while self.running:
            time.sleep(10)  # Revisar cada 10 segundos
            
            now = datetime.now()
            to_remove = []
            
            with self.services_lock:
                for service_id, service_info in self.services.items():
                    last_seen = service_info['last_seen']
                    
                    if isinstance(last_seen, str):
                        last_seen = datetime.fromisoformat(last_seen)
                    
                    if (now - last_seen).seconds > self.heartbeat_timeout:
                        to_remove.append(service_id)
                
                for service_id in to_remove:
                    service_info = self.services[service_id]
                    self.logger.warning(f"❌ Servicio eliminado (timeout): {service_info['type']} ({service_id})")
                    del self.services[service_id]
    
    def _start_query_server(self):
        """Inicia servidor para consultas DNS"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind(('0.0.0.0', self.registry_port))
            self.server_socket.listen(5)
            
            while self.running:
                try:
                    self.server_socket.settimeout(1.0)
                    client_socket, client_address = self.server_socket.accept()
                    
                    # Manejar consulta en thread separado
                    query_thread = threading.Thread(
                        target=self._handle_query,
                        args=(client_socket, client_address),
                        daemon=True
                    )
                    query_thread.start()
                    
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.running:
                        self.logger.error(f"Error aceptando conexión: {e}")
        
        except Exception as e:
            self.logger.error(f"Error iniciando servidor de consultas: {e}")
    
    def _handle_query(self, client_socket: socket.socket, client_address):
        """
        Maneja una consulta DNS
        
        Args:
            client_socket: Socket del cliente
            client_address: Dirección del cliente
        """
        try:
            # Recibir consulta
            data = client_socket.recv(4096).decode().strip()
            query = json.loads(data)
            
            query_type = query.get('query_type')
            
            if query_type == 'list_all':
                response = self._query_list_all()
            elif query_type == 'find_by_type':
                service_type = query.get('service_type')
                response = self._query_by_type(service_type)
            elif query_type == 'find_by_id':
                service_id = query.get('service_id')
                response = self._query_by_id(service_id)
            else:
                response = {'error': 'Unknown query type'}
            
            # Enviar respuesta
            client_socket.send(json.dumps(response).encode() + b'\n')
            
        except Exception as e:
            self.logger.error(f"Error manejando consulta de {client_address}: {e}")
        finally:
            client_socket.close()
    
    def _query_list_all(self) -> Dict:
        """Retorna todos los servicios registrados"""
        with self.services_lock:
            services_list = []
            for service_id, service_info in self.services.items():
                service_copy = service_info.copy()
                # Convertir datetime a string para JSON
                if isinstance(service_copy['last_seen'], datetime):
                    service_copy['last_seen'] = service_copy['last_seen'].isoformat()
                services_list.append(service_copy)
            
            return {
                'services': services_list,
                'count': len(services_list)
            }
    
    def _query_by_type(self, service_type: str) -> Dict:
        """Retorna servicios de un tipo específico"""
        with self.services_lock:
            filtered = [
                {**s, 'last_seen': s['last_seen'].isoformat() if isinstance(s['last_seen'], datetime) else s['last_seen']}
                for s in self.services.values()
                if s['type'] == service_type
            ]
            
            return {
                'services': filtered,
                'count': len(filtered)
            }
    
    def _query_by_id(self, service_id: str) -> Dict:
        """Retorna información de un servicio específico"""
        with self.services_lock:
            if service_id in self.services:
                service_info = self.services[service_id].copy()
                if isinstance(service_info['last_seen'], datetime):
                    service_info['last_seen'] = service_info['last_seen'].isoformat()
                return {'service': service_info, 'found': True}
            else:
                return {'found': False}


def main():
    import os
    
    # Configuración desde variables de entorno
    network_range = os.environ.get('NETWORK_RANGE', '192.168.1.0/24')
    scan_port = int(os.environ.get('SCAN_PORT', 8080))
    registry_port = int(os.environ.get('REGISTRY_PORT', 5353))
    
    # Crear e iniciar registry
    registry = ServiceRegistry(
        network_range=network_range,
        scan_port=scan_port,
        registry_port=registry_port
    )
    
    registry.start()


if __name__ == '__main__':
    main()
