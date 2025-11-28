"""
DNS Module
"""
import socket
import json
import threading
import time
import ipaddress
import logging
import os
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class DNSRegistry:
    def __init__(self, host='localhost', scan_port=8080, port=5353, network_ranges=None):
        self.host = host
        self.port = port
        self.scan_port = scan_port
        self.services = {} # 'service_name': {'type': ..., 'id': ..., 'address': ..., 'port': ..., 'last_heartbeat': ...}
        self.dns_services = {} # 'dns_name': {'address': ..., 'port': ..., 'last_heartbeat': ...}
        self.lock = threading.Lock()
        self._id_lock = threading.Lock()  
        self._id_counter = 0
        
        self.running = False
        self.scan_interval = 30  # seconds
        self.heartbeat_timeout = 60  # seconds
        
        # Configurar rangos de red (por defecto o desde parámetro)
        self.network_ranges = network_ranges or ["192.168.1.0/24", "10.0.0.0/24", "172.16.0.0/24", "172.18.0.0/16"]
        # Para buscar otros DNS, usar los mismos rangos de red pero en puerto 5353
        
    def start(self):
        """Inicia hilos para escanear rangos de red y limpiar servicios inactivos."""
        self.running = True
        threading.Thread(target=self._scan_network, daemon=True).start()
        threading.Thread(target=self._cleanup_services, daemon=True).start()
        
        threading.Thread(target=self._scan_dns_port, daemon=True).start()
        threading.Thread(target=self._cleanup_dns_services, daemon=True).start()
        threading.Thread(target=self._listen_port, daemon=True).start()
        # threading.Thread(target=self._start_query_server, daemon=True).start()
        logging.info("DNS Registry started on %s:%d", self.host, self.port)
        
    def stop(self):
        self.running = False
        logging.info("DNS Registry stopped")
        
    def _get_next_id(self):
        """Thread-safe ID generation"""
        with self._id_lock:
            self._id_counter += 1
            return self._id_counter
        
    def _scan_network(self):
        while self.running:
            logging.info("Scanning network ranges for services...")
            for net_range in self.network_ranges:
                threading.Thread(target=self._scan_range, args=(net_range,), daemon=True).start()
            time.sleep(self.scan_interval)
            
    def _scan_range(self, net_range):
        """Escanea varios rangos de red.

        Args:
            net_range (str): Rango de red en formato CIDR (e.g., '192.168.1.0/24')
        """
        network = ipaddress.ip_network(net_range)
        for ip in network.hosts():
            if not self.running:
                break
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1)
                sock.connect((str(ip), self.port))
                # si el ip ya está registrado, saltarlo
                with self.lock:
                    if any(s['address'] == str(ip) for s in self.services.values()):
                        continue
                query = {'query_type': 'identify_service'}
                sock.send(json.dumps(query).encode() + b'\n')
                response = sock.recv(4096).decode().strip()
                service_info = json.loads(response)
                #Estructura esperada del service_info:
                # {'service_type': 'type'}
                # El id se asigna secuencialmente
                # service_name = service_type + '_' + str(id)
                if service_info.get('service_type'):
                    # Generar ID único thread-safe
                    unique_id = self._get_next_id()
                    service_name = f"{service_info['service_type']}_{unique_id}"
                    
                    # Registrar servicio
                    with self.lock:
                        self.services[service_name] = {
                            'type': service_info['service_type'],
                            'id': unique_id,
                            'address': str(ip),
                            'port': self.scan_port,
                            'last_heartbeat': datetime.now(),
                            'service_socket': sock
                        }
                    
                    logging.info("Discovered service: %s at %s", service_name, str(ip))
                    
                    # Enviar confirmación de registro
                    registration_msg = {
                        'type': 'registration',
                        'service_name': service_name
                    }
                    sock.send(json.dumps(registration_msg).encode() + b'\n')
                    
                    # Iniciar hilo de comunicación continua (heartbeat)
                    threading.Thread(target=self._handle_service_connection, args=(service_name,), daemon=True).start()
                else:
                    logging.warning("Invalid service info from %s", str(ip))
                    sock.close()
                        
            except (socket.timeout, ConnectionRefusedError, json.JSONDecodeError):
                continue
            except Exception as e:
                logging.error("Error scanning IP %s: %s", str(ip), str(e))  
    
    def _handle_service_connection(self, service_name):
        """
        Maneja la conexión persistente con un servicio.
        Recibe heartbeats y mensajes de comunicación.
        
        Args:
            service_name (str): Nombre del servicio registrado
        """
        try:
            service_info = self.services[service_name]
            with service_info['service_socket'] as sock:
                sock.settimeout(10)
                while self.running:
                    data = sock.recv(4096).decode().strip()
                    if not data:
                        logging.info("Service %s disconnected", service_name)
                        break
                    message = json.loads(data)
                    if message.get('type') == 'heartbeat':
                        with self.lock:
                            if service_name in self.services:
                                self.services[service_name]['last_heartbeat'] = datetime.now()
                        logging.info("Received heartbeat from %s", service_name)
                    if message.get('type') == 'communication':
                        # DNS recibe un mensaje de comunicación entre servicios
                        # Estructura del mensaje:
                        # {'type': 'communication', 'from': 'service_name', 'to': 'service_name', 'message': '...'}
                        if message.get('from') != service_name:
                            #ATTENTION: el servicio que envía el mensaje debe conocer su propio nombre registrado en el DNS
                            logging.warning("Mismatched 'from' field in communication message from %s", service_name)
                            continue
                        target_service = self.services.get(message.get('to'))
                        if target_service:
                            try:
                                target_sock = self.services[message.get('to')]['service_socket']
                                target_sock.send(json.dumps(message).encode() + b'\n')
                                logging.info("Forwarded message from %s to %s", message.get('from'), message.get('to'))
                            except Exception as e:
                                logging.error("Error forwarding message to %s: %s", message.get('to'), str(e))
                        else:
                            logging.warning("Target service %s not found for communication from %s", message.get('to'), message.get('from'))
        except Exception as e:
            logging.error("Error in heartbeat connection with %s: %s", service_info['service_name'], str(e))
        
    def _cleanup_services(self):
        """
        Erases inactive services that have not sent a heartbeat within the timeout period.
        """
        while self.running:
            time.sleep(self.heartbeat_timeout)
            now = datetime.now()
            with self.lock:
                to_remove = [name for name, info in self.services.items() if now - info['last_heartbeat'] > timedelta(seconds=self.heartbeat_timeout)]
                for name in to_remove:
                    self.services[name]['service_socket'].close()
                    del self.services[name]
                    logging.info("Removed inactive service: %s", name)
                    
############################################################################################################################################################################
########################################### VARIOS DNS ########################################################################################################################
############################################################################################################################################################################
    
    def _scan_dns_port(self):
        """Hilo para escanear otros DNS periódicamente"""
        while self.running:
            logging.info("Scanning network ranges for DNS services...")
            for net_range in self.network_ranges:
                threading.Thread(target=self._scan_for_more_dns, args=(net_range,), daemon=True).start()
            time.sleep(self.scan_interval)
            
    def _scan_for_more_dns(self, net_range):
        """
        Escanea un rango de red en busca de otros servicios DNS.
        """
        logging.info("Scanning for other DNS services...")
        network = ipaddress.ip_network(net_range)
        for ip in network.hosts():
            if not self.running:
                break
            # Si el ip es el mio, saltarlo
            if str(ip) == self.host:
                continue
            try:
                # with socket.create_connection((str(ip), self.port), timeout=1) as sock:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1)
                sock.connect((str(ip), self.port))
                # si el ip ya está registrado, saltarlo
                with self.lock:
                    if str(ip) in self.dns_services:
                        continue
                query = {'query_type': 'identify_dns'}
                sock.send(json.dumps(query).encode() + b'\n')
                response = sock.recv(4096).decode().strip()
                dns_info = json.loads(response)
                if dns_info.get('service_type') == 'DNS':
                    with self.lock:
                        if str(ip) not in self.dns_services:
                            self.dns_services[str(ip)] = {
                                'address': str(ip),
                                'port': self.port,
                                'last_heartbeat': datetime.now()
                            }
                    logging.info("Discovered another DNS service at %s", str(ip))
                    # Aquí podríamos implementar lógica para sincronizar servicios entre DNS
                    threading.Thread(target=self._handle_dns_connection, args=(str(ip),), daemon=True).start()
                    
            except (socket.timeout, ConnectionRefusedError, json.JSONDecodeError):
                continue
            except Exception as e:
                logging.error("Error scanning for DNS at IP %s: %s", str(ip), str(e))
    
    def _handle_dns_connection(self, service_name):
        """
        Maneja la conexión persistente con otro nodo DNS.
        Recibe heartbeats y mensajes de comunicación.
        
        Args:
            service_name (str): Nombre del nodo DNS registrado
        """
        try:
            service_info = self.services[service_name]
            with service_info['service_socket'] as sock:
                sock.settimeout(10)
                while self.running:
                    data = sock.recv(4096).decode().strip()
                    if not data:
                        logging.info("Service %s disconnected", service_name)
                        break
                    message = json.loads(data)
                    if message.get('type') == 'heartbeat':
                        with self.lock:
                            if service_name in self.services:
                                self.services[service_name]['last_heartbeat'] = datetime.now()
                        logging.info("Received heartbeat from %s", service_name)
                    if message.get('type') == 'communication':
                        # DNS recibe un mensaje de comunicación entre servicios
                        # Estructura del mensaje:
                        # {'type': 'communication', 'from': 'service_name', 'to': 'service_name', 'message': '...'}
                        if message.get('from') != service_name:
                            #ATTENTION: el servicio que envía el mensaje debe conocer su propio nombre registrado en el DNS
                            logging.warning("Mismatched 'from' field in communication message from %s", service_name)
                            continue
                        target_service = self.services.get(message.get('to'))
                        if target_service:
                            try:
                                target_sock = self.services[message.get('to')]['service_socket']
                                target_sock.send(json.dumps(message).encode() + b'\n')
                                logging.info("Forwarded message from %s to %s", message.get('from'), message.get('to'))
                            except Exception as e:
                                logging.error("Error forwarding message to %s: %s", message.get('to'), str(e))
                        else:
                            logging.warning("Target service %s not found for communication from %s", message.get('to'), message.get('from'))
        except Exception as e:
            logging.error("Error in heartbeat connection with %s: %s", service_info['service_name'], str(e))
    
    def _listen_port(self):
        """
        Inicia un servidor para escuchar en el puerto DNS y manejar conexiones entrantes.
        """
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.bind((self.host, self.port))
        server_sock.listen(5)
        logging.info("DNS listening on %s:%d", self.host, self.port)
        
        while self.running:
            try:
                client_sock, addr = server_sock.accept()
                threading.Thread(target=self._handle_dns_connection, args=(client_sock,), daemon=True).start()
            except Exception as e:
                logging.error("Error accepting DNS connection: %s", str(e))
                
    def _cleanup_dns_services(self):
        """
        Erases inactive DNS services that have not sent a heartbeat within the timeout period.
        """
        while self.running:
            time.sleep(self.heartbeat_timeout)
            now = datetime.now()
            with self.lock:
                to_remove = [name for name, info in self.dns_services.items() if now - info['last_heartbeat'] > timedelta(seconds=self.heartbeat_timeout)]
                for name in to_remove:
                    del self.dns_services[name]
                    logging.info("Removed inactive DNS service: %s", name)
                    

    ############################################# REVISAR ##########################################################          
    def _query_service(self, query_type, **kwargs):
        """
        Consulta los servicios registrados.
        
        Args:
            query_type (str): Tipo de consulta ('list_all', 'find_by_type', 'find_by_id')
            **kwargs: Parámetros adicionales según el tipo de consulta
        
        Returns:
            Listado o información del servicio consultado
        """
        with self.lock:
            if query_type == 'list_all':
                return list(self.services.keys())
            elif query_type == 'find_by_type':
                service_type = kwargs.get('service_type')
                return [name for name, info in self.services.items() if info['type'] == service_type]
            elif query_type == 'find_by_id':
                service_id = kwargs.get('service_id')
                for name, info in self.services.items():
                    if info['id'] == service_id:
                        return name
                return None
            else:
                return None
            
    def _start_query_server(self):
        """
        Inicia un servidor para responder consultas de servicios.
        """
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.bind((self.host, self.port))
        server_sock.listen(5)
        logging.info("Query server listening on %s:%d", self.host, self.port)
        
        while self.running:
            try:
                client_sock, addr = server_sock.accept()
                threading.Thread(target=self._handle_query_connection, args=(client_sock,), daemon=True).start()
            except Exception as e:
                logging.error("Error accepting query connection: %s", str(e))
    ############## REVISAR #####################################################       
    def _handle_query_connection(self, client_sock):
        """
        Maneja una conexión de consulta entrante.
        
        Args:
            client_sock (socket.socket): Socket del cliente que realiza la consulta
        """
        try:
            data = client_sock.recv(4096).decode().strip()
            query = json.loads(data)
            query_type = query.get('query_type')
            
            if query_type == 'identify_dns':
                # Responder como servicio DNS
                response = {
                    'service_type': 'DNS',
                    'dns_id': f'dns_{self.host}_{self.port}',
                    'registered_services': len(self.services),
                    'other_dns_nodes': len(self.dns_services)
                }
                client_sock.send(json.dumps(response).encode() + b'\n')
            elif query_type:
                response = self._query_service(query_type, **query)
                client_sock.send(json.dumps(response).encode() + b'\n')
            else:
                client_sock.send(json.dumps({'error': 'Invalid query'}).encode() + b'\n')
        except Exception as e:
            logging.error("Error handling query connection: %s", str(e))
        finally:
            client_sock.close()


if __name__ == "__main__":
    # Obtener configuración desde variables de entorno
    host = '0.0.0.0'
    port = int(os.environ.get('DNS_PORT', 5353))
    scan_port = int(os.environ.get('SCAN_PORT', 8080))
    
    # Configurar rango de red desde variable de entorno
    network_range = os.environ.get('NETWORK_RANGE', '172.18.0.0/16')
    network_ranges = [network_range] if network_range else ["192.168.1.0/24", "10.0.0.0/24", "10.0.1.0/24", "172.16.0.0/24", "0.0.0.0/24"]
    # network_ranges = ["192.168.1.0/24", "10.0.0.0/24", "172.16.0.0/24", "0.0.0.0/24"]
    
    logging.info("Iniciando DNS Registry...")
    logging.info(f"Host: {host}:{port}")
    logging.info(f"Puerto de escaneo: {scan_port}")
    # logging.info(f"Rangos de red: {network_ranges}")
    
    # Crear e iniciar el DNS Registry
    dns = DNSRegistry(
        host=host,
        port=port, 
        scan_port=scan_port,
        # network_ranges=network_ranges
    )
    
    try:
        dns.start()
        
        # Mantener el programa corriendo
        while dns.running:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logging.info("Deteniendo DNS Registry...")
    finally:
        dns.stop()