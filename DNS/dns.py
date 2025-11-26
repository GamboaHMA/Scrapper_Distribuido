"""
DNS Module
"""
import socket
import json
import threading
import time
import ipaddress
import logging
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class DNSRegistry:
    def __init__(self, host='localhost', scan_port=8080, port=5353):
        self.host = host
        # self.port = port
        self.scan_port = scan_port
        self.services = {} # 'service_name': {'type': ..., 'id': ..., 'address': ..., 'port': ..., 'last_heartbeat': ...}
        self.lock = threading.Lock()
        self._id_lock = threading.Lock()  
        self._id_counter = 0
        
        self.running = False
        self.scan_interval = 30  # seconds
        self.heartbeat_timeout = 60  # seconds
        
        self.network_ranges = ["192.168.1.0/24", "10.0.0.0/24", "172.16.0.0/24"]
        
    def start(self):
        self.running = True
        threading.Thread(target=self._scan_network, daemon=True).start()
        threading.Thread(target=self._cleanup_services, daemon=True).start()
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
        network = ipaddress.ip_network(net_range)
        for ip in network.hosts():
            if not self.running:
                break
            try:
                with socket.create_connection((str(ip), self.scan_port), timeout=1) as sock:
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
                        threading.Thread(target=self._handle_service_connection, args=(service_name,), daemon=False).start()
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
        while self.running:
            time.sleep(self.heartbeat_timeout)
            now = datetime.now()
            with self.lock:
                to_remove = [name for name, info in self.services.items() if now - info['last_heartbeat'] > timedelta(seconds=self.heartbeat_timeout)]
                for name in to_remove:
                    self.services[name]['service_socket'].close()
                    del self.services[name]
                    logging.info("Removed inactive service: %s", name)
                    
        
        