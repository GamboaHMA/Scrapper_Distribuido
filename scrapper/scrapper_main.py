import socket
import json
import time
import threading
import random
import logging
import os
from datetime import datetime
import struct
import queue

# Importar funciones de scrapping desde el módulo scrapper
from scrapper import get_html_from_url


# config del logging (igual que en el server)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class ScrapperNode():
    def __init__(self, scrapper_port = 8080, bd_port = 9090, router_port = 7070) -> None:
        '''inicializa el scrapper node'''
        # Almacenamiento de información completa de nodos
        # Estructura: {ip: {"puerto": int, "ultimo_heartbeat": datetime, "estado": str}}
        self.scrapper_nodes = {}  # Estados: "libre", "ocupado", "desconectado"
        self.bd_nodes = {}        # Estados: "libre", "ocupado", "desconectado" 
        self.router_nodes = {}    # Estados: "libre", "ocupado", "desconectado"
        
        self.my_ip = None
        self.i_am_a_boss = False
        self.my_boss_ip = None
        
        self.running = False
        # puertos por defecto
        self.scrapper_port = scrapper_port
        self.bd_port = bd_port
        self.router_port = router_port
        
        # Lock para operaciones thread-safe
        self.nodes_lock = threading.Lock()

        
    # Mensajes de identificacion
    def _get_identification_msg(self):
        '''Retorna el mensaje de identificación actualizado'''
        return {
            "type": "identification",
            "rol": "scrapper",
            "ip": self.my_ip,
            "port": self.scrapper_port,
            "boss": self.i_am_a_boss
        }
    def _get_election_response_msg(self):
        '''Retorna el mensaje de respuesta de elección actualizado'''
        return {
            "type": "election_response"
        }
    def _get_heartbeat_msg(self):
        '''Retorna el mensaje de heartbeat actualizado'''
        return {
            "type": "heartbeat"
        }
    def _get_call_elections_msg(self):
        '''Retorna el mensaje de llamada a elecciones actualizado'''
        return {
            "type": "election"
        }
        
    def start(self):
        '''inicia el scrapper node'''
        self.running = True
        logging.info("Scrapper Node iniciado")
        #Pedir al dns (de docker) la ip de todos los scrapper
        self.my_ip = socket.gethostbyname(socket.gethostname())
        logging.info(f"Mi IP es: {self.my_ip}")
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((self.my_ip, self.scrapper_port))
        self.socket.listen(10)
        
        self.discover_nodes("scrapper", self.scrapper_port)
        
        # Iniciar hilo para escuchar conexiones entrantes
        threading.Thread(target=self.listen_for_connections, args=("scrapper",), daemon=True).start()
        
        # Avisar a todos los scrapper que estoy aqui (en hilos separados para no bloquear)
        for ip in self.scrapper_nodes.keys():
            if ip != self.my_ip:
                threading.Thread(
                    target=self.send_message_to, 
                    args=(ip, self.scrapper_nodes[ip]["puerto"], self._get_identification_msg()),
                    daemon=True
                ).start()
        
        self.scrapper_behavior()
    
    def boss_behavior(self):
        '''Comportamiento del nodo jefe'''
        while self.running and self.i_am_a_boss:
            # Mantener heartbeats con subordinados
            for ip in self.scrapper_nodes.keys():
                if ip != self.my_ip:
                    threading.Thread(target=self.heartbeat_to, args=(ip, self.scrapper_nodes[ip]["puerto"]), daemon=True).start()
            time.sleep(5)  # Intervalo entre heartbeats
    
    def scrapper_behavior(self):
        '''Comportamiento del nodo scrapper'''
        while self.running and not self.i_am_a_boss:
            if self.my_boss_ip is None:
                # Buscar jefe
                found = self.search_for_boss()
                if not found:
                    logging.info("No se encontró un jefe activo. Iniciando elecciones.")
                    self.call_elections()
            else:
                # Mantener heartbeats con el jefe
                threading.Thread(target=self.heartbeat_to, args=(self.my_boss_ip, self.scrapper_nodes[self.my_boss_ip]["puerto"]), daemon=True).start()
            time.sleep(10)  # Intervalo entre intentos de búsqueda de jefe o heartbeats
        
        if self.i_am_a_boss:
            self.boss_behavior()
                   
    def send_message_to(self, node_ip, node_port, message_dict, sock=None, should_close=True):
        '''Envía un mensaje a un nodo específico. Si se proporciona un socket, lo utiliza; de lo contrario, crea uno nuevo.
        
        Args:
            node_ip (str): IP del nodo destino.
            node_port (int): Puerto del nodo destino.
            message_dict (dict): Diccionario del mensaje a enviar.
            sock (socket.socket, optional): Socket existente para usar. Si es None, se crea uno nuevo.
            should_close (bool, optional): Indica si se debe cerrar el socket después de enviar el mensaje. Por defecto es True.
        '''
        if sock is None and (node_ip is None or node_port is None):
            logging.error("Error al enviar mensaje: No se proporcionó socket ni información de destino (IP/puerto).")
            return
        try:
            if sock is None:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)  # Timeout de 5 segundos para evitar bloqueos
                sock.connect((node_ip, node_port))
            message = json.dumps(message_dict).encode()
            length_ = len(message)
            sock.send(length_.to_bytes(2, 'big'))
            sock.send(message)
            if should_close:
                sock.close()
            logging.info(f"Mensaje enviado a {node_ip}:{node_port} - {message_dict}")
        except socket.timeout:
            logging.warning(f"Timeout al conectar con {node_ip}:{node_port} - El nodo podría no estar listo")
        except Exception as e:
            logging.error(f"Error enviando mensaje a {node_ip}:{node_port} - {e}")
            
    def listen_for_connections(self, node_type):
        '''Escucha conexiones entrantes de otros nodos de un tipo específico'''
        logging.info("Escuchando conexiones entrantes...")
        while self.running:
            try:
                client_sock, client_addr = self.socket.accept()
                logging.info(f"Conexión entrante desde {client_addr}")
                # Iniciar un hilo para manejar la conexión
                threading.Thread(target=self.handle_node_connection, args=(client_sock, node_type, client_addr[0])).start()
            except Exception as e:
                logging.error(f"Error aceptando conexión entrante: {e}")
    
    def discover_nodes(self, node_alias, node_port):
        """Descubre nodos utilizando el DNS interno de Docker.

        Args:
            node_alias (str): Alias del nodo a descubrir (ej. 'scrapper', 'bd', 'router').
            node_port (int): Puerto por defecto para los nodos descubiertos.

        Returns:
            list: Lista de IPs de nodos descubiertos.
        """
        try:
            # Resolver el alias que Docker maneja internamente
            result = socket.getaddrinfo(node_alias, None, socket.AF_INET)
            
            # Extraer todas las IPs únicas
            discovered_ips = []
            for addr_info in result:
                ip = addr_info[4][0]  # La IP está en la posición [4][0]
                if ip not in discovered_ips and ip != self.my_ip:
                    discovered_ips.append(ip)
        
            # Actualizar la estructura de nodos con thread safety
            with self.nodes_lock:
                for ip in discovered_ips:
                    # Inicializar o actualizar información del nodo (usar versión interna sin lock)
                    self._save_node_internal(node_alias, ip, {
                        "puerto": node_port,
                        "ultimo_heartbeat": None,
                        "estado": "desconectado"
                    })
            
            discovered_count = len([ip for ip in discovered_ips if ip != self.my_ip])
            logging.info(f"Nodos {node_alias} descubiertos: {discovered_count}")
            logging.info(f"Mi IP: {self.my_ip}")
            logging.info(f"IPs descubiertas: {[ip for ip in discovered_ips if ip != self.my_ip]}")
            
            return [ip for ip in discovered_ips if ip != self.my_ip]
            
        except socket.gaierror as e:
            logging.error(f"Error consultando DNS de Docker para {node_alias}: {e}")
            return []
        except Exception as e:
            logging.error(f"Error inesperado en descubrimiento de {node_alias}: {e}")
            return []

    def connect_to_nodes(self, node_type):
        """Intenta conectar a todos los nodos de un tipo específico.

        Args:
            node_type (str): Tipo de nodo al que se desea conectar ("scrapper", "bd", "router"). Se puede usar "all" para conectar a todos los tipos.
        """
        if node_type == "all":
            for nt in ["scrapper", "bd", "router"]:
                self.connect_to_nodes(nt)
            return
        target_nodes = self._get_dict_for_node_type(node_type)
        if target_nodes is None:
            logging.error(f"Tipo de nodo desconocido: {node_type}")
            return
        
        with self.nodes_lock:
            for ip, node_info in target_nodes.items():
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect((ip, node_info["puerto"]))
                    logging.info(f"Conectado a nodo {node_type} en {ip}:{node_info['puerto']}")
                    
                    # Actualizar estado a libre una vez conectado
                    node_info["estado"] = "libre"
                    node_info["ultimo_heartbeat"] = datetime.now()
                    
                    # iniciar un hilo para manejar la comunicación con este nodo
                    threading.Thread(target=self.handle_node_connection, args=(sock, node_type, ip)).start()
                except Exception as e:
                    logging.error(f"No se pudo conectar al nodo {node_type} en {ip}:{node_info['puerto']} - {e}")
                    # Marcar como desconectado si falla la conexión
                    node_info["estado"] = "desconectado"
                
    def handle_node_connection(self, sock, node_type, node_ip):
        '''Maneja la comunicación con un nodo conectado. Mantiene un socket abierto para recibir mensajes y procesa dichos mensajes.'''
        logging.info(f"Manejando conexión con nodo {node_type} - IP: {node_ip}")
        try:
            while True:
                # recibir el tamaño del mensaje
                length_bytes = sock.recv(2)
                if not length_bytes:
                    logging.info(f"Conexión cerrada por el nodo {node_type} - {node_ip}")
                    break
                message_length = int.from_bytes(length_bytes, 'big')
                
                # recibir el mensaje completo
                message_bytes = b''
                while len(message_bytes) < message_length:
                    chunk = sock.recv(message_length - len(message_bytes))
                    if not chunk:
                        logging.info(f"Conexión cerrada por el nodo {node_type} - {node_ip}")
                        return
                    message_bytes += chunk
                
                message = json.loads(message_bytes.decode())
                logging.info(f"Mensaje recibido del nodo {node_type} - {node_ip}: {message}")
                
                # procesar el mensaje según su tipo
                # switcher : [funcion, args...]
                switcher = {
                    'heartbeat': [self.update_node_heartbeat, node_type, node_ip],
                    'identification': [self._process_identification_message, message, node_type, node_ip, sock],
                    'status_update': [self.update_node_status, node_type, node_ip, message.get('estado', 'libre')],
                    'election': [self._process_election_message, node_type, node_ip],
                    'election_response': [self._process_election_response_message, node_type, node_ip],
                    'boss_query': [self._process_boss_query_message, node_type, node_ip, sock],
                }
                func = switcher.get(message['type'], None)
                if func:
                    most_break = func[0](*func[1:])
                    if most_break:
                        sock.close()
                        return
                else:
                    logging.warning(f"Tipo de mensaje desconocido del nodo {node_type} - {node_ip}: {message['type']}")
                # if message['type'] == 'heartbeat':
                #     self.update_node_heartbeat(node_type, node_ip)
                #     logging.info(f"Heartbeat recibido del nodo {node_type} - {node_ip}")
                # elif message['type'] == 'identification':
                #     #Si soy jefe, almacenar info del nodo y mantener conexion
                #     if self.i_am_a_boss:
                #         info = {
                #             "puerto": message.get("port", None),
                #             "ultimo_heartbeat": datetime.now(),
                #             "estado": "libre"
                #         }
                #         self._save_node(node_type, node_ip, info)
                #         logging.info(f"Nodo {node_type} - {node_ip} identificado y almacenado.")
                #         # Como boss, debo responder con mi identificacion
                #         self.send_message_to(node_ip, message.get("port", None), self._get_identification_msg())
                #     else:
                #         #Si no soy jefe, pero el nodo se identifica como jefe, almacenar su IP y mantener conexion
                #         if message.get("boss", False):
                #             self.my_boss_ip = node_ip
                #             info = {
                #                 "puerto": message.get("port", None),
                #                 "ultimo_heartbeat": datetime.now(),
                #                 "estado": "libre"
                #             }
                #             self._save_node(node_type, node_ip, info)
                #             logging.info(f"Nodo {node_type} - {node_ip} es jefe. Almacenando su IP como jefe.")
                #             #Mantener heartbeats con el jefe
                #             threading.Thread(target=self.maintain_heartbeat_to, args=(self.my_boss_ip, message.get("port", None)), daemon=True).start()
                #         else:
                #             #Si no soy jefe y el nodo tampoco, cerrar conexion
                #             #Aun asi guardo su info en caso de posibles elecciones futuras
                #             info = {
                #                 "puerto": message.get("port", None),
                #                 "ultimo_heartbeat": None,
                #                 "estado": "desconectado"
                #             }
                #             self._save_node(node_type, node_ip, info)
                #             logging.info(f"Cerrando conexión con nodo {node_type} - {node_ip} ya que ninguno es jefe")
                #             sock.close()
                #             return
                    
                # elif message['type'] == 'status_update':
                #     self.update_node_status(node_type, node_ip, message.get('estado', 'libre'))
                # elif message['type'] == 'election':
                #     if self.i_am_a_boss:
                #         logging.info(f"Recibido mensaje de elección del nodo {node_type} - {node_ip}.")
                #         self.send_message_to(node_ip, message.get("port", None), self._get_election_msg())
                #     else:
                #         logging.info(f"Recibido mensaje de elección del nodo {node_type} - {node_ip}, pero no soy jefe. Ignorando.")
                # elif message['type'] == 'boss_query':
                #     if self.i_am_a_boss:
                #         logging.info(f"Recibido consulta de jefe del nodo {node_type} - {node_ip}. Respondiendo que soy jefe.")
                #         response_msg = {
                #             "type": "boss_response",
                #             "boss_ip": self.my_ip
                #         }
                #     elif not self.i_am_a_boss and self.my_boss_ip is not None:
                #         logging.info(f"Recibido consulta de jefe del nodo {node_type} - {node_ip}. Respondiendo con la IP del jefe.")
                #         response_msg = {
                #             "type": "boss_response",
                #             "boss_ip": self.my_boss_ip
                #         }
                #     #Se debe responder en el mismo socket
                #     response_bytes = json.dumps(response_msg).encode()
                #     length_ = len(response_bytes)
                #     sock.send(length_.to_bytes(2, 'big'))
                #     sock.send(response_bytes)
                #     sock.close()
                #     return
                    
                
        except Exception as e:
            logging.error(f"Error en la comunicación con el nodo {node_type} - {node_ip}: {e}")
        finally:
            # Marcar nodo como desconectado al cerrar conexión
            self.update_node_status(node_type, node_ip, "desconectado")
            sock.close()
            logging.info(f"Conexión con el nodo {node_type} - {node_ip} cerrada")
    
    def _process_boss_query_message(self, node_type, node_ip, sock):
        '''Procesa un mensaje de consulta de jefe recibido de un nodo'''
        if self.i_am_a_boss:
            logging.info(f"Recibido consulta de jefe del nodo {node_type} - {node_ip}. Respondiendo que soy jefe.")
            response_msg = {
                "type": "boss_response",
                "boss_ip": self.my_ip
            }
        elif not self.i_am_a_boss and self.my_boss_ip is not None:
            logging.info(f"Recibido consulta de jefe del nodo {node_type} - {node_ip}. Respondiendo con la IP del jefe.")
            response_msg = {
                "type": "boss_response",
                "boss_ip": self.my_boss_ip
            }
        else:
            logging.info(f"Recibido consulta de jefe del nodo {node_type} - {node_ip}, pero no conozco un jefe activo.")
            response_msg = {
                "type": "boss_response",
                "boss_ip": None
            }
        #Se debe responder en el mismo socket
        self.send_message_to(node_ip, response_msg.get("port", None), response_msg, sock=sock, should_close=False)
        # try:
        #     response_bytes = json.dumps(response_msg).encode()
        #     length_ = len(response_bytes)
        #     sock.send(length_.to_bytes(2, 'big'))
        #     sock.send(response_bytes)
        #     time.sleep(0.1)  # Pequeña pausa para asegurar el envío antes de cerrar
        # except Exception as e:
        #     logging.error(f"Error enviando respuesta de jefe al nodo {node_type} - {node_ip}: {e}")
        
        return True  # Indicar que se debe cerrar la conexión
    
    def _process_election_message(self, node_type, node_ip):
        '''Procesa un mensaje de elección recibido de un nodo'''
        if self.i_am_a_boss:
            logging.info(f"Recibido mensaje de elección del nodo {node_type} - {node_ip}.")
            self.send_message_to(node_ip, self.scrapper_nodes[node_ip]["puerto"], self._get_election_response_msg())
        else:
            logging.info(f"Recibido mensaje de elección del nodo {node_type} - {node_ip}, pero no soy jefe. Ignorando.")
        return False
    
    def _process_election_response_message(self, node_type, node_ip):
        '''Procesa un mensaje de respuesta de elección recibido de un nodo'''
        logging.info(f"Recibido respuesta de elección del nodo {node_type} - {node_ip}. Estableciendo a {node_type} - {node_ip} como jefe.")
        self.i_am_a_boss = False
        self.my_boss_ip = node_ip
        return False
    
    def _process_identification_message(self, message, node_type, node_ip, sock):
        '''Procesa un mensaje de identificación recibido de un nodo'''
        if self.i_am_a_boss:
            info = {
                "puerto": message.get("port", None),
                "socket": sock,
                "ultimo_heartbeat": datetime.now(),
                "estado": "libre"
            }
            self._save_node(node_type, node_ip, info)
            logging.info(f"Nodo {node_type} - {node_ip} identificado y almacenado.")
            # Como boss, debo responder con mi identificacion
            self.send_message_to(None, None, self._get_identification_msg(), sock, should_close=False)
            # Mantener heartbeats con el nodo identificado
            threading.Thread(target=self.maintain_heartbeat_to, args=(node_ip, message.get("port", None)), daemon=True).start()
        else:
            #Si no soy jefe, pero el nodo se identifica como jefe, almacenar su IP y mantener conexion
            if message.get("boss", False):
                self.my_boss_ip = node_ip
                info = {
                    "puerto": message.get("port", None),
                    "socket": sock,
                    "ultimo_heartbeat": datetime.now(),
                    "estado": "libre"
                }
                self._save_node(node_type, node_ip, info)
                logging.info(f"Nodo {node_type} - {node_ip} es jefe. Almacenando su IP como jefe.")
                #Mantener heartbeats con el jefe
                threading.Thread(target=self.maintain_heartbeat_to, args=(self.my_boss_ip, message.get("port", None)), daemon=True).start()
            else:
                #Si no soy jefe y el nodo tampoco, cerrar conexion
                #Aun asi guardo su info en caso de posibles elecciones futuras
                info = {
                    "puerto": message.get("port", None),
                    "ultimo_heartbeat": None,
                    "estado": "desconectado"
                }
                self._save_node(node_type, node_ip, info)
                logging.info(f"Cerrando conexión con nodo {node_type} - {node_ip} ya que ninguno es jefe")
                return True  # Indicar que se debe cerrar la conexión
        return False
    
    def _get_dict_for_node_type(self, node_type):
        '''Retorna el diccionario correspondiente al tipo de nodo'''
        if node_type == "scrapper":
            return self.scrapper_nodes
        elif node_type == "bd":
            return self.bd_nodes
        elif node_type == "router":
            return self.router_nodes
        else:
            return None
            
    def _save_node_internal(self, node_type, node_ip, node_info):
        '''Guarda la información de un nodo (sin adquirir lock - para uso interno)'''
        if node_type == "scrapper":
            self.scrapper_nodes[node_ip] = node_info
        elif node_type == "bd":
            self.bd_nodes[node_ip] = node_info
        elif node_type == "router":
            self.router_nodes[node_ip] = node_info
    
    def _save_node(self, node_type, node_ip, node_info):
        '''Guarda la información de un nodo en el diccionario correspondiente (con lock)'''
        with self.nodes_lock:
            self._save_node_internal(node_type, node_ip, node_info)

    def maintain_heartbeat_to(self, node_ip, node_port):
        '''Mantiene un hilo de heartbeats periódicos a un nodo específico'''
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect((node_ip, node_port))
        while self.running:
            try:
                message = json.dumps(self._get_heartbeat_msg()).encode()
                length_ = len(message)
                sock.send(length_.to_bytes(2, 'big'))
                sock.send(message)
            except Exception as e:
                logging.error(f"Error enviando heartbeat al nodo {node_ip}: {e}")
            time.sleep(10)  # enviar heartbeat cada 10 segundos
        sock.close()
    
    def update_node_heartbeat(self, node_type, node_ip):
        '''Actualiza el último heartbeat de un nodo'''
        with self.nodes_lock:
            if node_type == "scrapper" and node_ip in self.scrapper_nodes:
                self.scrapper_nodes[node_ip]["ultimo_heartbeat"] = datetime.now()
            elif node_type == "bd" and node_ip in self.bd_nodes:
                self.bd_nodes[node_ip]["ultimo_heartbeat"] = datetime.now()
            elif node_type == "router" and node_ip in self.router_nodes:
                self.router_nodes[node_ip]["ultimo_heartbeat"] = datetime.now()
        return False
    
    def update_node_status(self, node_type, node_ip, nuevo_estado):
        '''Actualiza el estado de un nodo (libre, ocupado, desconectado)'''
        with self.nodes_lock:
            if node_type == "scrapper" and node_ip in self.scrapper_nodes:
                self.scrapper_nodes[node_ip]["estado"] = nuevo_estado
                logging.info(f"Nodo scrapper {node_ip} - Estado actualizado a: {nuevo_estado}")
            elif node_type == "bd" and node_ip in self.bd_nodes:
                self.bd_nodes[node_ip]["estado"] = nuevo_estado
                logging.info(f"Nodo BD {node_ip} - Estado actualizado a: {nuevo_estado}")
            elif node_type == "router" and node_ip in self.router_nodes:
                self.router_nodes[node_ip]["estado"] = nuevo_estado
                logging.info(f"Nodo router {node_ip} - Estado actualizado a: {nuevo_estado}")
    
    def get_available_nodes(self, node_type):
        '''Retorna lista de nodos disponibles (libres) de un tipo específico'''
        available_nodes = []
        with self.nodes_lock:
            if node_type == "scrapper":
                target_nodes = self.scrapper_nodes
            elif node_type == "bd":
                target_nodes = self.bd_nodes
            elif node_type == "router":
                target_nodes = self.router_nodes
            else:
                return []
            
            for ip, node_info in target_nodes.items():
                if node_info["estado"] == "libre":
                    available_nodes.append({
                        "ip": ip,
                        "puerto": node_info["puerto"],
                        "ultimo_heartbeat": node_info["ultimo_heartbeat"]
                    })
        
        return available_nodes
    
    def get_node_info(self, node_type, node_ip):
        '''Retorna información completa de un nodo específico'''
        with self.nodes_lock:
            if node_type == "scrapper" and node_ip in self.scrapper_nodes:
                return self.scrapper_nodes[node_ip].copy()
            elif node_type == "bd" and node_ip in self.bd_nodes:
                return self.bd_nodes[node_ip].copy()
            elif node_type == "router" and node_ip in self.router_nodes:
                return self.router_nodes[node_ip].copy()
        return None
    
    def monitor_heartbeats(self):
        '''Monitorea los heartbeats de los nodos y actualiza su estado si no responden'''
        while True:
            time.sleep(10)  # revisar cada 10 segundos
            now = datetime.now()
            with self.nodes_lock:
                #TODO: REVISAR, aqui no van todos los nodos bd ni router, van solo los jefes respectivos
                for node_dict in [self.scrapper_nodes, self.bd_nodes, self.router_nodes]:
                    for ip, info in node_dict.items():
                        estado = info["estado"]
                        if estado == "desconectado":
                            continue
                        ultimo_heartbeat = info["ultimo_heartbeat"]
                        if ultimo_heartbeat and (now - ultimo_heartbeat).total_seconds() > 30:
                            info["estado"] = "desconectado"
                            logging.warning(f"Nodo {ip} marcado como desconectado por falta de heartbeat")
                            if ip == self.my_boss_ip:
                                logging.warning("El jefe ha sido desconectado.")
                                self.my_boss_ip = None

    def search_for_boss(self):
        '''Busca un jefe activo entre los nodos scrapper conocidos'''
        # Copiar lista de nodos para no mantener lock durante operaciones de red
        with self.nodes_lock:
            nodes_to_check = [(ip, info.copy()) for ip, info in self.scrapper_nodes.items()]
        
        # Ahora buscar sin el lock
        for ip, info in nodes_to_check:
            if info["estado"] == "desconectado":
                continue
            # Intentar conectar y verificar si es jefe
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)  # Timeout de 5 segundos
                sock.connect((ip, info["puerto"]))
                
                # Enviar mensaje de consulta de jefe
                query_msg = {
                    "type": "boss_query"
                }
                self.send_message_to(ip, info["puerto"], query_msg, sock=sock, should_close=False)
                
                # Esperar respuesta
                length_bytes = sock.recv(2)
                if not length_bytes:
                    continue
                message_length = int.from_bytes(length_bytes, 'big')
                
                message_bytes = b''
                while len(message_bytes) < message_length:
                    chunk = sock.recv(message_length - len(message_bytes))
                    if not chunk:
                        break
                    message_bytes += chunk
                
                response = json.loads(message_bytes.decode())
                if response.get("type") == "boss_response" and response.get("boss_ip", None) is not None:
                    self.i_am_a_boss = False
                    self.my_boss_ip = response["boss_ip"]
                    logging.info(f"Jefe encontrado: {self.my_boss_ip}")
                    sock.close()
                    return True
                
                sock.close()
            except Exception as e:
                logging.error(f"Error al consultar nodo {ip} para jefe: {e}")
        return False
    
    def call_elections(self):
        '''Inicia el proceso de elecciones para convertirse en jefe'''
        # Implementar algoritmo de elección usando Bully
        logging.info("Iniciando proceso de elecciones para elegir un jefe...")
        self.i_am_a_boss = False
        
        # Obtener nodos con IP mayor sin mantener el lock
        with self.nodes_lock:
            higher_ip_nodes = [(ip, self.scrapper_nodes[ip]["puerto"]) 
                              for ip in self.scrapper_nodes.keys() 
                              if ip > socket.gethostbyname(socket.gethostname())]
        
        if not higher_ip_nodes:
            # No hay nodos con IP mayor, me convierto en jefe
            self.i_am_a_boss = True
            self.my_boss_ip = socket.gethostbyname(socket.gethostname())
            logging.info("Este nodo se ha convertido en el jefe.")
        else:
            # Enviar mensajes de elección a nodos con IP mayor (sin el lock)
            for ip, port in higher_ip_nodes:
                try:
                    self.send_message_to(ip, port, self._get_call_elections_msg(), should_close=True)
                except Exception as e:
                    logging.error(f"Error al enviar mensaje de elección a {ip}: {e}")
    
    def heartbeat_to(self, node_ip, node_port):
        '''Envía heartbeats periódicos a un nodo específico'''
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect((node_ip, node_port))
        
        while self.running:
            try:
                self.send_message_to(node_ip, node_port, self._get_heartbeat_msg(), sock=sock, should_close=False)
                # heartbeat_msg = {
                #     "type": "heartbeat"
                # }
                # message = json.dumps(heartbeat_msg).encode()
                # length_ = len(message)
                # sock.send(length_.to_bytes(2, 'big'))
                # sock.send(message)
                
            except Exception as e:
                logging.error(f"Error enviando heartbeat al nodo {node_ip}: {e}")
            time.sleep(10)  # enviar heartbeat cada 10 segundos
        sock.close()
            
if __name__ == "__main__":
    # Crear instancia del scrapper node
    scrapper = ScrapperNode()
    
    try:
        # Iniciar el nodo scrapper
        logging.info("=== INICIANDO SCRAPPER NODE ===")
        scrapper.start()
        
        # Mantener el programa corriendo
        logging.info("Scrapper node funcionando. Presiona Ctrl+C para salir.")
        
        # Iniciar monitor de heartbeats en un hilo separado
        monitor_thread = threading.Thread(target=scrapper.monitor_heartbeats, daemon=True)
        monitor_thread.start()
        
        # Loop principal - mantener vivo el proceso
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logging.info("Deteniendo scrapper node...")
    except Exception as e:
        logging.error(f"Error inesperado: {e}")
    finally:
        logging.info("Scrapper node detenido.")