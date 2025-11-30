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
        
        self.i_am_a_boss = False
        self.my_boss_ip = None
        
        self.running = False
        # puertos por defecto
        self.scrapper_port = scrapper_port
        self.bd_port = bd_port
        self.router_port = router_port
        
        # Lock para operaciones thread-safe
        self.nodes_lock = threading.Lock()
        
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((socket.gethostbyname(socket.gethostname()), self.scrapper_port))
        self.socket.listen(10)
        
    def start(self):
        '''inicia el scrapper node'''
        self.running = True
        logging.info("Scrapper Node iniciado")
        #Pedir al dns (de docker) la ip de todos los scrapper
        self.discover_scrapper_nodes()
        #self.discover_bd_nodes()
        #self.discover_router_nodes()
        
        # Conectarse a nodos conocidos  
        self.connect_to_nodes("scrapper")
        threading.Thread(target=self.monitor_heartbeats, daemon=True).start()
        
        # Iniciar hilo para escuchar conexiones entrantes
        threading.Thread(target=self.listen_for_connections, args=("scrapper",), daemon=True).start()
        while self.running:
            if self.i_am_a_boss:
                # EJECUTAR TAREAS DEL JEFE
                # Mantener heartbeats con subordinados
                for ip in self.scrapper_nodes.keys():
                    if ip != socket.gethostbyname(socket.gethostname()):
                        threading.Thread(target=self.heartbeat_to, args=(ip, self.scrapper_nodes[ip]["puerto"]), daemon=True).start()
                # TODO: Buscar router y bd nodes. Estos deben ser los jefes de sus respectivos grupos.
                # Mantener hilo de escucha con el router para recibir tareas de scrapping
                # Y con la bd pero solo para heartbeats y actualizaciones de estado
                # Si se reciben tareas de scrapping, asignarlas a nodos scrapper libres (randomly o round-robin)
                pass
            elif self.my_boss_ip is None:
                found_boss = self.search_for_boss()
                if not found_boss:
                    logging.info("No se encontró un jefe activo. Llamando a elecciones.")
                    # Iniciar proceso de elecciones para convertirse en jefe
                    # (Implementar algoritmo de elección aquí)
                    self.call_elections()
            else:
                # Mantener un hilo de comunicación con el jefe para enviar heartbeats y recibir actualizaciones
                threading.Thread(target=self.heartbeat_to, args=(self.my_boss_ip, self.scrapper_nodes[self.my_boss_ip]["puerto"]), daemon=True).start()
                    
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

    def discover_scrapper_nodes(self):
        """Descubre nodos scrapper utilizando el DNS interno de Docker.

        Returns:
            list: Lista de IPs de nodos scrapper descubiertos.
        """
        try:
            # Resolver el alias 'scrapper' que Docker maneja internamente
            result = socket.getaddrinfo('scrapper', None, socket.AF_INET)
            
            # Extraer todas las IPs únicas
            discovered_ips = []
            for addr_info in result:
                ip = addr_info[4][0]  # La IP está en la posición [4][0]
                if ip not in discovered_ips:
                    discovered_ips.append(ip)
            
            # Obtener nuestra propia IP para excluirla
            our_ip = socket.gethostbyname(socket.gethostname())
            
            # Actualizar la estructura de nodos scrapper con thread safety
            with self.nodes_lock:
                for ip in discovered_ips:
                    if ip != our_ip:  # Excluir nuestra propia IP
                        # Inicializar o actualizar información del nodo
                        if ip not in self.scrapper_nodes:
                            self.scrapper_nodes[ip] = {
                                "puerto": self.scrapper_port,
                                "ultimo_heartbeat": None,
                                "estado": "desconectado"
                            }
                        # Si ya existe, mantener el último heartbeat y estado actual
            
            discovered_count = len([ip for ip in discovered_ips if ip != our_ip])
            logging.info(f"Nodos scrapper descubiertos: {discovered_count}")
            logging.info(f"Nuestra IP: {our_ip}")
            logging.info(f"IPs descubiertas: {list(self.scrapper_nodes.keys())}")
            
            return list(self.scrapper_nodes.keys())
            
        except socket.gaierror as e:
            logging.error(f"Error consultando DNS de Docker: {e}")
            return []
        except Exception as e:
            logging.error(f"Error inesperado en descubrimiento: {e}")
            return []

    def connect_to_nodes(self, node_type):
        """Intenta conectar a todos los nodos de un tipo específico.

        Args:
            node_type (str): Tipo de nodo al que se desea conectar ("scrapper", "bd", "router"). Se puede usar "all" para conectar a todos los tipos.
        """
        if node_type == "scrapper":
            target_nodes = self.scrapper_nodes
        elif node_type == "bd":
            target_nodes = self.bd_nodes
        elif node_type == "router":
            target_nodes = self.router_nodes
        elif node_type == "all":
            for nt in ["scrapper", "bd", "router"]:
                self.connect_to_nodes(nt)
            return
        else:
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
                if message['type'] == 'heartbeat':
                    self.update_node_heartbeat(node_type, node_ip)
                    logging.info(f"Heartbeat recibido del nodo {node_type} - {node_ip}")
                elif message['type'] == 'status_update':
                    self.update_node_status(node_type, node_ip, message.get('estado', 'libre'))
                elif message['type'] == 'election' and self.i_am_a_boss:
                    # Responder que ya hay un jefe
                    response_msg = {
                        "type": "election_response",
                        "is_boss": True
                    }
                    response_bytes = json.dumps(response_msg).encode()
                    length_ = len(response_bytes)
                    sock.send(length_.to_bytes(2, 'big'))
                    sock.send(response_bytes)
                elif message['type'] == 'boss_query':
                    response_msg = {
                        "type": "boss_response",
                        "is_boss": self.i_am_a_boss
                    }
                    response_bytes = json.dumps(response_msg).encode()
                    length_ = len(response_bytes)
                    sock.send(length_.to_bytes(2, 'big'))
                    sock.send(response_bytes)
                    
                
        except Exception as e:
            logging.error(f"Error en la comunicación con el nodo {node_type} - {node_ip}: {e}")
        finally:
            # Marcar nodo como desconectado al cerrar conexión
            self.update_node_status(node_type, node_ip, "desconectado")
            sock.close()
            logging.info(f"Conexión con el nodo {node_type} - {node_ip} cerrada")
    
    def update_node_heartbeat(self, node_type, node_ip):
        '''Actualiza el último heartbeat de un nodo'''
        with self.nodes_lock:
            if node_type == "scrapper" and node_ip in self.scrapper_nodes:
                self.scrapper_nodes[node_ip]["ultimo_heartbeat"] = datetime.now()
            elif node_type == "bd" and node_ip in self.bd_nodes:
                self.bd_nodes[node_ip]["ultimo_heartbeat"] = datetime.now()
            elif node_type == "router" and node_ip in self.router_nodes:
                self.router_nodes[node_ip]["ultimo_heartbeat"] = datetime.now()
    
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
        with self.nodes_lock:
            for ip, info in self.scrapper_nodes.items():
                if info["estado"] != "desconectado":
                    # Intentar conectar y verificar si es jefe
                    try:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.settimeout(5)  # Timeout de 5 segundos
                        sock.connect((ip, info["puerto"]))
                        
                        # Enviar mensaje de consulta de jefe
                        query_msg = {
                            "type": "boss_query"
                        }
                        message = json.dumps(query_msg).encode()
                        length_ = len(message)
                        sock.send(length_.to_bytes(2, 'big'))
                        sock.send(message)
                        
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
                        if response.get("is_boss", False):
                            self.my_boss_ip = ip
                            logging.info(f"Jefe encontrado en {ip}")
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
        with self.nodes_lock:
            higher_ip_nodes = [ip for ip in self.scrapper_nodes.keys() if ip > socket.gethostbyname(socket.gethostname())]
            if not higher_ip_nodes:
                # No hay nodos con IP mayor, me convierto en jefe
                self.i_am_a_boss = True
                self.my_boss_ip = socket.gethostbyname(socket.gethostname())
                logging.info("Este nodo se ha convertido en el jefe.")
            else:
                # Enviar mensajes de elección a nodos con IP mayor
                for ip in higher_ip_nodes:
                    try:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.settimeout(5)
                        sock.connect((ip, self.scrapper_nodes[ip]["puerto"]))
                        
                        election_msg = {
                            "type": "election"
                        }
                        message = json.dumps(election_msg).encode()
                        length_ = len(message)
                        sock.send(length_.to_bytes(2, 'big'))
                        sock.send(message)
                        
                        sock.close()
                    except Exception as e:
                        logging.error(f"Error al enviar mensaje de elección a {ip}: {e}")
    
    def heartbeat_to(self, node_ip, node_port):
        '''Envía heartbeats periódicos a un nodo específico'''
        while self.running:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                sock.connect((node_ip, node_port))
                
                heartbeat_msg = {
                    "type": "heartbeat"
                }
                message = json.dumps(heartbeat_msg).encode()
                length_ = len(message)
                sock.send(length_.to_bytes(2, 'big'))
                sock.send(message)
                
                sock.close()
            except Exception as e:
                logging.error(f"Error enviando heartbeat al nodo {node_ip}: {e}")
            time.sleep(10)  # enviar heartbeat cada 10 segundos
            
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