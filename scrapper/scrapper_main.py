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
from node_connection import NodeConnection


# config del logging (igual que en el server)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class ScrapperNode2():
    def __init__(self, scrapper_port = 8080, bd_port = 9090, router_port = 7070) -> None:
        self.node_type = "scrapper"
        self.my_ip = socket.gethostbyname(socket.gethostname())
        self.my_id = f"{self.node_type}-{self.my_ip}:{scrapper_port}"
        
        # Rol y jefatura
        self.i_am_boss = False
        
        # Si NO soy jefe: solo necesito conexión a mi jefe
        self.boss_connection = None  # NodeConnection al jefe scrapper
        
        # Si SOY jefe: necesito múltiples conexiones
        self.subordinates = {}  # {node_id: NodeConnection} - scrappers subordinados
        self.bd_boss_connection = None  # NodeConnection al jefe de BD
        self.router_boss_connection = None  # NodeConnection al jefe de router
        
        # Puertos
        self.scrapper_port = scrapper_port
        self.bd_port = bd_port
        self.router_port = router_port
        
        # Estado del nodo
        self.is_busy = False
        self.status_lock = threading.Lock()
        self.current_task = {}
        
        # Control de ejecución
        self.running = False
        
        # Cache de IPs conocidas (nodos descubiertos o identificados)
        # Útil para elecciones futuras aunque no estén conectados
        self.known_nodes = {
            "scrapper": {},  # {ip: {"port": port, "last_seen": datetime, "is_boss": bool}}
            "bd": {},
            "router": {}
        }
        
        # Socket de escucha para conexiones entrantes
        self.listen_socket = None
        self.listen_thread = None
        
        # Hilo de monitoreo de heartbeats
        self.heartbeat_monitor_thread = None
        self.heartbeat_timeout = 90  # segundos sin heartbeat antes de considerar muerto
        self.heartbeat_check_interval = 30  # revisar cada 30 segundos
    
    def _handle_message_from_node(self, node_connection, message_dict):
        """
        Callback que se llama cuando se recibe un mensaje de cualquier nodo.
        
        Args:
            node_connection (NodeConnection): Conexión desde la que llegó el mensaje
            message_dict (dict): Mensaje recibido
        """
        msg_type = message_dict.get('type')
        
        if msg_type == 'heartbeat':
            logging.info(f"Heartbeat recibido de {node_connection.node_id}")
            # Ya se actualizó automáticamente en NodeConnection
            
        elif msg_type == 'task':
            self._handle_task_message(node_connection, message_dict)
            
        elif msg_type == 'identification':
            self._handle_identification(node_connection, message_dict)
            
        elif msg_type == 'status_update':
            node_connection.is_busy = message_dict.get('is_busy', False)
            logging.info(f"Estado actualizado para {node_connection.node_id}: busy={node_connection.is_busy}")
        
        # elif msg_type == 'election':
        #     # Mensaje de elección recibido
        #     self._handle_election_message(node_connection, message_dict)
        
        # elif msg_type == 'election_response':
        #     # Respuesta a elección (alguien con IP mayor está vivo)
        #     self._handle_election_response(node_connection, message_dict)
        
        # elif msg_type == 'new_boss':
        #     # Anuncio de nuevo jefe
        #     self._handle_new_boss_announcement(node_connection, message_dict)
            
        else:
            logging.warning(f"Tipo de mensaje desconocido de {node_connection.node_id}: {msg_type}")
    
    def connect_to_boss(self, boss_ip):
        """Conectar a mi jefe (cuando soy subordinado)"""
        if self.boss_connection and self.boss_connection.is_connected():
            logging.warning("Ya existe una conexión con el jefe")
            return True
        
        self.boss_connection = NodeConnection(
            "scrapper", 
            boss_ip, 
            self.scrapper_port,
            on_message_callback=self._handle_message_from_node
        )
        
        if self.boss_connection.connect():
            logging.info(f"Conectado exitosamente al jefe en {boss_ip}")
            
            # Enviar identificación PERSISTENTE (NO temporal)
            self.boss_connection.send_message({
                'type': 'identification',
                'node_type': self.node_type,
                'ip': self.my_ip,
                'port': self.scrapper_port,
                'is_boss': False,
                'is_temporary': False  # Marcar como persistente
            })
            
            # Iniciar envío periódico de heartbeats
            threading.Thread(
                target=self._heartbeat_loop,
                args=(self.boss_connection,),
                daemon=True
            ).start()
            
            return True
        else:
            logging.error(f"No se pudo conectar al jefe en {boss_ip}")
            self.boss_connection = None
            # Eliminar de known_nodes si no se pudo conectar
            self.remove_node_from_registry("scrapper", boss_ip)
            return False
    
    def add_subordinate(self, node_ip, existing_socket=None):
        """
        Agregar un subordinado (cuando soy jefe).
        
        Args:
            node_ip (str): IP del nodo subordinado
            existing_socket (socket.socket, optional): Socket ya conectado
        """
        node_id = f"scrapper-{node_ip}:{self.scrapper_port}"
        
        if node_id in self.subordinates:
            logging.warning(f"Subordinado {node_id} ya existe")
            return True
        
        conn = NodeConnection(
            "scrapper",
            node_ip,
            self.scrapper_port,
            on_message_callback=self._handle_message_from_node
        )
        
        if conn.connect(existing_socket=existing_socket):
            self.subordinates[node_id] = conn
            logging.info(f"Subordinado {node_id} agregado exitosamente")
            
            # Enviar identificación como jefe
            conn.send_message({
                'type': 'identification',
                'node_type': self.node_type,
                'ip': self.my_ip,
                'port': self.scrapper_port,
                'is_boss': True
            })
            
            # Iniciar heartbeats
            threading.Thread(
                target=self._heartbeat_loop,
                args=(conn,),
                daemon=True
            ).start()
            
            return True
        else:
            logging.error(f"No se pudo conectar con subordinado {node_ip}")
            # Eliminar de known_nodes si no se pudo conectar
            self.remove_node_from_registry("scrapper", node_ip)
            return False
    
    def _heartbeat_loop(self, node_connection):
        """Envía heartbeats periódicos a una conexión"""
        while self.running and node_connection.is_connected():
            node_connection.send_heartbeat({
                'my_id': self.my_id,
                'is_busy': self.is_busy
            })
            time.sleep(30)  # Heartbeat cada 30 segundos
    
    def send_temporary_message(self, target_ip, target_port, message_dict, 
                               expect_response=False, timeout=3.0, node_type=None):
        """
        Envía un mensaje temporal a un nodo sin mantener la conexión.
        Encapsula toda la lógica de: crear socket -> conectar -> enviar -> recibir (opcional) -> cerrar.
        
        Este método es útil para comunicación one-shot donde no necesitas mantener
        una conexión persistente. Maneja automáticamente el protocolo de longitud + mensaje,
        errores de conexión, timeouts y limpieza de recursos.
        
        Args:
            target_ip (str): IP del nodo destino
            target_port (int): Puerto del nodo destino
            message_dict (dict): Mensaje a enviar (será convertido a JSON)
            expect_response (bool): Si True, espera y retorna la respuesta
            timeout (float): Timeout para la conexión y recepción (en segundos)
            node_type (str, optional): Tipo de nodo ('scrapper', 'bd', 'router'). 
                                       Si se proporciona, el nodo se eliminará de known_nodes
                                       en caso de error de conexión.
        
        Returns:
            dict o bool:
                - Si expect_response=True: Retorna el mensaje de respuesta (dict) o None si falla
                - Si expect_response=False: Retorna True si se envió exitosamente, False si falla
        
        Notas:
            - El socket se cierra automáticamente al finalizar (éxito o error)
            - Los errores se logean como DEBUG para no saturar los logs
            - El protocolo usado es: 2 bytes (longitud) + mensaje JSON
            - Thread-safe: cada llamada usa su propio socket temporal
            - Si falla la conexión y node_type está especificado, el nodo se elimina de known_nodes
        """
        temp_sock = None
        try:
            # Crear y configurar socket
            temp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            temp_sock.settimeout(timeout)
            
            # Conectar
            temp_sock.connect((target_ip, target_port))
            
            # Serializar y enviar mensaje
            message_bytes = json.dumps(message_dict).encode()
            message_length = len(message_bytes)
            
            # Enviar longitud (2 bytes) + mensaje
            temp_sock.send(message_length.to_bytes(2, 'big'))
            temp_sock.send(message_bytes)
            
            logging.debug(f"Mensaje temporal enviado a {target_ip}:{target_port} - tipo: {message_dict.get('type', 'unknown')}")
            
            # Si se espera respuesta, recibirla
            if expect_response:
                # Recibir longitud de respuesta (2 bytes)
                length_bytes = temp_sock.recv(2)
                
                if not length_bytes:
                    logging.debug(f"No se recibió respuesta de {target_ip}:{target_port}")
                    return None
                
                response_length = int.from_bytes(length_bytes, 'big')
                
                # Recibir respuesta completa
                response_bytes = b''
                while len(response_bytes) < response_length:
                    chunk = temp_sock.recv(response_length - len(response_bytes))
                    if not chunk:
                        logging.debug(f"Conexión cerrada por {target_ip}:{target_port} durante recepción")
                        return None
                    response_bytes += chunk
                
                # Decodificar respuesta
                response_dict = json.loads(response_bytes.decode())
                logging.debug(f"Respuesta recibida de {target_ip}:{target_port} - tipo: {response_dict.get('type', 'unknown')}")
                
                return response_dict
            else:
                # No se espera respuesta, solo confirmar envío exitoso
                return True
        
        except socket.timeout:
            logging.debug(f"Timeout conectando/comunicando con {target_ip}:{target_port}")
            # Eliminar de known_nodes si se especificó node_type
            if node_type:
                self.remove_node_from_registry(node_type, target_ip)
            return None if expect_response else False
        
        except ConnectionRefusedError:
            logging.debug(f"Conexión rechazada por {target_ip}:{target_port}")
            # Eliminar de known_nodes si se especificó node_type
            if node_type:
                self.remove_node_from_registry(node_type, target_ip)
            return None if expect_response else False
        
        except OSError as e:
            # Incluye errores como "Network is unreachable", "No route to host", etc.
            logging.debug(f"Error de red con {target_ip}:{target_port}: {e}")
            # Eliminar de known_nodes si se especificó node_type
            if node_type:
                self.remove_node_from_registry(node_type, target_ip)
            return None if expect_response else False
        
        except Exception as e:
            logging.debug(f"Error en comunicación temporal con {target_ip}:{target_port}: {e}")
            # Eliminar de known_nodes si se especificó node_type (por cualquier error inesperado)
            if node_type:
                self.remove_node_from_registry(node_type, target_ip)
            return None if expect_response else False
        
        finally:
            # Siempre cerrar el socket
            if temp_sock:
                try:
                    temp_sock.close()
                except:
                    pass
    
    def _heartbeat_monitor_loop(self):
        """
        Hilo que monitorea los heartbeats de todas las conexiones.
        Si un nodo no ha enviado heartbeat en heartbeat_timeout segundos,
        se considera muerto y se desconecta.
        """
        logging.info(f"Iniciando monitor de heartbeats (timeout: {self.heartbeat_timeout}s, check interval: {self.heartbeat_check_interval}s)")
        
        while self.running:
            try:
                time.sleep(self.heartbeat_check_interval)
                
                # Limpiar nodos muertos
                self._cleanup_dead_nodes()
                
            except Exception as e:
                logging.error(f"Error en monitor de heartbeats: {e}")
    
    def _cleanup_dead_nodes(self):
        """
        Verifica todas las conexiones y elimina las que han dejado de enviar heartbeats
        o cuya conexión se ha cerrado.
        """
        dead_nodes = []
        
        # 1. Verificar jefe (si soy subordinado)
        if not self.i_am_boss and self.boss_connection:
            # Verificar si la conexión está cerrada
            if not self.boss_connection.is_connected():
                boss_ip = self.boss_connection.ip
                logging.warning(f"Jefe {self.boss_connection.node_id} desconectado (conexión cerrada)")
                logging.warning("Iniciando elecciones para encontrar nuevo jefe...")
                
                # Desconectar del jefe muerto
                self.boss_connection.disconnect()
                self.boss_connection = None
                
                # Eliminar de known_nodes
                self.remove_node_from_registry("scrapper", boss_ip)
                
                # Iniciar proceso de elección
                threading.Thread(target=self.call_elections, daemon=True).start()
            else:
                # La conexión está activa, verificar heartbeat
                time_since_heartbeat = self.boss_connection.get_time_since_last_heartbeat()
                
                if time_since_heartbeat is not None and time_since_heartbeat > self.heartbeat_timeout:
                    boss_ip = self.boss_connection.ip
                    logging.warning(f"Jefe {self.boss_connection.node_id} no responde (último heartbeat hace {time_since_heartbeat:.1f}s)")
                    logging.warning("Iniciando elecciones para encontrar nuevo jefe...")
                    
                    # Desconectar del jefe muerto
                    self.boss_connection.disconnect()
                    self.boss_connection = None
                    
                    # Eliminar de known_nodes
                    self.remove_node_from_registry("scrapper", boss_ip)
                    
                    # Iniciar proceso de elección
                    threading.Thread(target=self.call_elections, daemon=True).start()
        
        # 2. Verificar subordinados (si soy jefe)
        if self.i_am_boss and self.subordinates:
            for node_id, conn in list(self.subordinates.items()):
                # Primero verificar si la conexión está cerrada
                if not conn.is_connected():
                    logging.warning(f"Subordinado {node_id} desconectado (conexión cerrada)")
                    dead_nodes.append(node_id)
                    continue
                
                # La conexión está activa, verificar heartbeat
                time_since_heartbeat = conn.get_time_since_last_heartbeat()
                
                # Si nunca ha enviado heartbeat, darle más tiempo (puede estar iniciándose)
                if time_since_heartbeat is None:
                    continue
                
                if time_since_heartbeat > self.heartbeat_timeout:
                    logging.warning(f"Subordinado {node_id} no responde (último heartbeat hace {time_since_heartbeat:.1f}s)")
                    dead_nodes.append(node_id)
            
            # Eliminar subordinados muertos
            for node_id in dead_nodes:
                conn = self.subordinates[node_id]
                logging.info(f"Desconectando subordinado muerto: {node_id}")
                conn.disconnect()
                del self.subordinates[node_id]
                
                # También remover de known_nodes
                ip = conn.ip
                if ip in self.known_nodes.get("scrapper", {}):
                    del self.known_nodes["scrapper"][ip]
                    logging.info(f"Nodo {ip} eliminado de nodos conocidos")
            
            if dead_nodes:
                logging.info(f"Limpieza completada: {len(dead_nodes)} nodos eliminados")
                logging.info(f"Subordinados activos: {len(self.subordinates)}")
        
        # 3. Verificar conexiones con otros jefes (BD, Router)
        if self.bd_boss_connection:
            # Verificar si la conexión está cerrada
            if not self.bd_boss_connection.is_connected():
                bd_ip = self.bd_boss_connection.ip
                logging.warning(f"Jefe de BD desconectado (conexión cerrada)")
                self.bd_boss_connection.disconnect()
                self.bd_boss_connection = None
                # Eliminar de known_nodes
                self.remove_node_from_registry("bd", bd_ip)
                logging.info("Conexión con jefe de BD cerrada")
            else:
                # La conexión está activa, verificar heartbeat
                time_since_heartbeat = self.bd_boss_connection.get_time_since_last_heartbeat()
                
                if time_since_heartbeat is not None and time_since_heartbeat > self.heartbeat_timeout:
                    bd_ip = self.bd_boss_connection.ip
                    logging.warning(f"Jefe de BD no responde (último heartbeat hace {time_since_heartbeat:.1f}s)")
                    self.bd_boss_connection.disconnect()
                    self.bd_boss_connection = None
                    # Eliminar de known_nodes
                    self.remove_node_from_registry("bd", bd_ip)
                    logging.info("Conexión con jefe de BD cerrada")
        
        if self.router_boss_connection:
            # Verificar si la conexión está cerrada
            if not self.router_boss_connection.is_connected():
                router_ip = self.router_boss_connection.ip
                logging.warning(f"Jefe de Router desconectado (conexión cerrada)")
                self.router_boss_connection.disconnect()
                self.router_boss_connection = None
                # Eliminar de known_nodes
                self.remove_node_from_registry("router", router_ip)
                logging.info("Conexión con jefe de Router cerrada")
            else:
                # La conexión está activa, verificar heartbeat
                time_since_heartbeat = self.router_boss_connection.get_time_since_last_heartbeat()
                
                if time_since_heartbeat is not None and time_since_heartbeat > self.heartbeat_timeout:
                    router_ip = self.router_boss_connection.ip
                    logging.warning(f"Jefe de Router no responde (último heartbeat hace {time_since_heartbeat:.1f}s)")
                    self.router_boss_connection.disconnect()
                    self.router_boss_connection = None
                    # Eliminar de known_nodes
                    self.remove_node_from_registry("router", router_ip)
                    logging.info("Conexión con jefe de Router cerrada")
    
    def send_to_boss(self, message_dict):
        """Enviar mensaje a mi jefe"""
        if not self.boss_connection or not self.boss_connection.is_connected():
            logging.error("No hay conexión con el jefe")
            return False
        return self.boss_connection.send_message(message_dict)
    
    def broadcast_to_subordinates(self, message_dict):
        """Enviar mensaje a todos los subordinados. OJO: No hace broadcast real, solo envía individualmente"""
        if not self.i_am_boss:
            logging.warning("No soy jefe, no puedo hacer broadcast")
            return False
        
        success_count = 0
        for node_id, conn in self.subordinates.items():
            if conn.send_message(message_dict):
                success_count += 1
        
        logging.info(f"Broadcast enviado a {success_count}/{len(self.subordinates)} subordinados")
        return success_count > 0
    
    def _handle_task_message(self, node_connection, message_dict):
        """Procesa un mensaje de tarea"""
        task_id = message_dict.get("task_id")
        task_data = message_dict.get("task_data")
        
        if not task_id or not task_data:
            logging.error(f"Mensaje de tarea inválido: {message_dict}")
            return
        
        if self.is_busy:
            logging.warning(f"Ocupado, rechazando tarea {task_id}")
            node_connection.send_message({
                "type": "task_rejection",
                "task_id": task_id,
                "reason": "busy"
            })
            return
        
        # Aceptar tarea
        node_connection.send_message({
            "type": "task_accepted",
            "task_id": task_id
        })
        
        # Ejecutar en hilo separado
        threading.Thread(
            target=self._execute_task,
            args=(node_connection, task_id, task_data),
            daemon=True
        ).start()
    
    def _execute_task(self, node_connection, task_id, task_data):
        """Ejecuta una tarea de scraping"""
        self.update_busy_status(True)
        
        try:
            if not isinstance(task_data, dict) or 'url' not in task_data:
                raise Exception("Formato de tarea inválido")
            
            url = task_data['url']
            logging.info(f"Scraping: {url}")
            
            scrape_result = get_html_from_url(url)
            
            result = {
                'url': scrape_result['url'],
                'html_length': len(scrape_result['html']),
                'links_count': len(scrape_result['links']),
                'links': scrape_result['links'][:10],
                'status': 'success'
            }
            
        except Exception as e:
            logging.error(f"Error en scraping: {e}")
            result = {
                'status': 'error',
                'error': str(e)
            }
        
        # Enviar resultado
        node_connection.send_message({
            'type': 'task_result',
            'task_id': task_id,
            'result': result,
            'completed_at': datetime.now().isoformat()
        })
        
        self.update_busy_status(False)
    
    def _handle_identification(self, node_connection, message_dict):
        """Procesa mensaje de identificación"""
        node_ip = message_dict.get('ip')
        is_boss = message_dict.get('is_boss', False)
        
        logging.info(f"Identificación recibida de {node_ip} (boss={is_boss})")
        
        # Si soy jefe y un subordinado se identifica, ya lo tengo registrado
        # Si no soy jefe y el nodo es jefe, actualizar mi referencia
        if not self.i_am_boss and is_boss:
            self.boss_connection = node_connection
            logging.info(f"Jefe identificado: {node_ip}")
    
    def update_busy_status(self, is_busy):
        """Actualiza el estado de ocupado"""
        with self.status_lock:
            self.is_busy = is_busy
            logging.info(f"Estado actualizado a: {'ocupado' if is_busy else 'libre'}")
            
            # Notificar al jefe
            if self.boss_connection and self.boss_connection.is_connected():
                self.boss_connection.send_message({
                    'type': 'status_update',
                    'is_busy': self.is_busy
                })
        
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
        
            # Almacenar nodos descubiertos en known_nodes para uso posterior
            for ip in discovered_ips:
                if node_alias not in self.known_nodes:
                    self.known_nodes[node_alias] = {}
                
                # Solo actualizar si no existe o actualizar last_seen
                if ip not in self.known_nodes[node_alias]:
                    self.known_nodes[node_alias][ip] = {
                        "port": node_port,
                        "last_seen": datetime.now(),
                        "is_boss": False  # Por defecto, no sabemos si es jefe
                    }
                else:
                    # Actualizar last_seen si ya existe
                    self.known_nodes[node_alias][ip]["last_seen"] = datetime.now()
            
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
    
    def get_discovered_nodes(self, node_type=None):
        """
        Retorna nodos conocidos (descubiertos o identificados).
        
        Args:
            node_type (str, optional): Tipo de nodo ('scrapper', 'bd', 'router').
                                       Si es None, retorna todos.
        
        Returns:
            dict o list: Diccionario de nodos conocidos o lista de IPs
        """
        if node_type:
            return self.known_nodes.get(node_type, {})
        return self.known_nodes
    
    def remove_node_from_registry(self, node_type, ip):
        """
        Elimina un nodo del registro de nodos conocidos.
        Útil cuando un nodo se desconecta y no queremos mantenerlo en el registro.
        
        Args:
            node_type (str): Tipo de nodo ('scrapper', 'bd', 'router')
            ip (str): IP del nodo a eliminar
        
        Returns:
            bool: True si se eliminó, False si no existía
        """
        if node_type not in self.known_nodes:
            return False
        
        if ip in self.known_nodes[node_type]:
            del self.known_nodes[node_type][ip]
            logging.info(f"Nodo {node_type} {ip} eliminado del registro")
            return True
        
        return False
    
    def connect_to_discovered_nodes(self, node_type):
        """
        Conecta a todos los nodos conocidos de un tipo específico.
        Útil cuando soy jefe y necesito conectar con subordinados.
        
        Args:
            node_type (str): Tipo de nodo ('scrapper', 'bd', 'router')
        """
        discovered = self.known_nodes.get(node_type, {})
        
        if not discovered:
            logging.warning(f"No hay nodos {node_type} conocidos")
            return 0
        
        connected_count = 0
        
        for ip, info in discovered.items():
            if node_type == "scrapper":
                # Si es scrapper, usar add_subordinate
                if self.add_subordinate(ip):
                    connected_count += 1
            elif node_type == "bd":
                # Conectar al jefe de BD
                self.bd_boss_connection = NodeConnection(
                    "bd",
                    ip,
                    info["port"],
                    on_message_callback=self._handle_message_from_node
                )
                if self.bd_boss_connection.connect():
                    connected_count += 1
                    logging.info(f"Conectado al jefe de BD en {ip}")
                else:
                    self.bd_boss_connection = None
            elif node_type == "router":
                # Conectar al jefe de router
                self.router_boss_connection = NodeConnection(
                    "router",
                    ip,
                    info["port"],
                    on_message_callback=self._handle_message_from_node
                )
                if self.router_boss_connection.connect():
                    connected_count += 1
                    logging.info(f"Conectado al jefe de router en {ip}")
                else:
                    self.router_boss_connection = None
        
        logging.info(f"Conectado a {connected_count}/{len(discovered)} nodos {node_type}")
        return connected_count
    
    def start_listening(self):
        """
        Inicia el socket de escucha para recibir conexiones entrantes.
        Debe llamarse antes de broadcast_identification.
        """
        if self.listen_socket:
            logging.warning("Socket de escucha ya está activo")
            return
        
        try:
            self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.listen_socket.bind((self.my_ip, self.scrapper_port))
            self.listen_socket.listen(10)
            
            # Iniciar hilo de escucha
            self.listen_thread = threading.Thread(
                target=self._listen_for_connections,
                daemon=True
            )
            self.listen_thread.start()
            
            logging.info(f"Escuchando conexiones en {self.my_ip}:{self.scrapper_port}")
            
        except Exception as e:
            logging.error(f"Error iniciando socket de escucha: {e}")
            self.listen_socket = None
    
    def _listen_for_connections(self):
        """Hilo que escucha conexiones entrantes"""
        while self.running:
            try:
                self.listen_socket.settimeout(1.0)
                client_sock, client_addr = self.listen_socket.accept()
                logging.info(f"Conexión entrante desde {client_addr[0]}")
                
                # Procesar en hilo separado
                threading.Thread(
                    target=self._handle_incoming_connection,
                    args=(client_sock, client_addr),
                    daemon=True
                ).start()
                
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    logging.error(f"Error aceptando conexión: {e}")
    
    def _handle_incoming_connection(self, sock, addr):
        """
        Maneja una conexión entrante.
        
        Si soy JEFE:
            - Acepto subordinados, mantengo la conexión
        
        Si NO soy JEFE:
            - Solo registro la IP en cache, cierro la conexión
        """
        client_ip = addr[0]
        
        try:
            # Recibir mensaje de identificación
            sock.settimeout(5.0)
            
            # Recibir longitud
            length_bytes = sock.recv(2)
            if not length_bytes:
                sock.close()
                return
            
            message_length = int.from_bytes(length_bytes, 'big')
            
            # Recibir mensaje completo
            message_bytes = b''
            while len(message_bytes) < message_length:
                chunk = sock.recv(message_length - len(message_bytes))
                if not chunk:
                    break
                message_bytes += chunk
            
            message = json.loads(message_bytes.decode())
            msg_type = message.get('type')
            
            if msg_type == 'identification':
                self._handle_identification_incoming(sock, client_ip, message)
            elif msg_type == 'election':
                self._handle_election_incoming(sock, client_ip, message)
            elif msg_type == 'new_boss':
                self._handle_new_boss_incoming(sock, client_ip, message)
            else:
                logging.warning(f"Mensaje desconocido de {client_ip}: {msg_type}")
                sock.close()
                
        except socket.timeout:
            logging.warning(f"Timeout esperando mensaje de {client_ip}")
            sock.close()
        except Exception as e:
            logging.error(f"Error manejando conexión entrante de {client_ip}: {e}")
            sock.close()
    
    def _handle_identification_incoming(self, sock, client_ip, message):
        """Maneja mensaje de identificación entrante"""
        # Registrar nodo en cache de conocidos
        node_type = message.get('node_type', 'scrapper')
        node_port = message.get('port', self.scrapper_port)
        is_boss = message.get('is_boss', False)
        is_temporary = message.get('is_temporary', False)  # Nuevo flag
        
        if node_type not in self.known_nodes:
            self.known_nodes[node_type] = {}
        
        self.known_nodes[node_type][client_ip] = {
            "port": node_port,
            "last_seen": datetime.now(),
            "is_boss": is_boss
        }
        
        logging.info(f"Nodo {node_type} registrado en cache: {client_ip} (boss={is_boss}, temporary={is_temporary})")
        
        # Si es conexión temporal, solo responder y cerrar
        if is_temporary:
            logging.info(f"Conexión temporal de {client_ip}, respondiendo y cerrando...")
            
            # Enviar respuesta indicando si soy jefe
            my_id_msg = {
                'type': 'identification',
                'node_type': self.node_type,
                'ip': self.my_ip,
                'port': self.scrapper_port,
                'is_boss': self.i_am_boss
            }
            id_bytes = json.dumps(my_id_msg).encode()
            sock.send(len(id_bytes).to_bytes(2, 'big'))
            sock.send(id_bytes)
            
            # Cerrar socket temporal
            sock.close()
            logging.info(f"Conexión temporal con {client_ip} cerrada")
            return
        
        # Si llegamos aquí, es conexión PERSISTENTE
        # Decidir si mantener conexión
        if self.i_am_boss and node_type == "scrapper":
            # SOY JEFE: mantener conexión con subordinado
            
            # Verificar si ya existe este subordinado
            node_id = f"{node_type}-{client_ip}:{node_port}"
            if node_id in self.subordinates:
                logging.info(f"Subordinado {node_id} ya existe. Cerrando conexión duplicada.")
                sock.close()
                return
            
            logging.info(f"Soy jefe, agregando subordinado {client_ip}")
            
            # Enviar mi identificación como jefe
            my_id_msg = {
                'type': 'identification',
                'node_type': self.node_type,
                'ip': self.my_ip,
                'port': self.scrapper_port,
                'is_boss': self.i_am_boss
            }
            id_bytes = json.dumps(my_id_msg).encode()
            sock.send(len(id_bytes).to_bytes(2, 'big'))
            sock.send(id_bytes)
            
            # Crear NodeConnection con socket existente
            self.add_subordinate(client_ip, existing_socket=sock)
            
        # elif is_boss and node_type == "scrapper":
        #     logging.info(f"Conectando con jefe {client_ip}")
        #     self.i_am_boss = False
        #     self.connect_to_boss(client_ip)
            
        else:
            # NO SOY JEFE: solo registrar y cerrar
            logging.info(f"No soy jefe, solo registro {client_ip} y cierro conexión")
            sock.close()
    
    def _handle_election_incoming(self, sock, client_ip, message):
        """
        Maneja mensaje de elección entrante (algoritmo Bully).
        
        Regla: Solo respondo si mi IP es MAYOR que la del que pregunta.
        """
        # Asegurar q no tengo jefe
        if self.boss_connection: 
            self.boss_connection.disconnect()
            self.boss_connection = None
            
        sender_ip = message.get('ip', client_ip)
        
        logging.info(f"Mensaje de elección recibido de {sender_ip}")
        
        # Comparar IPs
        if self.my_ip > sender_ip:
            # Mi IP es mayor, respondo que estoy vivo (y puedo ser jefe)
            logging.info(f"Mi IP ({self.my_ip}) > IP del candidato ({sender_ip}). Respondiendo.")
            
            response_msg = {
                'type': 'election_response',
                'ip': self.my_ip,
                'port': self.scrapper_port
            }
            response_bytes = json.dumps(response_msg).encode()
            
            try:
                sock.send(len(response_bytes).to_bytes(2, 'big'))
                sock.send(response_bytes)
            except Exception as e:
                logging.error(f"Error enviando respuesta de elección: {e}")
            
            sock.close()
            
            # Importante: Ahora yo también debo iniciar elecciones
            # porque hay un nodo que cree que puede ser jefe
            logging.info("Iniciando mis propias elecciones por protocolo Bully")
            threading.Thread(target=self.call_elections, daemon=True).start()
            
        else:
            # Mi IP es menor, no respondo
            logging.info(f"Mi IP ({self.my_ip}) <= IP del candidato ({sender_ip}). No respondo.")
            sock.close()
    
    def _handle_election_message(self, node_connection, message_dict):
        """Maneja mensaje de elección vía NodeConnection existente"""
        sender_ip = message_dict.get('ip', node_connection.ip)
        
        if self.my_ip > sender_ip:
            logging.info(f"Elección vía conexión: Mi IP mayor, respondiendo")
            node_connection.send_message({
                'type': 'election_response',
                'ip': self.my_ip,
                'port': self.scrapper_port
            })
            # Iniciar propias elecciones
            threading.Thread(target=self.call_elections, daemon=True).start()
    
    def _handle_election_response(self, node_connection, message_dict):
        """
        Maneja respuesta de elección.
        Significa que hay un nodo con IP mayor vivo, así que no soy jefe.
        """
        responder_ip = message_dict.get('ip', node_connection.ip)
        logging.info(f"Respuesta de elección recibida de {responder_ip}. No soy jefe.")
    
    def _handle_new_boss_announcement(self, node_connection, message_dict):
        """
        Maneja anuncio de nuevo jefe vía conexión persistente.
        Este es un caso especial donde recibimos el anuncio a través de una conexión existente.
        """
        new_boss_ip = message_dict.get('ip')
        new_boss_port = message_dict.get('port', self.scrapper_port)
        
        logging.info(f"📢 Anuncio de nuevo jefe recibido vía conexión persistente: {new_boss_ip}")
        
        # Si ya soy jefe, esto es un conflicto - resolver por IP
        # NOTE:Este pedazo no se debe ejecutar nunca pues solo se llama a eleccion si el jefe se desconecto
        # Luego, este pedazo de codigo solo se ejecuta si no hay jefe conectado
        if self.i_am_boss:
            if new_boss_ip > self.my_ip:
                logging.warning(f"Recibí anuncio de jefe con IP mayor ({new_boss_ip} > {self.my_ip}). Cediendo jefatura.")
                self._handle_boss_change(new_boss_ip, new_boss_port)
            else:
                logging.warning(f"Recibí anuncio de jefe con IP menor ({new_boss_ip} < {self.my_ip}). Mantengo jefatura.")
                # Podría iniciar nueva elección para resolver conflicto
        else:
            # No soy jefe, procesar normalmente
            self._handle_boss_change(new_boss_ip, new_boss_port)
    
    def _handle_new_boss_incoming(self, sock, client_ip, message):
        """
        Maneja mensaje de nuevo jefe en conexión temporal entrante.
        """
        # Asegurarme de no tener jefe
        if self.boss_connection:
            self.boss_connection.disconnect()
            self.boss_connection = None

        new_boss_ip = message.get('ip', client_ip)
        new_boss_port = message.get('port', self.scrapper_port)
        
        logging.info(f"📢 Anuncio de nuevo jefe recibido: {new_boss_ip}:{new_boss_port}")
        
        # Cerrar socket temporal
        sock.close()
        
        # Procesar cambio de jefe
        if self.i_am_boss:
            if new_boss_ip > self.my_ip:
                logging.warning(f"Nuevo jefe con IP mayor ({new_boss_ip} > {self.my_ip}). Cediendo jefatura.")
                self._handle_boss_change(new_boss_ip, new_boss_port)
            else:
                logging.warning(f"Nuevo jefe con IP menor ({new_boss_ip} < {self.my_ip}). Ignoring.")
        else:
            self._handle_boss_change(new_boss_ip, new_boss_port)
    
    def _handle_boss_change(self, new_boss_ip, new_boss_port):
        """
        Maneja el cambio de jefe: desconecta del jefe anterior y conecta al nuevo.
        
        Args:
            new_boss_ip (str): IP del nuevo jefe
            new_boss_port (int): Puerto del nuevo jefe
        """
        # Si soy jefe, dejar de serlo
        # if self.i_am_boss:
        #     logging.info("Dejando de ser jefe...")
        #     self.i_am_boss = False
            
        #     # Desconectar todos los subordinados
        #     for node_id, conn in list(self.subordinates.items()):
        #         logging.info(f"Desconectando subordinado: {node_id}")
        #         conn.disconnect()
        #         del self.subordinates[node_id]
        
        # Desconectar del jefe anterior si existe
        if self.boss_connection:
            old_boss_ip = self.boss_connection.ip
            logging.info(f"Desconectando del jefe anterior: {old_boss_ip}")
            self.boss_connection.disconnect()
            self.boss_connection = None
        
        # Conectar al nuevo jefe
        if new_boss_ip != self.my_ip:
            logging.info(f"Conectando al nuevo jefe: {new_boss_ip}:{new_boss_port}")
            
            # Pequeña pausa para evitar condiciones de carrera
            time.sleep(1)
            
            if self.connect_to_boss(new_boss_ip):
                logging.info(f"✓ Conectado exitosamente al nuevo jefe {new_boss_ip}")
            else:
                logging.error(f"✗ Error conectando al nuevo jefe {new_boss_ip}")
                # Si falla, podríamos iniciar elecciones
                logging.info("Iniciando elecciones por fallo de conexión...")
                threading.Thread(target=self.call_elections, daemon=True).start()
    
    def call_elections(self):
        """
        Inicia proceso de elección usando algoritmo Bully.
        
        Algoritmo:
        1. Obtener nodos conocidos con IP > mi_ip
        2. Ordenarlos de mayor a menor (para ser más rápido)
        3. Enviar mensaje de elección a cada uno
        4. Si alguien responde, él podría ser el jefe
        5. Si nadie responde, me autoproclamo jefe
        """
        logging.info("=== INICIANDO ELECCIONES (Algoritmo Bully) ===")
        logging.info(f"Mi IP: {self.my_ip}")
        
        # Obtener nodos scrapper conocidos
        known_scrappers = self.known_nodes.get("scrapper", {})
        
        if not known_scrappers:
            logging.info("No hay otros nodos conocidos. Me autoproclamo jefe.")
            self._become_boss()
            return
        
        # Filtrar nodos con IP mayor que la mía
        higher_ip_nodes = []
        for ip, info in known_scrappers.items():
            if ip > self.my_ip:
                higher_ip_nodes.append((ip, info["port"]))
        
        if not higher_ip_nodes:
            logging.info(f"No hay nodos con IP mayor que {self.my_ip}. Me autoproclamo jefe.")
            self._become_boss()
            return
        
        # Ordenar de mayor a menor IP (para encontrar al jefe más rápido)
        higher_ip_nodes.sort(reverse=True)
        logging.info(f"Contactando nodos con IP mayor: {[ip for ip, _ in higher_ip_nodes]}")
        
        # Enviar mensaje de elección a cada uno (de mayor a menor)
        someone_responded = False
        
        for ip, port in higher_ip_nodes:
            election_msg = {
                'type': 'election',
                'ip': self.my_ip,
                'port': self.scrapper_port
            }
            
            logging.info(f"Mensaje de elección enviado a {ip}")
            response = self.send_temporary_message(ip, port, election_msg, 
                                                   expect_response=True, 
                                                   timeout=3.0, 
                                                   node_type="scrapper")
            
            if response and response.get('type') == 'election_response':
                # ¡Hay alguien con IP mayor vivo!
                logging.info(f"✓ Respuesta recibida de {ip}. Él será el jefe.")
                someone_responded = True
                break  # Salir, ya no soy jefe
        
        # Decidir resultado
        if someone_responded:
            logging.info("Hay un nodo con IP mayor vivo. NO soy jefe.")
            self.i_am_boss = False
            # Esperar a que el nuevo jefe haga broadcast de identificación
        else:
            logging.info("Nadie con IP mayor respondió. ME AUTOPROCLAMO JEFE.")
            self._become_boss()
    
    def _become_boss(self):
        """Me convierto en jefe y notifico a todos"""
        self.i_am_boss = True
        logging.info("🔶 SOY EL NUEVO JEFE")
        
        # Cerrar conexión con jefe anterior si existía
        if self.boss_connection:
            self.boss_connection.disconnect()
            self.boss_connection = None
        
        # Limpiar subordinados antiguos (por si acaso)
        old_subordinates = list(self.subordinates.keys())
        for node_id in old_subordinates:
            conn = self.subordinates[node_id]
            conn.disconnect()
            del self.subordinates[node_id]
            
        # #Esperar un tiempo para q todos los nodos procesen la desconexión del jefe anterior
        # time.sleep(2)
        
        logging.info("=== ENVIANDO ANUNCIO DE NUEVO JEFE ===")
        
        # Obtener todos los nodos conocidos
        all_known_ips = set()
        
        # Agregar nodos conocidos (ya incluye descubiertos e identificados)
        for ip in self.known_nodes.get("scrapper", {}).keys():
            if ip != self.my_ip:
                all_known_ips.add(ip)
        
        logging.info(f"Notificando a {len(all_known_ips)} nodos: {list(all_known_ips)}")
        
        # 1. Enviar mensaje "new_boss" a todos los nodos conocidos
        for ip in all_known_ips:
            port = self.known_nodes.get("scrapper", {}).get(ip, {}).get("port", self.scrapper_port)
            
            new_boss_msg = {
                'type': 'new_boss',
                'node_type': self.node_type,
                'ip': self.my_ip,
                'port': self.scrapper_port
            }
            
            if self.send_temporary_message(ip, port, new_boss_msg, 
                                           expect_response=False, 
                                           node_type="scrapper"):
                logging.info(f"✓ Anuncio 'new_boss' enviado a {ip}")
            else:
                logging.warning(f"✗ No se pudo enviar anuncio a {ip} (nodo eliminado de registro)")
        
        # 2. Esperar un momento para que los nodos procesen el mensaje
        logging.info("Esperando a que los nodos procesen el anuncio...")
        time.sleep(2)
        
        # 3. Establecer conexiones persistentes con todos los subordinados
        logging.info("=== ESTABLECIENDO CONEXIONES CON SUBORDINADOS ===")
        
        connected_count = 0
        for ip in all_known_ips:
            port = self.known_nodes.get("scrapper", {}).get(ip, {}).get("port", self.scrapper_port)
            
            # Intentar agregar como subordinado
            if self.add_subordinate(ip):
                connected_count += 1
                logging.info(f"✓ Subordinado {ip} conectado exitosamente")
            else:
                logging.warning(f"✗ No se pudo conectar con {ip}")
        
        logging.info(f"=== JEFATURA ESTABLECIDA: {connected_count}/{len(all_known_ips)} subordinados conectados ===")
   
    
    def broadcast_identification(self, node_type="scrapper"):
        """
        Envía identificación a todos los nodos conocidos de un tipo.
        Solo mantiene conexión con quien responda (el jefe).
        
        Flujo:
        1. Envía identificación a todos
        2. Todos lo registran en cache
        3. Solo el jefe responde
        4. Establece conexión persistente solo con el jefe
        
        Args:
            node_type (str): Tipo de nodo a contactar ('scrapper', 'bd', 'router')
        """
        discovered = self.known_nodes.get(node_type, {})
        
        if not discovered:
            logging.warning(f"No hay nodos {node_type} conocidos para contactar")
            return False
    
        boss_found = False
        
        for ip, info in discovered.items():
            if ip == self.my_ip:
                continue
            
            # Enviar identificación TEMPORAL (para descubrimiento solamente)
            identification = {
                'type': 'identification',
                'node_type': node_type,
                'node_port': self.scrapper_port,
                'is_boss': self.i_am_boss,
                'is_temporary': True  # Marcar como temporal
            }
            
            logging.info(f"Identificación enviada a {ip}")
            response = self.send_temporary_message(ip, info["port"], identification, 
                                                   expect_response=True, 
                                                   timeout=5.0, 
                                                   node_type=node_type)
            
            if response and response.get('type') == 'identification' and response.get('is_boss', False):
                # ¡Es el jefe!
                logging.info(f"¡Jefe encontrado en {ip}!")
                boss_found = True
                
                # Crear NUEVA conexión persistente (evita conflicto de sockets)
                logging.info(f"Estableciendo conexión persistente con jefe...")
                time.sleep(0.3)  # Pequeña pausa para que el jefe registre
                
                self.connect_to_boss(ip)
                
                # Iniciar heartbeats
                if self.boss_connection and self.boss_connection.is_connected():
                    threading.Thread(
                        target=self._heartbeat_loop,
                        args=(self.boss_connection,),
                        daemon=True
                    ).start()
                    logging.info("Conexión persistente establecida exitosamente")
                else:
                    logging.error(f"No se pudo establecer conexión persistente con {ip}")
                    boss_found = False
        
        if boss_found:
            logging.info("Conexión con jefe establecida exitosamente")
        else:
            logging.warning("No se encontró ningún jefe que respondiera")
        
        return boss_found
        
    def start(self):
        """
        Inicia el nodo scrapper con el flujo completo:
        1. Descubre otros scrappers
        2. Inicia socket de escucha
        3. Hace broadcast de identificación
        4. Establece conexión solo con el jefe
        """
        self.running = True
        logging.info(f"=== Iniciando ScrapperNode2 ===")
        logging.info(f"Mi IP: {self.my_ip}")
        logging.info(f"Mi ID: {self.my_id}")
        
        # 1. Descubrir otros nodos scrapper
        logging.info("Descubriendo nodos scrapper en la red...")
        discovered_ips = self.discover_nodes("scrapper", self.scrapper_port)
        
        if not discovered_ips:
            logging.info("No se encontraron otros scrappers. Asumiendo rol de jefe.")
            self.i_am_boss = True
        else:
            logging.info(f"Descubiertos {len(discovered_ips)} scrappers: {discovered_ips}")
        
        # 2. Iniciar socket de escucha
        logging.info("Iniciando socket de escucha...")
        self.start_listening()
        
        # 3. Iniciar monitor de heartbeats
        logging.info("Iniciando monitor de heartbeats...")
        self.heartbeat_monitor_thread = threading.Thread(
            target=self._heartbeat_monitor_loop,
            name="HeartbeatMonitor",
            daemon=True
        )
        self.heartbeat_monitor_thread.start()
        
        # 4. Broadcast de identificación (todos me registran, solo jefe responde)
        if discovered_ips:
            logging.info("Enviando identificación a todos los scrappers...")
            boss_found = self.broadcast_identification("scrapper")
            
            if not boss_found:
                logging.warning("No se encontró jefe activo. Iniciando elecciones...")
                self.call_elections()
        
        # 5. Comportamiento según rol
        if self.i_am_boss:
            logging.info("🔶 Soy el JEFE de scrappers")
            # TODO: Conectar con jefes de BD y Router si es necesario
            # self.discover_nodes("bd", self.bd_port)
            # self.connect_to_discovered_nodes("bd")
        else:
            logging.info(f"✓ Soy subordinado, conectado al jefe en {self.boss_connection.ip if self.boss_connection else 'desconocido'}")
        
        logging.info("=== ScrapperNode2 iniciado correctamente ===")
        
        # Mantener vivo
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Deteniendo nodo...")
            self.stop()
    
    def stop(self):
        """Detiene el nodo limpiamente"""
        self.running = False
        
        # Cerrar conexiones
        if self.boss_connection:
            self.boss_connection.disconnect()
        
        for node_id, conn in self.subordinates.items():
            conn.disconnect()
        
        if self.bd_boss_connection:
            self.bd_boss_connection.disconnect()
        
        if self.router_boss_connection:
            self.router_boss_connection.disconnect()
        
        # Cerrar socket de escucha
        if self.listen_socket:
            self.listen_socket.close()
        
        logging.info("Nodo detenido")


# ==================== EJEMPLO DE USO ====================
if __name__ == "__main__":
    # Puedes elegir qué versión usar:
    
    # OPCIÓN 1: Usar ScrapperNode2 (nueva arquitectura con NodeConnection)
    logging.info("=== Usando ScrapperNode2 (Nueva Arquitectura) ===")
    node = ScrapperNode2()
    
    try:
        node.start()
    except KeyboardInterrupt:
        logging.info("Deteniendo nodo...")
        node.stop()
    except Exception as e:
        logging.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
    
    # OPCIÓN 2: Usar ScrapperNode (versión anterior)
    # logging.info("=== Usando ScrapperNode (Arquitectura Anterior) ===")
    # scrapper = ScrapperNode()
    # try:
    #     scrapper.start()
    # except KeyboardInterrupt:
    #     logging.info("Deteniendo...")
    # except Exception as e:
    #     logging.error(f"Error: {e}")