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
import sys

# Agregar el directorio padre al path para imports absolutos
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from base_node.utils import NodeConnection, MessageProtocol, BossProfile

PORTS = {
    'scrapper': 8080,
    'bd': 9090,
    'router': 7070
}

# Por defecto INFO, pero se puede cambiar con LOG_LEVEL=DEBUG
log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class Node:
    def __init__(self, node_type):
        self.node_type = node_type
        self.ip = socket.gethostbyname(socket.gethostname())
        self.port = PORTS.get(self.node_type)
        self.node_id = f"{self.node_type}-{self.ip}:{self.port}"
        
        self.i_am_boss = False
        self.my_boss_profile = BossProfile(self.node_type, self.port)
        
        self.subordinates = {}

        self.running = True
        
        # Cache de IPs conocidas (nodos descubiertos o identificados)
        # Útil para elecciones futuras aunque no estén conectados
        self.nodes_cache = {
            "scrapper": {},  # {ip: {"port": port, "last_seen": datetime, "is_boss": bool}}
            "bd": {},
            "router": {}
        }
        
        # Información de jefes externos replicada (para subordinados)
        # {node_type: {'ip': ip, 'port': port}}
        self.external_bosses_cache = {}
        
        # Conexiones persistentes con jefes de otros tipos (cuando soy jefe)
        # {node_id: NodeConnection} donde node_id = f"{node_type}-{ip}:{port}"
        self.bosses_connections = {}
        
        self.listen_socket = None
        self.listen_thread = None
        
        # Hilo de monitoreo de heartbeats
        self.heartbeat_monitor_thread = None
        self.heartbeat_timeout = 90  # segundos sin heartbeat antes de considerar muerto
        self.heartbeat_check_interval = 30  # revisar cada 30 segundos
        
        self.persistent_message_handler = {
            MessageProtocol.MESSAGE_TYPES['HEARTBEAT']: self._handle_heartbeat,
            MessageProtocol.MESSAGE_TYPES['IDENTIFICATION']: self._handle_identification,
            MessageProtocol.MESSAGE_TYPES['STATUS_UPDATE']: self._handle_status_update,
            MessageProtocol.MESSAGE_TYPES['EXTERNAL_BOSSES_INFO']: self._handle_external_bosses_info,
            # Agregar más manejadores según los tipos de mensaje necesarios
        }
        self.temporary_message_handler = {
            MessageProtocol.MESSAGE_TYPES['IDENTIFICATION']: self._handle_identification_incoming,
            MessageProtocol.MESSAGE_TYPES['ELECTION']: self._handle_election_message,
            MessageProtocol.MESSAGE_TYPES['NEW_BOSS']: self._handle_new_boss_message,
            MessageProtocol.MESSAGE_TYPES['NEW_EXTERNAL_BOSS']: self._handle_new_external_boss,
            # Agregar más manejadores temporales según sea necesario
        }
    
    @property
    def boss_connection(self):
        """Propiedad para mantener compatibilidad con código existente"""
        return self.my_boss_profile.connection
    
    @boss_connection.setter
    def boss_connection(self, value):
        """Setter para mantener compatibilidad con código existente"""
        if value is None:
            self.my_boss_profile.clear_connection()
        else:
            self.my_boss_profile.set_connection(value)

    def _create_message(self, msg_type, data=None):
        """
        Helper para crear mensajes usando MessageProtocol.
        
        Args:
            msg_type (str): Tipo de mensaje (usar MessageProtocol.MESSAGE_TYPES)
            data (dict, optional): Datos adicionales del mensaje
        
        Returns:
            dict: Mensaje estructurado listo para enviar
        """
        message_json = MessageProtocol.create_message(
            msg_type=msg_type,
            sender_id=self.node_id,
            node_type=self.node_type,
            data=data
        )
        return json.loads(message_json)  # Retornar como dict para NodeConnection
    
    def _handle_message_from_node(self, node_connection, message_dict):
        """
        Maneja mensajes recibidos de otros nodos.
        
        Args:
            node_connection (NodeConnection): Conexión del nodo que envió el mensaje
            message_dict (dict): Mensaje recibido parseado
        """
        msg_type = message_dict.get('type')
        sender_id = message_dict.get('sender_id')
        data = message_dict.get('data', {})
        
        logging.debug(f"Mensaje recibido de {sender_id}: {msg_type}")
        
        handler = self.persistent_message_handler.get(msg_type)
        if handler:
            handler(node_connection, message_dict)
        else:
            logging.warning(f"No hay manejador para el tipo de mensaje: {msg_type} de {sender_id}")
            
    def _handle_heartbeat(self, node_connection, message_dict):
        """Procesa mensaje de heartbeat"""
        # ya el NodeConnection maneja el update del heartbeat
        pass
        
    def _handle_identification(self, node_connection, message_dict):
        """Procesa mensaje de identificación"""
        data = message_dict.get('data', {})
        node_ip = data.get('ip')
        # node_port = data.get('port', self.port)
        is_boss = data.get('is_boss', False)
        
        # Obtener el tipo de nodo del remitente
        sender_node_type = message_dict.get('node_type', self.node_type)
        
        # Registrar en known_nodes usando el tipo del remitente
        if node_ip:
            if sender_node_type not in self.nodes_cache:
                self.nodes_cache[sender_node_type] = {}
            
            self.nodes_cache[sender_node_type][node_ip] = {
                "port": data.get('port', self.port),
                "last_seen": datetime.now(),
                "is_boss": is_boss
            }
        
        # Si soy jefe y un subordinado se identifica, ya lo tengo registrado
        # Si no soy jefe y el nodo es jefe, actualizar mi referencia
        if not self.i_am_boss and is_boss:
            self.my_boss_profile.set_connection(node_connection)
            logging.info(f"Jefe {sender_node_type} identificado: {node_ip}")
        else:
            logging.info(f"Identificación recibida de {sender_node_type} {node_ip} (boss={is_boss})")
    
    def _handle_status_update(self, node_connection, message_dict):
        data = message_dict.get('data', {})
        node_connection.is_busy = data.get('is_busy', False)
        logging.info(f"Actualización de estado de {node_connection.node_id}: is_busy={node_connection.is_busy}")
    
    def _handle_external_bosses_info(self, node_connection, message_dict):
        """
        Handler para recibir información de jefes externos desde mi jefe.
        
        Args:
            node_connection: Conexión con mi jefe
            message_dict: Mensaje completo con info de jefes externos
        """
        data = message_dict.get('data', {})
        bosses_info = data.get('bosses', {})
        
        # Actualizar información de jefes externos
        for node_type, info in bosses_info.items():
            self.external_bosses_cache[node_type] = {
                'ip': info.get('ip'),
                'port': info.get('port')
            }
            logging.info(f"Información de jefe externo recibida: {node_type} en {info.get('ip')}:{info.get('port')}")
        
        logging.debug(f"Información de jefes externos actualizada: {len(bosses_info)} jefes")
    
    def replicate_external_bosses_info(self):
        """
        Replica información de jefes externos a todos los subordinados.
        Solo el jefe ejecuta este método.
        """
        if not self.i_am_boss:
            return
        
        logging.debug(f"Replicando info de jefes externos. Cache actual: {self.external_bosses_cache}")
        
        # Usar el cache directamente (ya tiene la info correcta)
        bosses_info = {}
        
        for node_type, info in self.external_bosses_cache.items():
            bosses_info[node_type] = {
                'ip': info.get('ip'),
                'port': info.get('port')
            }
        
        if not bosses_info:
            logging.debug("No hay jefes externos para replicar")
            return
        
        # Crear mensaje de replicación
        message = self._create_message(
            MessageProtocol.MESSAGE_TYPES['EXTERNAL_BOSSES_INFO'],
            {'bosses': bosses_info}
        )
        
        # Enviar a todos los subordinados
        for node_id, conn in self.subordinates.items():
            conn.send_message(message)
        
        logging.info(f"Información de {len(bosses_info)} jefes externos replicada a {len(self.subordinates)} subordinados")
    
    def _connect_to_external_bosses(self):
        """
        Notificar a jefes externos conocidos que soy el nuevo jefe.
        Usa la información replicada en external_bosses_cache.
        Envía mensajes temporales para que ambos jefes establezcan conexiones persistentes.
        """
        if not self.external_bosses_cache:
            logging.debug("No hay jefes externos conocidos para conectar")
            return
        
        logging.info(f"=== NOTIFICANDO A JEFES EXTERNOS: {list(self.external_bosses_cache.keys())} ===")
        
        for node_type, info in self.external_bosses_cache.items():
            boss_ip = info.get('ip')
            boss_port = info.get('port')
            
            if not boss_ip or not boss_port:
                logging.warning(f"Información incompleta para jefe externo {node_type}")
                continue
            
            logging.info(f"Notificando a jefe externo {node_type} en {boss_ip}:{boss_port}")
            
            # Enviar mensaje temporal NEW_EXTERNAL_BOSS
            new_boss_msg = self._create_message(
                MessageProtocol.MESSAGE_TYPES['NEW_EXTERNAL_BOSS'],
                {
                    'ip': self.ip,
                    'port': self.port,
                    'node_type': self.node_type
                }
            )
            
            if self.send_temporary_message(boss_ip, boss_port, new_boss_msg, 
                                          expect_response=False, 
                                          node_type=node_type):
                logging.info(f"✓ Notificación enviada a jefe externo {node_type}")
            else:
                logging.error(f"✗ No se pudo notificar a jefe externo {node_type}")
    
    def _handle_new_external_boss(self, sock, client_ip, message):
        """
        Handler temporal para recibir notificación de nuevo jefe externo.
        Crea una conexión persistente con el nuevo jefe.
        
        Args:
            sock: Socket temporal
            client_ip: IP del nuevo jefe
            message: Mensaje con info del nuevo jefe
        """
        data = message.get('data', {})
        new_boss_ip = data.get('ip')
        new_boss_port = data.get('port')
        new_boss_type = data.get('node_type')
        
        if not all([new_boss_ip, new_boss_port, new_boss_type]):
            logging.warning("Mensaje NEW_EXTERNAL_BOSS con datos incompletos")
            return
        
        logging.info(f"Nuevo jefe externo {new_boss_type} notificado: {new_boss_ip}:{new_boss_port}")
        
        # Cerrar conexión antigua con ese tipo si existe
        if new_boss_type in self.bosses_connections:
            old_conn = self.bosses_connections[new_boss_type]
            logging.info(f"Cerrando conexión antigua con jefe {new_boss_type}")
            old_conn.disconnect()
            del self.bosses_connections[new_boss_type]
        
        # Actualizar cache
        self.external_bosses_cache[new_boss_type] = {
            'ip': new_boss_ip,
            'port': new_boss_port
        }
        
        # Crear nueva conexión persistente
        conn = NodeConnection(
            new_boss_type,
            new_boss_ip,
            new_boss_port,
            on_message_callback=self._handle_message_from_node,
            sender_node_type=self.node_type,
            sender_id=self.node_id
        )
        
        if conn.connect():
            self.bosses_connections[new_boss_type] = conn
            
            # Enviar identificación
            conn.send_message(
                self._create_message(
                    MessageProtocol.MESSAGE_TYPES['IDENTIFICATION'],
                    {
                        'ip': self.ip,
                        'port': self.port,
                        'is_boss': True
                    }
                )
            )
            
            # Iniciar heartbeats
            threading.Thread(
                target=self._heartbeat_loop,
                args=(conn,),
                daemon=True
            ).start()
            
            # Replicar info a subordinados
            self.replicate_external_bosses_info()
            
            logging.info(f"Conexión persistente con nuevo jefe externo {new_boss_type} establecida")
        else:
            logging.error(f"No se pudo establecer conexión con nuevo jefe externo {new_boss_type}")
    
    def _handle_identification_incoming(self, sock, client_ip, message):
        """
        Handler para identificaciones entrantes (conexiones temporales Y persistentes).
        
        - Si is_temporary=True: Responde si es jefe y cierra (broadcast_identification)
        - Si is_temporary=False: Agrega como subordinado y mantiene conexión (connect_to_boss)
        """
        data = message.get('data', {})
        is_boss = data.get('is_boss', False)
        # node_port = data.get('node_port', self.port)
        is_temporary = data.get('is_temporary', False)
        
        # Obtener el tipo de nodo del remitente (del mensaje raíz)
        sender_node_type = message.get('node_type', self.node_type)
        
        # Registrar el nodo en known_nodes usando el tipo del remitente
        if sender_node_type not in self.nodes_cache:
            self.nodes_cache[sender_node_type] = {}
        
        self.nodes_cache[sender_node_type][client_ip] = {
            "port": self.port,
            "last_seen": datetime.now(),
            "is_boss": is_boss
        }
        
        logging.debug(f"Nodo {sender_node_type} {client_ip} registrado (boss={is_boss}, temporary={is_temporary})")
        
        # CASO 1: Conexión temporal (broadcast_identification)
        if is_temporary:
            # Solo responder si soy jefe
            if self.i_am_boss:
                response = self._create_message(
                    MessageProtocol.MESSAGE_TYPES['IDENTIFICATION'],
                    {
                        'node_port': self.port,
                        'is_boss': True,
                        'is_temporary': True
                    }
                )
                
                try:
                    # Enviar respuesta
                    response_bytes = json.dumps(response).encode()
                    sock.sendall(len(response_bytes).to_bytes(2, 'big'))
                    sock.sendall(response_bytes)
                    logging.debug(f"Respuesta de jefe enviada a {client_ip}")
                except Exception as e:
                    logging.error(f"Error enviando respuesta a {client_ip}: {e}")
            
            # Cerrar socket temporal
            sock.close()
        
        # CASO 2: Conexión persistente (subordinado o jefe externo conectándose)
        else:
            if self.i_am_boss and not is_boss:
                # Es un subordinado (no jefe) conectándose a mí (el jefe)
                
                # Verificar si es del mismo tipo (subordinado) o de otro tipo
                if sender_node_type == self.node_type:
                    # Es un subordinado de mi mismo tipo → ACEPTAR
                    logging.info(f"Subordinado {sender_node_type} {client_ip} estableciendo conexión persistente")
                    
                    # Agregar como subordinado usando el socket ya conectado
                    success = self.add_subordinate(client_ip, existing_socket=sock)
                    
                    if not success:
                        logging.error(f"No se pudo agregar subordinado {client_ip}")
                        sock.close()
                else:
                    # Es un subordinado de otro tipo → NO ME INTERESA, RECHAZAR
                    logging.debug(f"Subordinado {sender_node_type} {client_ip} intentó conectar (no me interesa, solo jefes externos)")
                    sock.close()
            
            elif self.i_am_boss and is_boss:
                # Es un JEFE de otro tipo conectándose a mí (también jefe)
                
                if sender_node_type != self.node_type:
                    # Es un jefe de otro tipo (ej: Router jefe → Scrapper jefe) → ACEPTAR
                    logging.info(f"Jefe externo {sender_node_type} {client_ip} estableciendo conexión persistente")
                    
                    # Obtener el puerto del mensaje
                    sender_port = data.get('port', self.port)
                    
                    # Agregar como cliente externo persistente
                    success = self.add_external_client(client_ip, sender_node_type, sender_port, existing_socket=sock)
                    
                    if not success:
                        logging.error(f"No se pudo agregar jefe externo {sender_node_type} {client_ip}")
                        sock.close()
                else:
                    # Es otro jefe de mi mismo tipo (caso raro, ambos son jefes) → RECHAZAR
                    logging.debug(f"Otro jefe {sender_node_type} {client_ip} intentó conectar (ambos somos jefes)")
                    sock.close()
            
            else:
                # No soy jefe, no debería recibir conexiones persistentes
                logging.debug(f"Cerrando conexión persistente de {sender_node_type} {client_ip} (no soy jefe)")
                sock.close()
    
    def _handle_election_message(self, sock, client_ip, message):
        """
        Handler para mensajes de elección (algoritmo Bully).
        Responde siempre (indica que estoy vivo y tengo IP mayor).
        """
        data = message.get('data', {})
        requester_ip = data.get('ip')
        
        logging.info(f"Mensaje de elección recibido de {requester_ip}")
        
        # Responder que estoy vivo (tengo IP mayor)
        response = self._create_message(
            MessageProtocol.MESSAGE_TYPES['ELECTION_RESPONSE'],
            {
                'ip': self.ip,
                'port': self.port
            }
        )
        
        try:
            response_bytes = json.dumps(response).encode()
            sock.send(len(response_bytes).to_bytes(2, 'big'))
            sock.send(response_bytes)
            logging.info(f"Respuesta de elección enviada a {requester_ip}")
        except Exception as e:
            logging.error(f"Error respondiendo elección a {requester_ip}: {e}")
        
        sock.close()
        
        # Iniciar mis propias elecciones (por si acaso)
        threading.Thread(target=self.call_elections, daemon=True).start()
    
    def _handle_new_boss_message(self, sock, client_ip, message):
        """
        Handler para anuncios de nuevo jefe.
        Actualiza la referencia al jefe y desconecta del anterior.
        """
        data = message.get('data', {})
        new_boss_ip = data.get('ip')
        new_boss_port = data.get('port', self.port)
        
        logging.info(f"🔶 Anuncio de nuevo jefe recibido: {new_boss_ip}")
        
        # Si yo era el jefe, dejo de serlo
        if self.i_am_boss and new_boss_ip != self.ip:
            logging.info("Ya no soy jefe, cediendo jefatura...")
            self.i_am_boss = False
            
            # Desconectar subordinados
            for node_id, conn in list(self.subordinates.items()):
                conn.disconnect()
            self.subordinates.clear()
        
        # Desconectar del jefe anterior si existía
        if self.my_boss_profile.connection:
            old_boss_ip = self.my_boss_profile.connection.ip
            if old_boss_ip != new_boss_ip:
                logging.info(f"Desconectando del jefe anterior {old_boss_ip}")
                self.my_boss_profile.connection.disconnect()
                self.my_boss_profile.clear_connection()
        
        # Actualizar known_nodes
        if self.node_type not in self.nodes_cache:
            self.nodes_cache[self.node_type] = {}
        
        self.nodes_cache[self.node_type][new_boss_ip] = {
            "port": new_boss_port,
            "last_seen": datetime.now(),
            "is_boss": True
        }
        
        sock.close()
        
        # Conectar al nuevo jefe (después de un pequeño delay)
        if new_boss_ip != self.ip:
            time.sleep(1)
            logging.info(f"Conectando al nuevo jefe en {new_boss_ip}...")
            self.connect_to_boss(new_boss_ip)
        
    def connect_to_boss(self, boss_ip):
        """Conectar a mi jefe (cuando soy subordinado)"""
        if self.my_boss_profile.is_connected():
            logging.warning("Ya existe una conexión con el jefe")
            return True
        
        new_connection = NodeConnection(
            self.node_type, 
            boss_ip, 
            self.port,
            on_message_callback=self._handle_message_from_node,
            sender_node_type=self.node_type,
            sender_id=self.node_id
        )
        
        if new_connection.connect():
            logging.info(f"Conectado al jefe en {boss_ip}")
            
            # Enviar identificación PERSISTENTE (NO temporal)
            new_connection.send_message(
                self._create_message(
                    MessageProtocol.MESSAGE_TYPES['IDENTIFICATION'],
                    data={
                        'ip': self.ip,
                        'port': self.port,
                        'is_boss': False,
                        'is_temporary': False
                    }
                )
            )
            
            # Establecer conexión en el perfil
            self.my_boss_profile.set_connection(new_connection)
            
            # Iniciar envío periódico de heartbeats
            threading.Thread(
                target=self._heartbeat_loop,
                args=(new_connection,),
                daemon=True
            ).start()
            
            return True
        else:
            logging.error(f"No se pudo conectar al jefe en {boss_ip}")
            self.my_boss_profile.clear_connection()
            # Eliminar de known_nodes si no se pudo conectar
            self.remove_node_from_registry(self.node_type, boss_ip)
            return False
        
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
        if node_type not in self.nodes_cache:
            return False
        
        if ip in self.nodes_cache[node_type]:
            del self.nodes_cache[node_type][ip]
            logging.info(f"Nodo {node_type} {ip} eliminado del registro")
            return True
        
        return False
    
    def _heartbeat_loop(self, node_connection):
        """Envía heartbeats periódicos a una conexión"""
        while self.running and node_connection.is_connected():
            node_connection.send_heartbeat()
            time.sleep(30)  # Heartbeat cada 30 segundos
            
    def add_subordinate(self, node_ip, existing_socket=None):
        """
        Agregar un subordinado (cuando soy jefe).
        
        Args:
            node_ip (str): IP del nodo subordinado
            existing_socket (socket.socket, optional): Socket ya conectado
        """
        # Validar que no se agregue a sí mismo
        if node_ip == self.ip:
            logging.warning(f"Intento de agregar a sí mismo como subordinado ({node_ip}), ignorando")
            if existing_socket:
                existing_socket.close()
            return False
        
        node_id = f"{self.node_type}-{node_ip}:{self.port}"
        
        if node_id in self.subordinates:
            logging.warning(f"Subordinado {node_id} ya existe")
            return True
        
        conn = NodeConnection(
            self.node_type,
            node_ip,
            self.port,
            on_message_callback=self._handle_message_from_node,
            sender_node_type=self.node_type,
            sender_id=self.node_id
        )
        
        if conn.connect(existing_socket=existing_socket):
            self.subordinates[node_id] = conn
            logging.info(f"Subordinado {node_ip} agregado")
            
            # Enviar identificación como jefe
            conn.send_message(
                self._create_message(
                    MessageProtocol.MESSAGE_TYPES['IDENTIFICATION'],
                    data={
                        'ip': self.ip,
                        'port': self.port,
                        'is_boss': True
                    }
                )
            )
            
            # Iniciar heartbeats
            threading.Thread(
                target=self._heartbeat_loop,
                args=(conn,),
                daemon=True
            ).start()
            
            # Enviar info de jefes externos al nuevo subordinado
            if self.external_bosses_cache:
                bosses_info = {
                    node_type: {
                        'ip': info['ip'],
                        'port': info['port']
                    } for node_type, info in self.external_bosses_cache.items()
                }
                logging.debug(f"Enviando info de jefes externos al subordinado: {bosses_info}")
                conn.send_message(
                    self._create_message(
                        MessageProtocol.MESSAGE_TYPES['EXTERNAL_BOSSES_INFO'],
                        {'bosses': bosses_info}
                    )
                )
                logging.debug(f"Información de {len(bosses_info)} jefes externos enviada al nuevo subordinado")
            else:
                logging.debug(f"No hay jefes externos para enviar al subordinado. Cache: {self.external_bosses_cache}")
            
            return True
        else:
            logging.error(f"No se pudo conectar con subordinado {node_ip}")
            # Eliminar de known_nodes si no se pudo conectar
            self.remove_node_from_registry(self.node_type, node_ip)
            return False
    
    def add_external_client(self, client_ip, client_node_type, client_port, existing_socket=None):
        """
        Agregar un cliente externo de otro tipo de nodo (cuando soy jefe).
        Por ejemplo: un Router conectándose a un Scrapper jefe.
        
        Args:
            client_ip (str): IP del nodo cliente
            client_node_type (str): Tipo de nodo del cliente ('router', 'bd', etc.)
            client_port (int): Puerto en el que escucha el cliente
            existing_socket (socket.socket, optional): Socket ya conectado
        
        Returns:
            bool: True si se agregó correctamente, False en caso contrario
        """
        # Validar que no se agregue a sí mismo
        if client_ip == self.ip:
            logging.warning(f"Intento de agregar a sí mismo como cliente externo ({client_ip}), ignorando")
            if existing_socket:
                existing_socket.close()
            return False
        
        # Verificar si ya existe
        if client_node_type in self.bosses_connections:
            logging.warning(f"Cliente externo {client_node_type} ya existe")
            return True
        
        # Crear NodeConnection hacia el cliente (aunque sea el cliente quien inició)
        conn = NodeConnection(
            client_node_type,  # Tipo del nodo remoto
            client_ip,
            client_port,  # Puerto correcto del cliente
            on_message_callback=self._handle_message_from_node,
            sender_node_type=self.node_type,  # Mi tipo
            sender_id=self.node_id  # Mi ID
        )
        
        if conn.connect(existing_socket=existing_socket):
            self.bosses_connections[client_node_type] = conn
            
            # Guardar info en cache para replicación
            self.external_bosses_cache[client_node_type] = {
                'ip': client_ip,
                'port': client_port  # Puerto correcto del cliente externo
            }
            
            logging.info(f"Cliente externo {client_node_type} {client_ip} agregado")
            logging.debug(f"Cache actualizado: {self.external_bosses_cache}")
            
            # Enviar identificación como jefe
            conn.send_message(
                self._create_message(
                    MessageProtocol.MESSAGE_TYPES['IDENTIFICATION'],
                    data={
                        'ip': self.ip,
                        'port': self.port,
                        'is_boss': True
                    }
                )
            )
            
            # Iniciar heartbeats
            threading.Thread(
                target=self._heartbeat_loop,
                args=(conn,),
                daemon=True
            ).start()
            
            # Replicar información de jefes externos a subordinados
            self.replicate_external_bosses_info()
            
            return True
        else:
            logging.error(f"No se pudo conectar con cliente externo {client_node_type} {client_ip}")
            # Eliminar de known_nodes si no se pudo conectar
            self.remove_node_from_registry(client_node_type, client_ip)
            return False
        
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
            
            # Enviar longitud (2 bytes) + mensaje (usando sendall para asegurar envío completo)
            temp_sock.sendall(message_length.to_bytes(2, 'big'))
            temp_sock.sendall(message_bytes)
            
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
        if not self.i_am_boss and self.my_boss_profile.connection:
            # Verificar si la conexión está cerrada
            if not self.my_boss_profile.connection.is_connected():
                boss_ip = self.my_boss_profile.connection.ip
                logging.warning(f"Jefe {self.my_boss_profile.connection.node_id} desconectado (conexión cerrada)")
                logging.warning("Iniciando elecciones para encontrar nuevo jefe...")
                
                # Desconectar del jefe muerto
                self.my_boss_profile.connection.disconnect()
                self.my_boss_profile.clear_connection()
                
                # Eliminar de known_nodes
                self.remove_node_from_registry(self.node_type, boss_ip)
                
                # Iniciar proceso de elección
                threading.Thread(target=self.call_elections, daemon=True).start()
            else:
                # La conexión está activa, verificar heartbeat
                time_since_heartbeat = self.my_boss_profile.connection.get_time_since_last_heartbeat()
                
                if time_since_heartbeat is not None and time_since_heartbeat > self.heartbeat_timeout:
                    boss_ip = self.my_boss_profile.connection.ip
                    logging.warning(f"Jefe {self.my_boss_profile.connection.node_id} no responde (último heartbeat hace {time_since_heartbeat:.1f}s)")
                    logging.warning("Iniciando elecciones para encontrar nuevo jefe...")
                    
                    # Desconectar del jefe muerto
                    self.my_boss_profile.connection.disconnect()
                    self.my_boss_profile.clear_connection()
                    
                    # Eliminar de known_nodes
                    self.remove_node_from_registry(self.node_type, boss_ip)
                    
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
                
                self.reassign_tasks_from_subordinate(node_id)
                
                del self.subordinates[node_id]
                
                # También remover de known_nodes
                ip = conn.ip
                if ip in self.nodes_cache.get(self.node_type, {}):
                    del self.nodes_cache[self.node_type][ip]
                    logging.info(f"Nodo {ip} eliminado de nodos conocidos")
            
            if dead_nodes:
                logging.info(f"Limpieza completada: {len(dead_nodes)} nodos eliminados")
                logging.info(f"Subordinados activos: {len(self.subordinates)}")
        
        # 3. Verificar conexiones con otros jefes
        for node_type, conn in list(self.bosses_connections.items()):
            if conn:
                # Verificar si la conexión está cerrada
                if not conn.is_connected():
                    boss_ip = conn.ip
                    logging.warning(f"Jefe {node_type} desconectado (conexión cerrada)")
                    conn.disconnect()
                    self.bosses_connections[node_type] = None
                    # Eliminar de known_nodes
                    self.remove_node_from_registry(node_type, boss_ip)
                    logging.info(f"Conexión con jefe de {node_type} cerrada")
                else:
                    # La conexión está activa, verificar heartbeat
                    time_since_heartbeat = conn.get_time_since_last_heartbeat()
                    
                    if time_since_heartbeat is not None and time_since_heartbeat > self.heartbeat_timeout:
                        boss_ip = conn.ip
                        logging.warning(f"Jefe {node_type} no responde (último heartbeat hace {time_since_heartbeat:.1f}s)")
                        conn.disconnect()
                        self.bosses_connections[node_type] = None
                        # Eliminar de known_nodes
                        self.remove_node_from_registry(node_type, boss_ip)
                        logging.info(f"Conexión con jefe de {node_type} cerrada")
                        
    def reassign_tasks_from_subordinate(self, node_id):
        """
        Reasigna las tareas que estaban asignadas a un subordinado que ha muerto.
        DEBE ser implementado por clases hijas que gestionen tareas.
        
        Args:
            node_id (str): ID del subordinado muerto
        
        Returns:
            int: Número de tareas reasignadas
        """
        # Implementación base: no hace nada (para nodos sin tareas como Router o BD)
        logging.debug(f"reassign_tasks_from_subordinate no implementado para {self.node_type}")
        return 0
        
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
    
    def discover_nodes(self, node_alias, node_port):
        """Descubre nodos utilizando el DNS interno de Docker.

        Args:
            node_alias (str): Alias del nodo a descubrir (ej. 'scrapper', 'bd', 'router').
            node_port (int): Puerto por defecto para los nodos descubiertos.

        Returns:
            list: Lista de IPs de nodos descubiertos.
        """
        
        while(True):
            try:
                # Resolver el alias que Docker maneja internamente
                result = socket.getaddrinfo(node_alias, None, socket.AF_INET)
                
                # Extraer todas las IPs únicas
                discovered_ips = []
                for addr_info in result:
                    ip = addr_info[4][0]  # La IP está en la posición [4][0]
                    if ip not in discovered_ips and ip != self.ip:
                        discovered_ips.append(ip)
            
                # Almacenar nodos descubiertos en known_nodes para uso posterior
                for ip in discovered_ips:
                    if node_alias not in self.nodes_cache:
                        self.nodes_cache[node_alias] = {}
                    
                    # Solo actualizar si no existe o actualizar last_seen
                    if ip not in self.nodes_cache[node_alias]:
                        self.nodes_cache[node_alias][ip] = {
                            "port": node_port,
                            "last_seen": datetime.now(),
                            "is_boss": False  # Por defecto, no sabemos si es jefe
                        }
                    else:
                        # Actualizar last_seen si ya existe
                        self.nodes_cache[node_alias][ip]["last_seen"] = datetime.now()
                
                discovered_count = len([ip for ip in discovered_ips if ip != self.ip])
                #logging.info(f"Nodos {node_alias} descubiertos: {discovered_count}")
                #logging.info(f"Mi IP: {self.ip}")
                #logging.info(f"IPs descubiertas: {[ip for ip in discovered_ips if ip != self.ip]}")
                
                #return [ip for ip in discovered_ips if ip != self.ip]
                discovered_ips = [ip for ip in discovered_ips if ip != self.ip]

                if self.node_type == node_alias and len(discovered_ips) != 0:
                    logging.info(f"Enviando identificación a todos los nodos {self.node_type}...")
                    boss_found = self.broadcast_identification(self.node_type)

                    if not boss_found and not self.i_am_boss:
                        logging.warning("No se encontraron jefes activos, iniciando elecciones")
                        self.call_elections()


                if not discovered_ips and node_alias == self.node_type and not self.boss_connection and not self.i_am_boss:
                    logging.info(f"No se encontraron otros nodos {self.node_type}. Asumiendo rol de jefe.")
                    self.i_am_boss = True

                else:
                    #logging.info(f"Descubiertos {len(discovered_ips)} nodos {self.node_type}: {discovered_ips}")
                    pass

                
            except socket.gaierror as e:
                #logging.error(f"Error consultando DNS de Docker para {node_alias}: {e}")
                continue
            except Exception as e:
                #logging.error(f"Error inesperado en descubrimiento de {node_alias}: {e}")
                continue
            
            time.sleep(10)
    
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
            return self.nodes_cache.get(node_type, {})
        return self.nodes_cache
    
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
            self.listen_socket.bind((self.ip, self.port))
            self.listen_socket.listen(10)
            
            # Iniciar hilo de escucha
            self.listen_thread = threading.Thread(
                target=self._listen_for_connections,
                daemon=True
            )
            self.listen_thread.start()
            
            logging.info(f"Escuchando conexiones en {self.ip}:{self.port}")
            
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
            
            message_str = message_bytes.decode('utf-8')
            message = json.loads(message_str)
            logging.info(f"message: {message}")
            #logging.info(f"type: {type(message)}")
            msg_type = message.get('type')
            
            # handler para procesar mensaje
            handler = self.temporary_message_handler.get(msg_type)
            if handler:
                handler(sock, client_ip, message)
            else:
                logging.warning(f"Mensaje desconocido de {client_ip}: {msg_type}")
                sock.close()
                
        except socket.timeout:
            logging.warning(f"Timeout esperando mensaje de {client_ip}")
            sock.close()
        except Exception as e:
            logging.error(f"Error manejando conexión entrante de {client_ip}: {e}")
            sock.close()  
    
    def add_persistent_message_handler(self, msg_type, handler_func):
        """Agrega un manejador persistente para un tipo de mensaje específico"""
        self.persistent_message_handler[msg_type] = handler_func
        
    def add_temporary_message_handler(self, msg_type, handler_func):
        """Agrega un manejador temporal para un tipo de mensaje específico"""
        self.temporary_message_handler[msg_type] = handler_func
        
    def broadcast_identification(self, node_type):
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
        discovered = self.nodes_cache.get(node_type, {})
        
        if not discovered:
            logging.warning(f"No hay nodos {node_type} conocidos para contactar")
            return False
    
        boss_found = False
        
        for ip, info in discovered.items():
            if ip == self.ip:
                continue
            
            # Enviar identificación TEMPORAL (para descubrimiento solamente)
            identification = self._create_message(
                MessageProtocol.MESSAGE_TYPES['IDENTIFICATION'],
                {
                    'node_port': self.port,
                    'is_boss': self.i_am_boss,
                    'is_temporary': True  # Marcar como temporal
                }
            )
            
            logging.debug(f"Identificación enviada a {ip}")
            response = self.send_temporary_message(ip, info["port"], identification, 
                                                   expect_response=True, 
                                                   timeout=5.0, 
                                                   node_type=node_type)
            
            if response and response.get('type') == MessageProtocol.MESSAGE_TYPES['IDENTIFICATION']:
                # Extraer datos del campo 'data'
                response_data = response.get('data', {})
                if response_data.get('is_boss', False):
                    # ¡Es el jefe!
                    logging.info(f"¡Jefe encontrado en {ip}!")
                    boss_found = True
                
                # Crear NUEVA conexión persistente (evita conflicto de sockets)
                logging.debug("Estableciendo conexión persistente con jefe...")
                time.sleep(0.3)  # Pequeña pausa para que el jefe registre
                
                self.connect_to_boss(ip)
                
                # El hilo de heartbeat ya se inicia en connect_to_boss()
                if self.boss_connection and self.boss_connection.is_connected():
                    logging.info("Conexión con jefe ya establecida")
                else:
                    logging.error(f"No se pudo establecer conexión persistente con {ip}")
                    boss_found = False
        
        if not boss_found:
            logging.warning("No se encontró ningún jefe que respondiera")
        
        return boss_found
    
    def start_boss_tasks(self):
        """Inicia tareas específicas si este nodo es jefe"""
        if self.i_am_boss:
            logging.info("Iniciando tareas de jefe...")
            # Aquí irían las tareas específicas del jefe
            # Ejemplo: iniciar asignación de tareas, monitoreo, etc.
            pass
        
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
        logging.info(f"Mi IP: {self.ip}")
        
        # Obtener nodos del mismo tipo conocidos
        known_nodes_of_my_type = self.nodes_cache.get(self.node_type, {})
        
        if not known_nodes_of_my_type:
            logging.info("No hay otros nodos conocidos. Me autoproclamo jefe.")
            self._become_boss()
            return
        
        # Filtrar nodos con IP mayor que la mía
        higher_ip_nodes = []
        for ip, info in known_nodes_of_my_type.items():
            if ip > self.ip:
                higher_ip_nodes.append((ip, info["port"]))
        
        if not higher_ip_nodes:
            logging.info(f"No hay nodos con IP mayor que {self.ip}. Me autoproclamo jefe.")
            self._become_boss()
            return
        
        # Ordenar de mayor a menor IP (para encontrar al jefe más rápido)
        higher_ip_nodes.sort(reverse=True)
        logging.info(f"Contactando nodos con IP mayor: {[ip for ip, _ in higher_ip_nodes]}")
        
        # Enviar mensaje de elección a cada uno (de mayor a menor)
        someone_responded = False
        
        for ip, port in higher_ip_nodes:
            election_msg = self._create_message(
                MessageProtocol.MESSAGE_TYPES['ELECTION'],
                {
                    'ip': self.ip,
                    'port': self.port
                }
            )
            
            logging.info(f"Mensaje de elección enviado a {ip}")
            response = self.send_temporary_message(ip, port, election_msg, 
                                                   expect_response=True, 
                                                   timeout=3.0, 
                                                   node_type=self.node_type)
            
            if response and response.get('type') == MessageProtocol.MESSAGE_TYPES['ELECTION_RESPONSE']:
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
        if self.my_boss_profile.connection:
            self.my_boss_profile.connection.disconnect()
            self.my_boss_profile.clear_connection()
        
        # # Limpiar subordinados antiguos (por si acaso)
        # old_subordinates = list(self.subordinates.keys())
        # for node_id in old_subordinates:
        #     conn = self.subordinates[node_id]
            
        #     # Reasignar tareas antes de desconectar
        #     reassigned = self.task_queue.reassign_node_tasks(node_id)
        #     if reassigned > 0:
        #         logging.info(f"Reasignadas {reassigned} tareas del subordinado antiguo {node_id}")
            
        #     conn.disconnect()
        #     del self.subordinates[node_id]
            
        # #Esperar un tiempo para q todos los nodos procesen la desconexión del jefe anterior
        # time.sleep(2)
        
        logging.info("=== ENVIANDO ANUNCIO DE NUEVO JEFE ===")
        
        # Obtener todos los nodos conocidos
        all_known_ips = set()
        
        # Agregar nodos conocidos (ya incluye descubiertos e identificados)
        for ip in self.nodes_cache.get(self.node_type, {}).keys():
            if ip != self.ip:
                all_known_ips.add(ip)
        
        logging.info(f"Notificando a {len(all_known_ips)} nodos: {list(all_known_ips)}")
        
        # 1. Enviar mensaje "new_boss" a todos los nodos conocidos
        for ip in all_known_ips:
            if ip == self.ip:
                continue
            
            port = self.nodes_cache.get(self.node_type, {}).get(ip, {}).get("port", self.port)
            
            new_boss_msg = self._create_message(
                MessageProtocol.MESSAGE_TYPES['NEW_BOSS'],
                {
                    'ip': self.ip,
                    'port': self.port
                }
            )
            
            if self.send_temporary_message(ip, port, new_boss_msg, 
                                           expect_response=False, 
                                           node_type=self.node_type):
                logging.info(f"✓ Anuncio 'new_boss' enviado a {ip}")
            else:
                logging.warning(f"✗ No se pudo enviar anuncio a {ip} (nodo eliminado de registro)")
        
        # 2. Esperar un momento para que los nodos procesen el mensaje
        logging.info("Esperando a que los nodos procesen el anuncio...")
        time.sleep(2)
        
        # 3. Establecer conexiones persistentes con todos los subordinados
        logging.info("=== ESTABLECIENDO CONEXIONES CON SUBORDINADOS ===")
        
        # 3.1 Conectarse a jefes externos si tengo su información
        self._connect_to_external_bosses()
        
        connected_count = 0
        for ip in all_known_ips:
            if ip == self.ip:
                continue
            
            port = self.nodes_cache.get(self.node_type, {}).get(ip, {}).get("port", self.port)
            
            # Intentar agregar como subordinado
            if self.add_subordinate(ip):
                connected_count += 1
                logging.info(f"✓ Subordinado {ip} conectado exitosamente")
            else:
                logging.warning(f"✗ No se pudo conectar con {ip}")
        
        logging.info(f"=== JEFATURA ESTABLECIDA: {connected_count}/{len(all_known_ips)} subordinados conectados ===")
        
        # Iniciar tareas de jefe (si aplica)
        self.start_boss_tasks() # thread?

    def _discover_boss_nodes(self):
        '''Hilo que buscara continuamente los otros jefes'''
        
        logging.info("Soy el jefe, buscando a los otros jefes...")
        others_roles = list(self.nodes_cache.keys())
        others_roles = [rol for rol in others_roles if rol != self.node_type]
        logging.info(f"others_roles: {others_roles}")

        while(True):
            for rol in others_roles:
                try:
                    # Resolver el alias que Docker maneja internamente
                    result = socket.getaddrinfo(rol, None, socket.AF_INET)
                    
                    # Extraer todas las IPs únicas
                    discovered_ips = []
                    for addr_info in result:
                        ip = addr_info[4][0]  # La IP está en la posición [4][0]
                        if ip not in discovered_ips and ip != self.ip:
                            discovered_ips.append(ip)
                
                    # Almacenar nodos descubiertos en known_nodes para uso posterior
                    for ip in discovered_ips:
                        if rol not in self.nodes_cache:
                            self.nodes_cache[rol] = {}
                        
                        # Solo actualizar si no existe o actualizar last_seen
                        if ip not in self.nodes_cache[rol]:
                            self.nodes_cache[rol][ip] = {
                                "port": rol,
                                "last_seen": datetime.now(),
                                "is_boss": False  # Por defecto, no sabemos si es jefe
                            }
                        else:
                            # Actualizar last_seen si ya existe
                            self.nodes_cache[rol][ip]["last_seen"] = datetime.now()

                    logging.info(f"Nodos {rol} descubiertos {len(discovered_ips)}")
                    logging.info(f"IP descubiertas: {discovered_ips}")

                except socket.gaierror as e:
                    #logging.error(f"Error consultando DNS de Docker para {rol}: {e}")
                    continue
                    
                except Exception as e:
                    logging.error(f"Error inesperado en descubrimiento de {rol}: {e}")

            time.sleep(20)

    def _comprobar_mi_jefatura(self):
        '''Hilo que comprueba cada cirto tiempo si me converti en jefe, en caso de ser afirmativo, 
        ejecuto una serie de pasos y salgo del metodo, ya que la unica manera de dejar de ser jefe
        es desconectandose de la red'''

        while(True):
            if self.i_am_boss:
                threading.Thread(
                    target=self._discover_boss_nodes,
                    daemon=True
                ).start()
                break
            else:
                time.sleep(1)

    def start(self):
        '''Inicia el nodo (escucha, heartbeat, etc.)'''
        
        self.running = True
        logging.info(f"Iniciando nodo {self.node_id} (tipo: {self.node_type})")
        
        # 1. Descubrir otros nodos del mismo node_type
        logging.info(f"Descubriendo nodos {self.node_type} en la red...")

        threading.Thread(
            target=self.discover_nodes,
            args=(self.node_type, self.port),
            daemon=True
        ).start()
        logging.info("Hilo de descubrimiento de nodos del mismo tipo iniciado")
        
        #if not discovered_ips:
        #    logging.info(f"No se encontraron otros nodos {self.node_type}. Asumiendo rol de jefe.")
        #    self.i_am_boss = True
        #else:
        #    logging.info(f"Descubiertos {len(discovered_ips)} nodos {self.node_type}: {discovered_ips}")
        
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
        
        # 4. Broadcast de identificación (todos me registran, solo jefe responde) --- YA SE HACE EN discover_nodes
        #if discovered_ips:
        #    logging.info(f"Enviando identificación a todos los nodos {self.node_type}...")
        #    boss_found = self.broadcast_identification(self.node_type)
            
        #    if not boss_found:
        #        logging.warning("No se encontró jefe activo. Iniciando elecciones...")
        #        self.call_elections()

        # 5. Comportamiento según rol
        # Ya en discover_boss_nodes se busca a los otros jefes, solo para el nodo cuando es jefe

        # hilo de comprobar la jefatura
        threading.Thread(
            target=self._comprobar_mi_jefatura,
            daemon=True
        ).start()

        time.sleep(1)

        if self.i_am_boss:
            logging.info(f"🔶 Soy el JEFE de nodos {self.node_type}")
            # Iniciar hilo de asignación de tareas
            self.start_boss_tasks() # thread?
            # TODO: Conectar con jefes de BD y Router si es necesario
            # self.discover_nodes("bd", self.bd_port)
            # self.connect_to_discovered_nodes("bd")
        else:
            logging.info(f"✓ Soy subordinado {self.node_type}, conectado al jefe en {self.boss_connection.ip if self.boss_connection else 'desconocido'}")
        
        logging.info("Nodo iniciado correctamente.")
        
        # Mantener vivo
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Deteniendo nodo...")
            self.stop()
            
    def stop(self):
        '''Detiene el nodo y cierra todas las conexiones'''
        self.running = False
        
        # Cerrar socket de escucha
        if self.listen_socket:
            try:
                self.listen_socket.close()
            except:
                pass
            self.listen_socket = None
        
        # Cerrar conexión con jefe
        if self.my_boss_profile.connection:
            self.my_boss_profile.connection.disconnect()
            self.my_boss_profile.clear_connection()
        
        # Cerrar conexiones con subordinados
        for node_id, conn in self.subordinates.items():
            conn.disconnect()
        self.subordinates.clear()
        
        for node_type, conn in self.bosses_connections.items():
            if conn:
                conn.disconnect()
        self.bosses_connections.clear()
        
        logging.info("Nodo detenido.")