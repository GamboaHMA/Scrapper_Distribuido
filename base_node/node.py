from operator import ne
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

def compare_ips(ip1, ip2):
    """
    Compara dos direcciones IP numéricamente, no lexicográficamente.
    
    Returns:
        -1 si ip1 < ip2
        0 si ip1 == ip2
        1 si ip1 > ip2
    """
    try:
        # Convertir IPs a tuplas de enteros para comparación numérica
        parts1 = tuple(int(x) for x in ip1.split('.'))
        parts2 = tuple(int(x) for x in ip2.split('.'))
        
        if parts1 < parts2:
            return -1
        elif parts1 > parts2:
            return 1
        else:
            return 0
    except (ValueError, AttributeError):
        # Fallback a comparación lexicográfica si hay error
        logging.warning(f"Error comparando IPs {ip1} y {ip2}, usando comparación lexicográfica")
        if ip1 < ip2:
            return -1
        elif ip1 > ip2:
            return 1
        else:
            return 0

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
        self.subordinates_lock = threading.Lock()

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
        self.bosses_connections_lock = threading.Lock()
        
        self.listen_socket = None
        self.listen_thread = None
        
        # Thread de monitoreo de conexiones
        self.connection_monitor_thread = None
        self.connection_monitor_stop_event = threading.Event()
        self.connection_check_interval = 10  # Verificar cada 10 segundos
        
        # # Hilo de monitoreo de heartbeats
        # self.heartbeat_monitor_thread = None
        # self.heartbeat_timeout = 40  # segundos sin heartbeat antes de considerar muerto
        # self.heartbeat_check_interval = 30  # revisar cada 30 segundos
        
        # TODO: handler persistente para NEW_BOSS (anuncio de nuevo jefe)
        self.persistent_message_handler = {
            MessageProtocol.MESSAGE_TYPES['IDENTIFICATION']: self._handle_identification,
            MessageProtocol.MESSAGE_TYPES['STATUS_UPDATE']: self._handle_status_update,
            MessageProtocol.MESSAGE_TYPES['EXTERNAL_BOSSES_INFO']: self._handle_external_bosses_info,
            MessageProtocol.MESSAGE_TYPES['NEW_EXTERNAL_BOSS']: self._handle_new_external_boss_persistent,
            MessageProtocol.MESSAGE_TYPES['NEW_BOSS']: self._handle_new_boss_persistent_message,
            MessageProtocol.MESSAGE_TYPES['NEW_BOSS_NO_ROUTER']: self._handle_new_boss_no_router_persistent,
            # Agregar más manejadores según los tipos de mensaje necesarios
        }
        self.temporary_message_handler = {
            MessageProtocol.MESSAGE_TYPES['IDENTIFICATION']: self._handle_identification_incoming,
            MessageProtocol.MESSAGE_TYPES['ELECTION']: self._handle_election_message,
            MessageProtocol.MESSAGE_TYPES['NEW_BOSS']: self._handle_new_boss_message,
            MessageProtocol.MESSAGE_TYPES['NEW_EXTERNAL_BOSS']: self._handle_new_external_boss_temporary,
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
            
    # def _handle_heartbeat(self, node_connection, message_dict):
    #     """Procesa mensaje de heartbeat"""
    #     # ya el NodeConnection maneja el update del heartbeat
    #     pass
        
    def _handle_new_boss_persistent_message(self, node_connection, message_dict):
        data = message_dict.get('data', {})
        new_boss_ip = data.get('ip', {})
        new_boss_port = data.get('port', {})
        new_boss_node_type = data.get('node_type', {})
        
        self._connect_to_external_boss(new_boss_node_type, new_boss_ip, new_boss_port)
        
        
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
        
        # TODO: Lógica de actualización de referencias según rol y tipo
        # Si soy jefe y un subordinado se identifica, ya lo tengo registrado
        # Si no soy jefe y el nodo es jefe, actualizar mi referencia
        # if sender_node_type == self.node_type:
        #     # Mismo tipo de nodo
        #     if self.i_am_boss and not is_boss:
        #         # Soy jefe y es subordinado → ya lo tengo registrado
        #         logging.debug(f"Subordinado {node_ip} identificado")
        #     elif not self.i_am_boss and is_boss:
        #         # Soy subordinado y es jefe → actualizar referencia
        #         self.my_boss_profile.set_connection(node_connection)
        #         logging.info(f"Jefe {sender_node_type} identificado: {node_ip}")
        # else:
        #     # Diferente tipo de nodo (jefe externo)
        #     if not self.i_am_boss and is_boss:
        #         self.my_boss_profile.set_connection(node_connection)
        #         logging.info(f"Jefe {sender_node_type} identificado: {node_ip}")
        #     else:
        #         logging.debug(f"Identificación recibida de {sender_node_type} {node_ip} (boss={is_boss})")
    
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
        
        # Enviar solo a subordinados conectados
        sent_count = 0
        with self.subordinates_lock:
            for node_id, conn in self.subordinates.items():
                if conn.is_connected():
                    if conn.send_message(message):
                        sent_count += 1
                else:
                    logging.debug(f"Subordinado {node_id} desconectado, no se envía replicación")
            
            logging.info(f"Información de {len(bosses_info)} jefes externos replicada a {sent_count}/{len(self.subordinates)} subordinados")
    
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
    
    def _handle_new_external_boss_persistent(self, node_connection, message_dict):
        """
        Handler persistente para NEW_EXTERNAL_BOSS (ej: Router notifica a un jefe ya conectado).
        
        Args:
            node_connection: Conexión persistente desde la que viene el mensaje
            message_dict: Dict con el mensaje completo
        """
        sender_type = node_connection.node_type if node_connection else "desconocido"
        logging.debug(f"📥 NEW_EXTERNAL_BOSS recibido por conexión persistente desde {sender_type}")
        logging.debug(f"llaves para bosses_connections: {self.bosses_connections.keys()}")

        self._process_new_external_boss_info(message_dict.get('data', {}), sender_type)
        logging.debug(f"llaves para bosses_connections(after): {self.bosses_connections.keys()}")

    
    def _handle_new_external_boss_temporary(self, sock, client_ip, message):
        """
        Handler temporal para NEW_EXTERNAL_BOSS (ej: conexión de notificación directa).
        
        Args:
            sock: Socket temporal
            client_ip: IP del remitente
            message: Dict del mensaje
        """
        logging.debug(f"📥 NEW_EXTERNAL_BOSS recibido por conexión temporal desde {client_ip}")
        self._process_new_external_boss_info(message.get('data', {}), client_ip)
    
    # TODO: Quien manda este mensaje (temporal)?
    def _process_new_external_boss_info(self, data, sender_info):
        """
        Procesa información de nuevo(s) jefe(s) externo(s) y crea conexiones.
        
        Args:
            data: Dict con 'bosses' o campos legacy (ip, port, node_type)
            sender_info: Identificador del remitente (para logs)
        """
        # Formato 1: Lista de jefes {'bosses': {'bd': {...}, 'scrapper': {...}}}
        bosses_info = data.get('bosses', {})
        
        if bosses_info:
            # Procesar múltiples jefes
            logging.info(f"📥 Información de {len(bosses_info)} jefe(s) externo(s) recibida de {sender_info}")
            
            for boss_type, boss_info in bosses_info.items():
                boss_ip = boss_info.get('ip')
                boss_port = boss_info.get('port')
                
                if not boss_ip or not boss_port:
                    logging.warning(f"Información incompleta para jefe {boss_type}")
                    continue
                
                self._connect_to_external_boss(boss_type, boss_ip, boss_port)
        else:
            # Formato 2 (legacy): Info de un solo jefe
            new_boss_ip = data.get('ip')
            new_boss_port = data.get('port')
            new_boss_type = data.get('node_type')
            
            if not all([new_boss_ip, new_boss_port, new_boss_type]):
                logging.warning("Mensaje NEW_EXTERNAL_BOSS con datos incompletos")
                return
            
            logging.info(f"Nuevo jefe externo {new_boss_type} notificado: {new_boss_ip}:{new_boss_port}")
            self._connect_to_external_boss(new_boss_type, new_boss_ip, new_boss_port)
            
    def _handle_new_boss_no_router_persistent(self, node_connection, message_dict):
        """
        Handler persistente para NEW_BOSS_NO_ROUTER.
        
        Args:
            node_connection: Conexión persistente desde la que viene el mensaje
            message_dict: Dict con el mensaje completo
        """
        data = message_dict.get('data', {})
        new_boss_ip = data.get('ip')
        new_boss_port = data.get('port')
        new_boss_type = data.get('node_type')
        
        logging.debug(f"📥 NEW_BOSS_NO_ROUTER recibido de {new_boss_ip}:{new_boss_port}")
        
        # Mandar mensaje BOSS_NO_ROUTER_REUNIFICATION temporal
        message = self._create_message(MessageProtocol.MESSAGE_TYPES['BOSS_NO_ROUTER_REUNIFICATION'],
                                       {
                                           'ip': self.ip,
                                           'port': self.port,
                                           'node_type': self.node_type
                                       }
        )
        response = self.send_temporary_message(
            new_boss_ip,
            new_boss_port,
            message,
            expect_response=True
        )
        if response:
            accepted = response.get('data', {}).get('accepted', False)
            new_boss_ip = response.get('data', {}).get('ip', None)
            new_boss_port = response.get('data', {}).get('port', None)
            new_boss_node_type = response.get('data', {}).get('node_type', None)
            
            if accepted:
                logging.info(f"Fui aceptado por {new_boss_ip}:{new_boss_port} como jefe de tipo {new_boss_node_type}")
                # Crear node connection con el new_boss y mantenerme como jefe
                conn = NodeConnection(
                    self.node_type,
                    new_boss_ip,
                    new_boss_port,
                    on_message_callback=self._handle_message_from_node,
                    sender_node_type=self.node_type,
                    sender_id=self.node_id
                )
                if conn.connect():
                    self.bosses_connections[new_boss_node_type] = conn
                    logging.info(f"Conexión establecida con nuevo jefe {new_boss_node_type} en {new_boss_ip}:{new_boss_port}")

            else:
                logging.info(f"Nodo {new_boss_ip}:{new_boss_port} no aceptado como jefe de tipo {self.node_type}")
                # Si no fui aceptado, el me mando info de su jefe scrapper
                # Debo asumir ese jefe scrapper como mi jefe y comunicarlo a mis subordinados
                
                #Mensaje para mis subordinados
                subordinates_message = self._create_message(
                    MessageProtocol.MESSAGE_TYPES['NEW_BOSS'],
                    {
                        'ip': new_boss_ip,
                        'port': new_boss_port,
                        'node_type': new_boss_node_type
                    }
                )
                self.broadcast_to_subordinates(subordinates_message)
                self.stop_boss_tasks()

                # Asumir el nuevo jefe scrapper
                if new_boss_ip and new_boss_port:
                    logging.info(f"Asumiendo nuevo jefe scrapper {new_boss_ip}:{new_boss_port}")
                    self._connect_to_external_boss(new_boss_node_type, new_boss_ip, new_boss_port)

    
    def _connect_to_external_boss(self, boss_type, boss_ip, boss_port):
        """
        Conecta con un jefe externo y actualiza el cache.
        
        Args:
            boss_type: Tipo de nodo ('bd', 'scrapper', 'router')
            boss_ip: IP del jefe
            boss_port: Puerto del jefe
        """
        # Lock para evitar race condition con add_external_client
        if not hasattr(self, 'bosses_connections_lock'):
            self.bosses_connections_lock = threading.Lock()
        
        with self.bosses_connections_lock:
            # Si ya existe conexión con la misma IP, no hacer nada
            if boss_type in self.bosses_connections:
                existing_conn = self.bosses_connections[boss_type]
                if existing_conn.ip == boss_ip and existing_conn.is_connected():
                    logging.debug(f"Ya existe conexión activa con jefe {boss_type} en {boss_ip}")
                    return
                else:
                    # IP diferente o conexión muerta, cerrar la antigua
                    logging.info(f"Cerrando conexión antigua con jefe {boss_type}")
                    existing_conn.disconnect()
                    del self.bosses_connections[boss_type]
            
            # Actualizar cache
            self.external_bosses_cache[boss_type] = {
                'ip': boss_ip,
                'port': boss_port
            }
            
            # Crear nueva conexión persistente
            conn = NodeConnection(
                boss_type,
                boss_ip,
                boss_port,
                on_message_callback=self._handle_message_from_node,
                sender_node_type=self.node_type,
                sender_id=self.node_id
            )
            
            if conn.connect():
                self.bosses_connections[boss_type] = conn
                
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
                
                # Replicar info a subordinados
                self.replicate_external_bosses_info()
                
                logging.info(f"✓ Conexión con jefe externo {boss_type} ({boss_ip}:{boss_port}) establecida")
            else:
                logging.error(f"✗ No se pudo conectar con jefe externo {boss_type} en {boss_ip}:{boss_port}")
    
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
        
        # Registrar el nodo en nodes_cache usando el tipo del remitente
        if sender_node_type not in self.nodes_cache:
            self.nodes_cache[sender_node_type] = {}
        
        self.nodes_cache[sender_node_type][client_ip] = {
            "port": data.get('node_port', self.port),
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
                    # Es otro jefe de mi mismo tipo → COMPARAR IPs
                    logging.warning(f"⚠️  Conflicto: Otro jefe {sender_node_type} ({client_ip}) detectado. Comparando IPs...")
                    
                    # Comparar IPs numéricamente (no lexicográficamente)
                    ip_comparison = compare_ips(self.ip, client_ip)
                    
                    if ip_comparison > 0:
                        # Mi IP es mayor → YO sigo siendo jefe, él se vuelve subordinado
                        logging.info(f"✓ Mi IP ({self.ip}) > Su IP ({client_ip}). Mantengo rol de jefe, registrándolo como subordinado.")
                        
                        # Agregar como subordinado
                        success = self.add_subordinate(client_ip, existing_socket=sock)
                        
                        if success:
                            # Notificar a la subclase que se heredó un nuevo subordinado de un conflicto
                            self._on_subordinate_inherited_from_conflict(client_ip)
                        else:
                            logging.error(f"No se pudo registrar {client_ip} como subordinado")
                            sock.close()
                    else:
                        # Su IP es mayor o igual → ÉL debe ser jefe, yo me vuelvo subordinado
                        logging.warning(f"⚠️  Su IP ({client_ip}) >= Mi IP ({self.ip}). Cediendo rol de jefe...")
                        
                        # Cerrar el socket entrante (él debe iniciar la conexión como jefe)
                        sock.close()
                        
                        # Ceder el rol de jefe
                        self._demote_to_subordinate(client_ip)
            
            else:
                # No soy jefe. Verificar si es mi jefe adoptándome (via add_subordinate desde el boss)
                if is_boss and sender_node_type == self.node_type:
                    # El jefe de mi tipo me está adoptando como subordinado.
                    # Crear NodeConnection usando el socket entrante y registrarlo como mi jefe.
                    logging.info(f"Jefe {client_ip} adoptándome como subordinado. Aceptando conexión...")
                    conn = NodeConnection(
                        self.node_type,
                        client_ip,
                        data.get('node_port', self.port),
                        on_message_callback=self._handle_message_from_node,
                        sender_node_type=self.node_type,
                        sender_id=self.node_id
                    )
                    if conn.connect(existing_socket=sock):
                        with self.my_boss_profile.lock:
                            if self.my_boss_profile.connection:
                                self.my_boss_profile.connection.disconnect()
                            self.my_boss_profile.set_connection(conn)
                        logging.info(f"✓ Conectado al jefe {client_ip} via adopción")
                    else:
                        logging.error(f"No se pudo aceptar adopción del jefe {client_ip}")
                        sock.close()
                else:
                    # Conexión persistente de tipo inesperado mientras no soy jefe → rechazar
                    logging.debug(f"Cerrando conexión persistente de {sender_node_type} {client_ip} (no soy jefe)")
                    sock.close()
    
    def _demote_to_subordinate(self, new_boss_ip):
        """
        Cede el rol de jefe y se convierte en subordinado del nodo con new_boss_ip.
        
        Args:
            new_boss_ip: IP del nodo que debe ser el nuevo jefe
        """
        logging.warning(f"🔻 Cediendo rol de jefe. Nuevo jefe: {new_boss_ip}")
        
        # 1. Dejar de ser jefe
        self.i_am_boss = False
        
        # 2. Detener tareas de jefe
        try:
            self.stop_boss_tasks()
            logging.info("Tareas de jefe detenidas")
        except Exception as e:
            logging.error(f"Error deteniendo tareas de jefe: {e}")
            
        # 3. Enviar anuncio de nuevo jefe a todos los subordinados
        announcement = self._create_message(
            MessageProtocol.MESSAGE_TYPES['NEW_BOSS'],
            {
                'ip': new_boss_ip,
                'port': self.port
            }
        )
        
        if self.subordinates:
            logging.info(f"Anunciando a {len(self.subordinates)} subordinados sobre el nuevo jefe {new_boss_ip} y cerrando conexiones con ellos...")
            with self.subordinates_lock:
                for node_id, conn in list(self.subordinates.items()):
                    try:
                        conn.send_message(announcement)
                        logging.info(f"Anuncio de nuevo jefe enviado a subordinado {node_id}")
                    except Exception as e:
                        logging.error(f"Error enviando anuncio a subordinado {node_id}: {e}")
                    try:
                        conn.disconnect()
                    except Exception as e:
                        logging.error(f"Error desconectando subordinado {node_id}: {e}")
                self.subordinates.clear()                  
        
        # 4. Conectarse al nuevo jefe
        logging.info(f"Conectando al nuevo jefe en {new_boss_ip}:{self.port}...")
        self.connect_to_boss(new_boss_ip)
        
        logging.info(f"✓ Transición completada. Ahora soy subordinado de {new_boss_ip}")
    
    def _connect_to_boss_as_subordinate(self, boss_ip, boss_port):
        """
        Se conecta a un jefe como subordinado.
        
        Args:
            boss_ip: IP del jefe
            boss_port: Puerto del jefe
        """
        try:
            # Crear conexión al jefe
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((boss_ip, boss_port))
            
            # Enviar IDENTIFICATION como subordinado
            identification = self._create_message(
                MessageProtocol.MESSAGE_TYPES['IDENTIFICATION'],
                {
                    'node_port': self.port,
                    'is_boss': False,  # Ahora soy subordinado
                    'is_temporary': False  # Conexión persistente
                }
            )
            
            msg_bytes = json.dumps(identification).encode()
            sock.sendall(len(msg_bytes).to_bytes(2, 'big'))
            sock.sendall(msg_bytes)
            
            # Crear NodeConnection
            boss_conn = NodeConnection(
                node_type=self.node_type,
                ip=boss_ip,
                port=boss_port,
                on_message_callback=self._handle_message_from_node,
                sender_node_type=self.node_type,
                sender_id=self.node_id
            )
            
            # Conectar usando el socket existente
            if boss_conn.connect(existing_socket=sock):
                # Actualizar mi perfil de jefe
                with self.my_boss_profile.lock:
                    self.my_boss_profile.set_connection(boss_conn)
                
                logging.info(f"✓ Conectado exitosamente al jefe {boss_ip}:{boss_port}")
            else:
                logging.error(f"No se pudo establecer NodeConnection con {boss_ip}:{boss_port}")
            
        except Exception as e:
            logging.error(f"Error conectando al nuevo jefe {boss_ip}:{boss_port}: {e}")
    
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
        
        # Solo iniciar elecciones si no soy jefe todavía
        # Si ya soy jefe, no es necesario (y podría disrumpir conexiones activas)
        if not self.i_am_boss:
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
            # threading.Thread(
            #     target=self._heartbeat_loop,
            #     args=(new_connection,),
            #     daemon=True
            # ).start()
            
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
    
    # def _heartbeat_loop(self, node_connection):
    #     """Envía heartbeats periódicos a una conexión"""
    #     while self.running and node_connection.is_connected():
    #         node_connection.send_heartbeat()
    #         time.sleep(30)  # Heartbeat cada 30 segundos
            
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
            with self.subordinates_lock:
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
            # threading.Thread(
            #     target=self._heartbeat_loop,
            #     args=(conn,),
            #     daemon=True
            # ).start()
            
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
        
        # Lock para prevenir race conditions cuando el mismo jefe se conecta simultáneamente desde ambos lados
        if not hasattr(self, 'bosses_connections_lock'):
            self.bosses_connections_lock = threading.Lock()
        
        with self.bosses_connections_lock:
            # Verificar si ya existe
            if client_node_type in self.bosses_connections:
                existing_conn = self.bosses_connections[client_node_type]
                
                # Verificar que existing_conn no sea None
                if existing_conn is not None:
                    # Si es la misma IP y la conexión está activa, no hacer nada
                    if existing_conn.ip == client_ip and existing_conn.is_connected():
                        logging.warning(f"Cliente externo {client_node_type} {client_ip} ya existe y está conectado")
                        if existing_socket:
                            existing_socket.close()
                        return True
                    else:
                        # La IP cambió o la conexión está muerta, reemplazar
                        logging.info(f"Reemplazando cliente externo {client_node_type}: {existing_conn.ip} → {client_ip}")
                        try:
                            existing_conn.disconnect()
                        except:
                            pass
                else:
                    # La conexión es None, eliminar la entrada
                    logging.warning(f"Entrada None encontrada para {client_node_type}, eliminando...")
                    del self.bosses_connections[client_node_type]
        
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
                # threading.Thread(
                #     target=self._heartbeat_loop,
                #     args=(conn,),
                #     daemon=True
                # ).start()
                
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
    
    # def _heartbeat_monitor_loop(self):
    #     """
    #     Hilo que monitorea los heartbeats de todas las conexiones.
    #     Si un nodo no ha enviado heartbeat en heartbeat_timeout segundos,
    #     se considera muerto y se desconecta.
    #     """
    #     logging.info(f"Iniciando monitor de heartbeats (timeout: {self.heartbeat_timeout}s, check interval: {self.heartbeat_check_interval}s)")
        
    #     while self.running:
    #         try:
    #             time.sleep(self.heartbeat_check_interval)
                
    #             # Limpiar nodos muertos
    #             self._cleanup_dead_nodes()
                
    #         except Exception as e:
    #             logging.error(f"Error en monitor de heartbeats: {e}")
    
    def _connection_monitor_loop(self):
        """
        Thread que monitorea periódicamente el estado de las conexiones.
        - Si soy subordinado: verifica que el jefe esté vivo, sino llama a elecciones
        - Si soy jefe: verifica que los subordinados estén vivos y los elimina si no
        - Verifica conexiones con otros jefes
        """
        logging.info(f"Iniciando monitoreo de conexiones (intervalo: {self.connection_check_interval}s)")
        
        while self.running and not self.connection_monitor_stop_event.is_set():
            try:
                time.sleep(self.connection_check_interval)
                
                # Limpiar nodos muertos (verifica jefe, subordinados y otros jefes)
                self._cleanup_dead_nodes()
                
            except Exception as e:
                logging.error(f"Error en monitor de conexiones: {e}")
        
        logging.info("Thread de monitoreo de conexiones finalizado")
    
    def _cleanup_dead_nodes(self):
        """
        Verifica todas las conexiones y elimina las que se han cerrado.
        NodeConnection maneja internamente los heartbeats y desconecta automáticamente
        si no recibe heartbeats, por lo que solo necesitamos verificar is_connected().
        """
        dead_nodes = []
        
        # 1. Verificar jefe (si soy subordinado)
        if not self.i_am_boss and self.my_boss_profile.connection:
            if not self.my_boss_profile.connection.is_connected():
                boss_ip = self.my_boss_profile.connection.ip
                logging.warning(f"⚠️ Jefe {self.my_boss_profile.connection.node_id} desconectado")
                logging.warning("🗳️ Iniciando elecciones para encontrar nuevo jefe...")
                
                # Desconectar del jefe muerto
                self.my_boss_profile.connection.disconnect()
                self.my_boss_profile.clear_connection()
                
                # Eliminar de known_nodes
                self.remove_node_from_registry(self.node_type, boss_ip)
                
                # Iniciar proceso de elección
                threading.Thread(target=self.call_elections, daemon=True).start()
        
        # 2. Verificar subordinados (si soy jefe)
        if self.i_am_boss:
            # Primero identificar y eliminar subordinados muertos con el lock
            nodes_to_reassign = []
            with self.subordinates_lock:
                if self.subordinates:
                    logging.debug(f"🔍 Verificando {len(self.subordinates)} subordinados...")
                    for node_id, conn in list(self.subordinates.items()):
                        is_conn = conn.is_connected()
                        time_since = conn.get_time_since_last_heartbeat()
                        logging.debug(f"   - {node_id}: connected={is_conn}, último heartbeat hace {time_since:.1f}s")
                        if not is_conn:
                            logging.warning(f"⚠️ Subordinado {node_id} desconectado (último heartbeat hace {time_since:.1f}s)")
                            dead_nodes.append(node_id)
                    
                    # Eliminar subordinados muertos
                    for node_id in dead_nodes:
                        conn = self.subordinates.get(node_id)
                        if conn:
                            logging.info(f"Desconectando subordinado muerto: {node_id}")
                            conn.disconnect()
                            nodes_to_reassign.append(node_id)
                            
                            del self.subordinates[node_id]
                            
                            # También remover de known_nodes
                            ip = conn.ip
                            if ip in self.nodes_cache.get(self.node_type, {}):
                                del self.nodes_cache[self.node_type][ip]
                                logging.info(f"Nodo {ip} eliminado de nodos conocidos")
                    
                    if dead_nodes:
                        logging.info(f"Limpieza completada: {len(dead_nodes)} nodos eliminados")
                        logging.info(f"Subordinados activos: {len(self.subordinates)}")
            
            # Reasignar tareas FUERA del lock para evitar deadlock
            for node_id in nodes_to_reassign:
                self.reassign_tasks_from_subordinate(node_id)
        
        # 3. Verificar conexiones con otros jefes (inter-tipo: scrapper↔db, scrapper↔router, etc.)
        # Snapshot fuera del lock para no bloquear demasiado tiempo
        with self.bosses_connections_lock:
            snapshot = list(self.bosses_connections.items())
        
        for node_type, conn in snapshot:
            if conn and not conn.is_connected():
                boss_ip = conn.ip
                boss_port = conn.port
                
                with self.bosses_connections_lock:
                    # Solo actuar si la conexión en el dict sigue siendo la misma (evitar race con _connect_to_external_boss)
                    if self.bosses_connections.get(node_type) is not conn:
                        continue  # Ya fue reemplazada, ignorar
                    logging.warning(f"Jefe {node_type} desconectado")
                    conn.disconnect()
                    del self.bosses_connections[node_type]
                
                logging.info(f"Conexión con jefe de {node_type} cerrada. Reintentando en 3s...")

                # Reintentar conexión con delay para resolver race condition de
                # conexión mutua simultánea al inicio o tras reunificación.
                def _retry_external_boss(nt=node_type, bip=boss_ip, bport=boss_port):
                    time.sleep(3)
                    if self.running and nt not in self.bosses_connections:
                        logging.info(f"🔄 Reintentando conexión con jefe externo {nt} ({bip}:{bport})...")
                        self._connect_to_external_boss(nt, bip, bport)

                threading.Thread(target=_retry_external_boss, daemon=True).start()
                        
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
    
    def _on_subordinate_inherited_from_conflict(self, subordinate_node_id):
        """
        Hook que se llama cuando se hereda un subordinado después de un conflicto entre jefes.
        Las subclases pueden sobrescribir para realizar acciones específicas.
        
        Args:
            subordinate_node_id (str): ID del subordinado heredado
        """
        logging.debug(f"Subordinado heredado en conflicto: {subordinate_node_id}")
        # Implementación base: no hace nada
        pass
        
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
        with self.subordinates_lock:
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
            logging.info(f"Nodos {node_alias} descubiertos: {discovered_count}")
            logging.info(f"Mi IP: {self.ip}")
            logging.info(f"IPs descubiertas: {[ip for ip in discovered_ips if ip != self.ip]}")
            
            return [ip for ip in discovered_ips if ip != self.ip]
            
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
            
            message = json.loads(message_bytes.decode())
            msg_type = message.get('type')
            
            # Ignorar heartbeats en conexiones temporales (se manejan en NodeConnection)
            if msg_type == MessageProtocol.MESSAGE_TYPES['HEARTBEAT']:
                logging.debug(f"Heartbeat recibido de {client_ip} en conexión temporal (ignorado)")
                return  # No cerrar el socket, simplemente retornar
            
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
            logging.error(f"Error manejando conexión entrante de {client_ip}: {e}", exc_info=True)
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
        
        # Hacer una copia para evitar "dictionary changed size during iteration"
        discovered_copy = dict(discovered)
        
        for ip, info in discovered_copy.items():
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
                logging.debug(f"Estableciendo conexión persistente con jefe...")
                time.sleep(0.3)  # Pequeña pausa para que el jefe registre
                
                self.connect_to_boss(ip)
                
                # El hilo de heartbeat ya se inicia en connect_to_boss()
                if self.boss_connection and self.boss_connection.is_connected():
                    logging.info("Conexión con jefe establecida")
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
    
    def stop_boss_tasks(self):
        """
        Detiene tareas específicas del jefe cuando se cede el rol.
        Las clases hijas deben sobrescribir este método para detener sus tareas específicas.
        
        Ejemplo de uso en subclases:
        - Router: Detener threads de distribución de tareas
        - Scrapper: Detener asignación de tareas de scraping
        - BD: Detener consolidación y balanceo de réplicas
        """
        logging.info("Deteniendo tareas de jefe (implementación base - sin tareas)")
        # Implementación base: no hace nada
        # Las subclases deben sobrescribir este método
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
        
        # Filtrar nodos con IP mayor que la mía (comparación numérica)
        higher_ip_nodes = []
        for ip, info in known_nodes_of_my_type.items():
            if compare_ips(ip, self.ip) > 0:
                higher_ip_nodes.append((ip, info["port"]))
        
        if not higher_ip_nodes:
            logging.info(f"No hay nodos con IP mayor que {self.ip}. Me autoproclamo jefe.")
            self._become_boss()
            return
        
        # Ordenar de mayor a menor IP (para encontrar al jefe más rápido)
        higher_ip_nodes.sort(key=lambda x: tuple(int(p) for p in x[0].split('.')), reverse=True)
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
        
        # Limpiar subordinados antiguos (heredados de replicación del jefe anterior)
        # Esto es crítico en caso de partición de red
        old_subordinates = list(self.subordinates.keys())
        if old_subordinates:
            logging.info(f"🧹 Limpiando {len(old_subordinates)} subordinados antiguos heredados...")
            for node_id in old_subordinates:
                conn = self.subordinates[node_id]
                
                # Reasignar tareas antes de desconectar
                reassigned = self.reassign_tasks_from_subordinate(node_id)
                
                conn.disconnect()
                logging.info(f"  ✓ Subordinado antiguo {node_id} desconectado")
            
            # Limpiar diccionario
            self.subordinates.clear()
            logging.info("✅ Limpieza de subordinados completada")
            
            # Esperar un tiempo para que todos los nodos procesen la desconexión del jefe anterior
            time.sleep(2)
        
        # Limpiar información de jefes externos heredada del jefe anterior
        # Esta info es obsoleta - como nuevo jefe descubriré a los jefes externos actuales
        if self.external_bosses_cache:
            old_cache = list(self.external_bosses_cache.keys())
            logging.info(f"🧹 Limpiando cache de {len(old_cache)} jefes externos antiguos: {old_cache}")
            self.external_bosses_cache.clear()
        
        # Cerrar conexiones con jefes externos antiguos
        if self.bosses_connections:
            old_bosses = list(self.bosses_connections.keys())
            logging.info(f"🧹 Desconectando de {len(old_bosses)} jefes externos antiguos: {old_bosses}")
            for boss_type, conn in list(self.bosses_connections.items()):
                if conn:
                    try:
                        conn.disconnect()
                        logging.info(f"  ✓ Desconectado de jefe externo {boss_type}")
                    except Exception as e:
                        logging.warning(f"  ⚠ Error desconectando de {boss_type}: {e}")
            self.bosses_connections.clear()
            logging.info("✅ Limpieza de jefes externos completada")
        
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
        failed_ips = []  # Nodos que no respondieron
        
        for ip in all_known_ips:
            if ip == self.ip:
                continue
            
            port = self.nodes_cache.get(self.node_type, {}).get(ip, {}).get("port", self.port)
            
            # Intentar agregar como subordinado
            if self.add_subordinate(ip):
                connected_count += 1
                logging.info(f"✓ Subordinado {ip} conectado exitosamente")
            else:
                logging.warning(f"✗ No se pudo conectar con {ip} - será eliminado del registro")
                failed_ips.append(ip)
        
        # Limpiar nodos inalcanzables del cache
        # Esto es crítico en particiones de red para eliminar nodos de la otra partición
        if failed_ips:
            logging.info(f"🧹 Eliminando {len(failed_ips)} nodos inalcanzables del registro...")
            for ip in failed_ips:
                self.remove_node_from_registry(self.node_type, ip)
                logging.info(f"  ✓ {ip} eliminado del nodes_cache")
        
        logging.info(f"=== JEFATURA ESTABLECIDA: {connected_count}/{len(all_known_ips)} subordinados conectados ===")
        
        # Iniciar tareas de jefe (si aplica)
        self.start_boss_tasks() # thread?

    def start(self):
        '''Inicia el nodo (escucha, heartbeat, etc.)'''
        
        self.running = True
        logging.info(f"Iniciando nodo {self.node_id} (tipo: {self.node_type})")
        
        # 1. Descubrir otros nodos del mismo node_type
        logging.info(f"Descubriendo nodos {self.node_type} en la red...")
        discovered_ips = self.discover_nodes(self.node_type, self.port)
        
        if not discovered_ips:
            logging.info(f"No se encontraron otros nodos {self.node_type}. Asumiendo rol de jefe.")
            self.i_am_boss = True
        else:
            logging.info(f"Descubiertos {len(discovered_ips)} nodos {self.node_type}: {discovered_ips}")
        
        # 2. Iniciar socket de escucha
        logging.info("Iniciando socket de escucha...")
        self.start_listening()
        
        # 3. Iniciar monitor de heartbeats
        # logging.info("Iniciando monitor de heartbeats...")
        # self.heartbeat_monitor_thread = threading.Thread(
        #     target=self._heartbeat_monitor_loop,
        #     name="HeartbeatMonitor",
        #     daemon=True
        # )
        # self.heartbeat_monitor_thread.start()
        
        # 4. Broadcast de identificación (todos me registran, solo jefe responde)
        if discovered_ips:
            logging.info(f"Enviando identificación a todos los nodos {self.node_type}...")
            boss_found = self.broadcast_identification(self.node_type)
            
            if not boss_found:
                logging.warning("No se encontró jefe activo. Iniciando elecciones...")
                self.call_elections()
        
        # 5. Comportamiento según rol
        if self.i_am_boss:
            logging.info(f"🔶 Soy el JEFE de nodos {self.node_type}")
            # Iniciar hilo de asignación de tareas
            self.start_boss_tasks() # thread?
            # TODO: Conectar con jefes de BD y Router si es necesario
            # self.discover_nodes("bd", self.bd_port)
            # self.connect_to_discovered_nodes("bd")

            # hilo para que se reconecte con el jefe bd
            if self.node_type == 'scrapper' and self.i_am_boss:
                threading.Thread(
                    target=self.mantener_conex_con_bd_boss,
                    daemon=True,
                    name='mantener-conex-con-bd-boss'
                ).start()

        else:
            logging.info(f"✓ Soy subordinado {self.node_type}, conectado al jefe en {self.boss_connection.ip if self.boss_connection else 'desconocido'}")
        
        # Iniciar thread de monitoreo de conexiones
        self.connection_monitor_stop_event.clear()
        self.connection_monitor_thread = threading.Thread(
            target=self._connection_monitor_loop,
            daemon=True,
            name=f"ConnectionMonitor-{self.node_id}"
        )
        self.connection_monitor_thread.start()
        logging.info("Thread de monitoreo de conexiones iniciado")
        
        logging.info("Nodo iniciado correctamente.")
        
        # Mantener vivo
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Deteniendo nodo...")
            self.stop()
            
    def mantener_conex_con_bd_boss(self):
        while(self.running):
            if 'bd' in self.bosses_connections.keys():
                # logging.debug("ya tengo conexion registrada con jefe bd")
                continue
            else:
                logging.debug("no tengo conexion registrada con jefe bd")
            
            if 'bd' in self.external_bosses_cache.keys():
                # logging.debug(f'existe info de jefe bd en cache: bd:{self.external_bosses_cache["bd"]}')
                if 'bd' not in self.bosses_connections.keys():
                    # conectar con jefe bd
                    conn = NodeConnection(
                        'bd',
                        self.external_bosses_cache['bd']['ip'],
                        self.external_bosses_cache['bd']['port'],
                        on_message_callback=self._handle_message_from_node,
                        sender_node_type=self.node_type,
                        sender_id=self.node_id
                    )

                    if conn.connect():
                        self.bosses_connections['bd'] = conn
                        
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
                        
                        # Replicar info a subordinados
                        self.replicate_external_bosses_info()
                        
                        logging.info(f"✓ (reset) Conexión con jefe externo {'bd'} ({self.external_bosses_cache['bd']['ip']}:{self.external_bosses_cache['bd']['port']}) establecida")
                    else:
                        logging.error(f"✗ (reset) No se pudo conectar con jefe externo {'bd'} en {self.external_bosses_cache['bd']['ip']}:{self.external_bosses_cache['bd']['port']}")
                else:
                    # logging.debug("ya tengo conexion con jefe bd")
                    pass
            else:
                logging.debug('no existe cache actual para bd')
            
            time.sleep(5)

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