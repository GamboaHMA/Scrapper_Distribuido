"""
Router Node - Nodo que maneja peticiones de clientes y coordina con BD y Scrapper

El router:
- Recibe peticiones de clientes para scrapping de URLs
- Consulta al jefe BD si la información ya existe
- Si no existe o BD no disponible → delega tarea al jefe Scrapper
- Mantiene conexiones persistentes con jefes BD y Scrapper
- Cola de tareas pendientes
"""

import sys
import os
import logging
import threading
import time
from datetime import datetime
from queue import Queue, Empty

# Agregar directorio padre al path para imports absolutos
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from base_node.node import Node
from base_node.utils import MessageProtocol, NodeConnection, BossProfile


class TaskQueue:
    """
    Gestiona una cola de tareas de routing pendientes.
    """
    def __init__(self):
        self.pending_tasks = Queue()
        self.in_progress = {}  # {task_id: {client_info, url, status, timestamp}}
        self.completed = {}    # {task_id: {result, timestamp}}
        self.lock = threading.Lock()

    
    def add_task(self, task_id, client_info, url):
        """Agregar tarea a la cola"""
        with self.lock:
            task = {
                'task_id': task_id,
                'client_info': client_info,
                'url': url,
                'timestamp': datetime.now()
            }
            self.pending_tasks.put(task)
            logging.info(f"Tarea {task_id} agregada a cola (URL: {url})")
    
    def get_next_task(self, timeout=1):
        """Obtener siguiente tarea pendiente"""
        try:
            return self.pending_tasks.get(timeout=timeout)
        except Empty:
            return None
    
    def mark_in_progress(self, task_id, url, client_info):
        """Marcar tarea como en progreso"""
        with self.lock:
            self.in_progress[task_id] = {
                'url': url,
                'client_info': client_info,
                'status': 'querying_bd',
                'timestamp': datetime.now()
            }
    
    def mark_completed(self, task_id, result):
        """Marcar tarea como completada"""
        with self.lock:
            if task_id in self.in_progress:
                del self.in_progress[task_id]
            self.completed[task_id] = {
                'result': result,
                'timestamp': datetime.now()
            }
    
    def get_pending_count(self):
        """Obtener cantidad de tareas pendientes"""
        return self.pending_tasks.qsize()

# Por defecto INFO, pero se puede cambiar con LOG_LEVEL=DEBUG
log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class RouterNode(Node):
    """
    Nodo Router que coordina peticiones entre clientes, BD y Scrapper.
    
    Responsabilidades:
    - Recibir peticiones de clientes
    - Consultar BD por información existente
    - Delegar scrapping a Scrapper si necesario
    - Responder a clientes
    """
    
    def __init__(self, scrapper_port=8080, bd_port=9090):
        """
        Inicializa el nodo Router.
        
        Args:
            node_type (str): Tipo de nodo ('router')
            port (int): Puerto para escuchar conexiones
            log_level (str): Nivel de logging
        """
        super().__init__(node_type='router')
        
        # Cola de tareas
        self.task_queue = TaskQueue()
        
        # Perfiles de jefes externos
        self.external_bosses = {
            'bd': BossProfile('bd', bd_port),
            'scrapper': BossProfile('scrapper', scrapper_port)
        }
        
        # Clientes conectados de forma persistente
        # {client_id: NodeConnection}
        self.connected_clients = {}
        self.clients_lock = threading.Lock()
        
        # Registrar handlers específicos del router
        self._register_router_handlers()
    
    def _register_router_handlers(self):
        """Registrar handlers para mensajes específicos del router"""
        # Handler para peticiones de clientes (conexión persistente)
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['CLIENT_REQUEST'],
            self._handle_client_request_persistent
        )
        
        # Handler para solicitudes de estado (conexión persistente)
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['STATUS_REQUEST'],
            self._handle_status_request_persistent
        )
        
        # Handlers para respuestas de BD y Scrapper (conexión persistente)
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['BD_QUERY_RESPONSE'],
            self._handle_bd_response
        )
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['TASK_RESULT'],
            self._handle_scrapper_result
        )
        
        # Handlers para visualización de BD
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['LIST_TABLES'],
            self._handle_list_tables_request
        )
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['GET_TABLE_DATA'],
            self._handle_get_table_data_request
        )
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['LIST_TABLES_RESPONSE'],
            self._handle_list_tables_response
        )
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['GET_TABLE_DATA_RESPONSE'],
            self._handle_get_table_data_response
        )
        
        # Handler temporal para BD_QUERY_RESPONSE desde subordinados BD (socket temporal)
        self.add_temporary_message_handler(
            MessageProtocol.MESSAGE_TYPES['BD_QUERY_RESPONSE'],
            self._handle_bd_response_temporary
        )
        
        logging.debug("Handlers del router registrados")
    
    def _handle_identification_incoming(self, sock, client_ip, message):
        """
        Sobrescribe handler de identificación entrante para interceptar clientes.
        
        Args:
            sock: Socket de la conexión
            client_ip: IP del cliente
            message: Mensaje de identificación
        """
        data = message.get('data', {})
        sender_node_type = message.get('node_type', self.node_type)
        is_temporary = data.get('is_temporary', False)
        
        # Si es un cliente, manejarlo de manera especial
        if sender_node_type == 'client' and not is_temporary:
            sender_id = message.get('sender_id', 'unknown-client')
            logging.info(f"Cliente {sender_id} conectando desde {client_ip}")
            
            # Crear NodeConnection para el cliente con callback para procesar mensajes
            node_connection = NodeConnection(
                node_type='client',  # Tipo del nodo remoto
                ip=client_ip,
                port=0,  # Clientes no tienen puerto de escucha
                on_message_callback=self._handle_message_from_node,  # Usar el mismo callback que subordinados
                sender_node_type=self.node_type,
                sender_id=self.node_id
            )
            
            # Conectar usando el socket existente
            if not node_connection.connect(existing_socket=sock):
                logging.error(f"No se pudo establecer conexión persistente con cliente {sender_id}")
                sock.close()
                return
            
            # Agregar a connected_clients
            with self.clients_lock:
                self.connected_clients[sender_id] = node_connection
                logging.info(f"✓ Cliente {sender_id} conectado persistentemente")
            
            # Responder con confirmación de identificación
            response = self._create_message(
                MessageProtocol.MESSAGE_TYPES['IDENTIFICATION'],
                {
                    'is_boss': self.i_am_boss,
                    'ip': self.ip,
                    'port': self.port,
                    'node_type': self.node_type
                }
            )
            node_connection.send_message(response)
            
            # Iniciar heartbeat monitoring para el cliente
            threading.Thread(
                target=self._heartbeat_loop,
                args=(node_connection,),
                daemon=True
            ).start()
            
            return  # No continuar con el procesamiento normal
        
        # Si es un jefe externo (BD o Scrapper) conectándose
        is_boss = data.get('is_boss', False)
        if self.i_am_boss and is_boss and sender_node_type in ['bd', 'scrapper']:
            logging.info(f"Jefe externo {sender_node_type} {client_ip} estableciendo conexión persistente")
            
            # Obtener el puerto del mensaje
            sender_port = data.get('port', self.port)
            
            # Crear NodeConnection para el jefe externo
            node_connection = NodeConnection(
                node_type=sender_node_type,
                ip=client_ip,
                port=sender_port,
                on_message_callback=self._handle_message_from_node,
                sender_node_type=self.node_type,
                sender_id=self.node_id
            )
            
            # Conectar usando el socket existente
            if not node_connection.connect(existing_socket=sock):
                logging.error(f"No se pudo establecer conexión persistente con jefe {sender_node_type}")
                sock.close()
                return
            
            # Actualizar BossProfile
            if sender_node_type in self.external_bosses:
                boss_profile = self.external_bosses[sender_node_type]
                if not boss_profile.is_connected():
                    boss_profile.set_connection(node_connection)
                    logging.info(f"✓ Jefe externo {sender_node_type} conectado vía conexión entrante")
                else:
                    logging.debug(f"Jefe externo {sender_node_type} ya tiene conexión activa")
            
            # Responder con confirmación
            response = self._create_message(
                MessageProtocol.MESSAGE_TYPES['IDENTIFICATION'],
                {
                    'is_boss': self.i_am_boss,
                    'ip': self.ip,
                    'port': self.port,
                    'node_type': self.node_type
                }
            )
            node_connection.send_message(response)
            
            # Iniciar heartbeats
            threading.Thread(
                target=self._heartbeat_loop,
                args=(node_connection,),
                daemon=True
            ).start()
            
            return  # No continuar con el procesamiento normal
        
        # Para otros casos (subordinados routers), delegar al handler del padre
        super()._handle_identification_incoming(sock, client_ip, message)
    
    def _handle_identification(self, node_connection, message_dict):
        """
        Sobrescribe handler de identificación para manejar clientes (conexiones ya establecidas).
        
        Args:
            node_connection: NodeConnection
            message_dict: Mensaje de identificación
        """
        sender_node_type = message_dict.get('node_type', 'unknown')
        data = message_dict.get('data', {})
        is_boss = data.get('is_boss', False)
        
        # Si es un cliente, ya debería estar registrado por _handle_identification_incoming
        if sender_node_type == 'client':
            sender_id = message_dict.get('sender_id', 'unknown-client')
            logging.debug(f"Mensaje de identificación de cliente {sender_id} (ya registrado)")
            
            # Responder con confirmación
            response = self._create_message(
                MessageProtocol.MESSAGE_TYPES['IDENTIFICATION'],
                {
                    'is_boss': self.i_am_boss,
                    'ip': self.ip,
                    'port': self.port,
                    'node_type': self.node_type
                }
            )
            node_connection.send_message(response)
        
        # Si es un jefe externo (BD o Scrapper) identificándose
        elif sender_node_type in ['bd', 'scrapper'] and is_boss:
            # Actualizar BossProfile si existe
            if sender_node_type in self.external_bosses:
                boss_profile = self.external_bosses[sender_node_type]
                
                # Si aún no tenemos conexión, usar esta
                if not boss_profile.is_connected():
                    boss_profile.set_connection(node_connection)
                    logging.info(f"✓ Jefe externo {sender_node_type} conectado vía identificación entrante")
                    logging.info(f"  BossProfile actualizado: disponible={boss_profile.available}, is_connected={boss_profile.is_connected()}")
                else:
                    logging.debug(f"Jefe externo {sender_node_type} ya tiene conexión activa")
            
            # NO delegar al padre - Router usa external_bosses (BossProfile), no bosses_connections
        
        else:
            # Delegar al handler del padre para nodos del sistema
            super()._handle_identification(node_connection, message_dict)
    
    def _handle_client_request_persistent(self, node_connection, message_dict):
        """
        Handler para peticiones de clientes (conexión persistente).
        
        Args:
            node_connection: NodeConnection del cliente
            message_dict: Mensaje con la petición
        """
        data = message_dict.get('data', {})
        task_id = data.get('task_id')
        url = data.get('url')
        
        if not task_id or not url:
            logging.warning(f"Petición inválida de {node_connection.node_id}: falta task_id o url")
            return
        
        logging.info(f"Petición recibida de {node_connection.node_id}: task_id={task_id}, url={url}")
        
        # Guardar información del cliente para responder después
        client_info = {
            'connection': node_connection  # Conexión persistente
        }
        
        # Agregar tarea a la cola
        self.task_queue.add_task(task_id, client_info, url)
    
    # TODO: COORDINAR CON DATA BASE
    def _handle_bd_response(self, node_connection, message_dict):
        """
        Handler para respuestas de BD sobre consultas de URLs.
        
        Args:
            node_connection: Conexión con el nodo BD
            message_dict: Mensaje completo con la respuesta
        """
        data = message_dict.get('data', {})
        task_id = data.get('task_id')
        found = data.get('found', False)
        result = data.get('result')
        
        logging.info(f"Respuesta de BD para task {task_id}: found={found}")
        
        if found and result:
            # La información ya existe en BD, responder al cliente
            logging.info(f"BD tiene la info para task {task_id}, respondiendo al cliente")
            self._respond_to_client(task_id, result)
        else:
            # No existe en BD, delegar a Scrapper
            logging.info(f"BD no tiene la info para task {task_id}, delegando a Scrapper")
            self._delegate_to_scrapper(task_id)

    def _handle_bd_response_temporary(self, sock, client_ip, message_dict):
        """
        Handler temporal para respuestas de BD subordinados (socket temporal).
        Procesa igual que las respuestas persistentes pero cierra el socket después.
        
        Args:
            sock: Socket de la conexión temporal
            client_ip: IP del nodo BD subordinado
            message_dict: Mensaje completo con la respuesta
        """
        try:
            logging.info(f"BD_QUERY_RESPONSE temporal recibida desde {client_ip}")
            
            data = message_dict.get('data', {})
            task_id = data.get('task_id')
            found = data.get('found', False)
            result = data.get('result')
            
            logging.info(f"Respuesta temporal de BD ({client_ip}) para task {task_id}: found={found}")
            
            if found and result:
                # La información existe en BD, responder al cliente
                logging.info(f"BD subordinado tiene la info para task {task_id}, respondiendo al cliente")
                self._respond_to_client(task_id, result)
            else:
                # No existe en BD, delegar a Scrapper
                logging.info(f"BD subordinado no tiene la info para task {task_id}, delegando a Scrapper")
                self._delegate_to_scrapper(task_id)
                
        finally:
            sock.close()
    
    def _handle_scrapper_result(self, node_connection, message_dict):
        """
        Handler para resultados de scrapping del jefe Scrapper.
        Envía el resultado al cliente que solicitó la tarea.
        
        Args:
            node_connection: Conexión con el nodo Scrapper
            message_dict: Mensaje completo del resultado
        """
        logging.debug(f"_handle_scrapper_result - mensaje recibido: {message_dict}")
        
        # Extraer el campo 'data' del mensaje
        data = message_dict.get('data', {})
        task_id = data.get('task_id')
        result = data.get('result')
        success = data.get('success', False)
        
        logging.info(f"Resultado de Scrapper recibido para task {task_id}: success={success}")
        
        if success:
            self._respond_to_client(task_id, result)
        else:
            self._respond_to_client(task_id, {'error': 'Scrapping falló', 'details': result})
    
    def _handle_status_request_persistent(self, node_connection, data):
        """
        Handler para solicitudes de estado del sistema (conexión persistente).
        
        Args:
            node_connection: Conexión con el cliente
            data: Datos de la solicitud
        """
        logging.info(f"Solicitud de estado recibida de {node_connection.node_id}")
        
        # Recopilar información del sistema
        status = {
            'router': {
                'ip': self.ip,
                'is_boss': self.i_am_boss,
                'bd_connected': self.external_bosses['bd'].is_connected(),
                'scrapper_connected': self.external_bosses['scrapper'].is_connected(),
            },
            'tasks': {
                'pending': self.task_queue.get_pending_count(),
                'in_progress': len(self.task_queue.in_progress),
                'completed': len(self.task_queue.completed)
            },
            'timestamp': datetime.now().isoformat()
        }
        
        # Crear respuesta
        response = self._create_message(
            MessageProtocol.MESSAGE_TYPES['STATUS_RESPONSE'],
            status
        )
        
        try:
            # Enviar respuesta a través de la conexión persistente
            node_connection.send_message(response)
            logging.info(f"Estado enviado al cliente {node_connection.node_id}")
        except Exception as e:
            logging.error(f"Error enviando estado al cliente: {e}")
    
    def _respond_to_client(self, task_id, result):
        """
        Responde al cliente con el resultado de su petición.
        
        Args:
            task_id: ID de la tarea
            result: Resultado del scrapping
        """
        with self.task_queue.lock:
            if task_id not in self.task_queue.in_progress:
                logging.warning(f"Task {task_id} no encontrada en in_progress")
                return
            
            task_info = self.task_queue.in_progress[task_id]
            client_info = task_info['client_info']
            client_connection = client_info.get('connection')
            
            if not client_connection:
                logging.error(f"No hay conexión para responder task {task_id}")
                return
        
        # Crear respuesta
        response = self._create_message(
            MessageProtocol.MESSAGE_TYPES['TASK_RESULT'],
            {
                'task_id': task_id,
                'result': result,
                'success': True
            }
        )
        
        try:
            # Enviar respuesta a través de la conexión persistente
            client_connection.send_message(response)
            logging.info(f"Respuesta enviada al cliente {client_connection.node_id} para task {task_id}")
            
        except Exception as e:
            logging.error(f"Error enviando respuesta al cliente: {e}")
        
        # Marcar tarea como completada
        self.task_queue.mark_completed(task_id, result)
    
    def _delegate_to_scrapper(self, task_id):
        """
        Delega una tarea de scrapping al jefe Scrapper.
        
        Args:
            task_id: ID de la tarea
        """
        with self.task_queue.lock:
            if task_id not in self.task_queue.in_progress:
                logging.warning(f"Task {task_id} no encontrada para delegar")
                return
            
            task_info = self.task_queue.in_progress[task_id]
            url = task_info['url']
        
        scrapper_boss = self.external_bosses['scrapper']
        
        if not scrapper_boss.is_connected():
            logging.error(f"Scrapper no disponible, no se puede procesar task {task_id}")
            self._respond_to_client(task_id, {'error': 'Servicio de scrapping no disponible'})
            return
        
        # Enviar tarea al jefe Scrapper
        message = self._create_message(
            MessageProtocol.MESSAGE_TYPES['NEW_TASK'],
            {
                'task_id': task_id,
                'task_data': {
                    'url': url,
                    'source': 'router'
                }
            }
        )
        
        scrapper_boss.connection.send_message(message)
        logging.info(f"Tarea {task_id} delegada al jefe Scrapper")
    
    def _process_tasks_loop(self):
        """Loop principal para procesar tareas de la cola"""
        logging.info("Iniciando loop de procesamiento de tareas")
        
        while self.running:
            try:
                # Obtener siguiente tarea
                task = self.task_queue.get_next_task(timeout=1)
                
                if not task:
                    continue
                
                task_id = task['task_id']
                url = task['url']
                client_info = task['client_info']
                
                # Marcar como en progreso
                self.task_queue.mark_in_progress(task_id, url, client_info)
                
                # Consultar a BD primero
                bd_boss = self.external_bosses['bd']
                if bd_boss.is_connected():
                    self._query_bd(task_id, url)
                else:
                    # BD no disponible, ir directo a Scrapper
                    logging.warning(f"BD no disponible, delegando task {task_id} a Scrapper")
                    self._delegate_to_scrapper(task_id)
                
            except Exception as e:
                logging.error(f"Error procesando tarea: {e}")
                time.sleep(1)
    
    def _query_bd(self, task_id, url):
        """
        Consulta al jefe BD si existe información de una URL.
        
        Args:
            task_id: ID de la tarea
            url: URL a consultar
        """
        bd_boss = self.external_bosses['bd']
        if not bd_boss.is_connected():
            logging.warning(f"No hay conexión con BD para task {task_id}")
            self._delegate_to_scrapper(task_id)
            return
        
        message = self._create_message(
            MessageProtocol.MESSAGE_TYPES['BD_QUERY'],
            {
                'task_id': task_id,
                'url': url
            }
        )
        
        bd_boss.connection.send_message(message)
        logging.info(f"✓ BD_QUERY enviada a BD para task {task_id}, URL: {url}")
    
    def _connect_to_external_bosses(self):
        """Conecta con los jefes de BD y Scrapper"""
        logging.info("Conectando con jefes externos (BD y Scrapper)...")
        
        for node_type in self.external_bosses.keys():
            logging.info(f"Iniciando thread de búsqueda periódica para jefe {node_type}")
            threading.Thread(
                target=self._periodic_boss_search,
                args=(node_type,),
                daemon=True,
                name=f"boss-search-{node_type}"
            ).start()
        # # Iniciar búsqueda periódica para BD
        # threading.Thread(
        #     target=self._periodic_boss_search,
        #     args=('bd',),
        #     daemon=True
        # ).start()
        
        # # Iniciar búsqueda periódica para Scrapper
        # threading.Thread(
        #     target=self._periodic_boss_search,
        #     args=('scrapper',),
        #     daemon=True
        # ).start()
    
    def _periodic_boss_search(self, node_type):
        """
        Busca periódicamente al jefe de un tipo de nodo hasta encontrarlo.
        Monitorea la conexión y reinicia búsqueda si se desconecta.
        
        Args:
            node_type: Tipo de nodo a buscar ('bd' o 'scrapper')
        """
        retry_interval = 5  # segundos entre intentos de búsqueda
        wait_after_disconnect = 15  # segundos de espera tras desconexión (para dar tiempo a nuevo jefe)
        boss_profile = self.external_bosses[node_type]
        
        logging.info(f"🔍 Iniciando búsqueda periódica del jefe {node_type}...")
        
        while self.running:
            # Si ya estamos conectados, monitorear la conexión
            if boss_profile.is_connected():
                logging.debug(f"Jefe {node_type} conectado, monitoreando...")
                
                # Esperar mientras esté conectado
                while self.running and boss_profile.is_connected():
                    time.sleep(5)  # Verificar cada 5 segundos
                
                # Se desconectó
                if self.running:
                    logging.warning(f"⚠️ Jefe {node_type} se desconectó. Esperando {wait_after_disconnect}s por nuevo jefe...")
                    
                    # Esperar un tiempo para ver si otro nodo se convierte en jefe y se conecta
                    time.sleep(wait_after_disconnect)
                    
                    # Si después de esperar aún no hay conexión, reiniciar búsqueda
                    if not boss_profile.is_connected():
                        logging.info(f"⟳ No apareció nuevo jefe {node_type}, reiniciando búsqueda activa...")
                    else:
                        logging.info(f"✓ Nuevo jefe {node_type} se conectó durante la espera")
                        continue
            
            # Búsqueda activa: intentar descubrir nodos
            if not boss_profile.is_connected():
                node_ips = self.discover_nodes(node_type, boss_profile.port)
                
                if node_ips:
                    # Buscar el jefe en la lista
                    boss_ip = self._find_boss_in_list(node_ips, node_type)
                    
                    if boss_ip:
                        logging.info(f"Jefe {node_type} encontrado en {boss_ip}")
                        self._connect_to_boss(node_type, boss_ip)
                        
                        # Verificar que la conexión fue exitosa
                        if boss_profile.is_connected():
                            logging.info(f"✓ Conexión con jefe {node_type} establecida")
                            continue  # Volver al modo monitor
                    else:
                        logging.debug(f"Nodos {node_type} encontrados pero ninguno es jefe")
                else:
                    logging.debug(f"No se encontraron nodos {node_type} en el DNS")
                
                # Esperar antes del siguiente intento
                time.sleep(retry_interval)
        
        logging.info(f"Búsqueda periódica de jefe {node_type} finalizada")
    
    def _find_boss_in_list(self, ip_list, node_type):
        """
        Encuentra el jefe en una lista de IPs consultando temporalmente.
        
        Args:
            ip_list: Lista de IPs a consultar
            node_type: Tipo de nodo ('bd', 'scrapper')
        
        Returns:
            str: IP del jefe o None
        """
        boss_profile = self.external_bosses[node_type]
        
        for ip in ip_list:
            if ip == self.ip:
                continue
            
            # Enviar identificación temporal
            msg = self._create_message(
                MessageProtocol.MESSAGE_TYPES['IDENTIFICATION'],
                {'is_temporary': True}
            )
            
            response = self.send_temporary_message(
                ip, 
                boss_profile.port, 
                msg, 
                expect_response=True,
                # timeout=5.0,
                node_type=node_type
            )
            if response:
                is_boss = response.get('data', {}).get('is_boss', False)
                if is_boss:
                    return ip
        
        return None
    
    def _connect_to_boss(self, node_type, boss_ip):
        """
        Conecta con el jefe de un tipo de nodo específico.
        
        Args:
            node_type: Tipo de nodo ('bd' o 'scrapper')
            boss_ip: IP del jefe
        """
        boss_profile = self.external_bosses[node_type]
        
        # Verificar si ya existe conexión (is_connected ya tiene lock interno)
        if boss_profile.is_connected():
            logging.warning(f"Ya existe conexión con jefe {node_type}")
            return
        
        # Crear nueva conexión
        new_connection = NodeConnection(
            node_type,
            boss_ip,
            boss_profile.port,
            on_message_callback=self._handle_message_from_node,
            sender_node_type=self.node_type,
            sender_id=self.node_id
        )
        
        if new_connection.connect():
            logging.info(f"Conectado con jefe {node_type} en {boss_ip}")
            
            try:
                # Enviar identificación inicial (NO temporal, es conexión persistente)
                identification = self._create_message(
                    MessageProtocol.MESSAGE_TYPES['IDENTIFICATION'],
                    {
                        'ip': self.ip,
                        'port': self.port,
                        'is_boss': self.i_am_boss,
                        'is_temporary': False
                    }
                )
                new_connection.send_message(identification)
                
                # Actualizar perfil (set_connection ya tiene lock interno)
                boss_profile.set_connection(new_connection)
                logging.info(f"✓ Conexión con jefe {node_type} establecida exitosamente")
                
                # Iniciar heartbeats
                threading.Thread(
                    target=self._heartbeat_loop,
                    args=(new_connection,),
                    daemon=True
                ).start()
            except Exception as e:
                logging.error(f"Error al configurar conexión con jefe {node_type}: {e}")
                import traceback
                traceback.print_exc()
                boss_profile.clear_connection()
        else:
            logging.error(f"No se pudo conectar con jefe {node_type} en {boss_ip}")
            boss_profile.clear_connection()
    
    def start_boss_tasks(self):
        """
        Tareas específicas del jefe Router.
        Override del método base.
        """
        logging.info("=== INICIANDO TAREAS DEL JEFE ROUTER ===")
        logging.info(f"Soy el router jefe: {self.i_am_boss}")
        logging.info(f"external_bosses keys: {list(self.external_bosses.keys())}")
        
        # Conectar con jefes externos
        self._connect_to_external_bosses()
        
        # Iniciar loop de procesamiento de tareas
        threading.Thread(
            target=self._process_tasks_loop,
            daemon=True
        ).start()
        
        logging.info("✓ Jefe Router operativo")

    def _handle_list_tables_request(self, node_connection, message):
        """
        Handler para petición de lista de tablas desde cliente.
        Reenvía la petición al jefe BD.
        
        Args:
            node_connection: Conexión con el cliente
            message: Mensaje con la solicitud
        """
        logging.info(f"Solicitud de lista de tablas recibida de {node_connection.node_id}")
        
        # Verificar conexión con BD
        bd_boss = self.external_bosses.get('bd')
        if not bd_boss or not bd_boss.is_connected():
            error_response = {
                'type': MessageProtocol.MESSAGE_TYPES['LIST_TABLES_RESPONSE'],
                'sender_id': self.node_id,
                'timestamp': datetime.now().isoformat(),
                'data': {
                    'success': False,
                    'error': 'BD no disponible'
                }
            }
            node_connection.send_message(error_response)
            return
        
        # Guardar referencia del cliente para responder después
        request_id = f"list_tables_{datetime.now().timestamp()}"
        if not hasattr(self, '_pending_db_requests'):
            self._pending_db_requests = {}
        self._pending_db_requests[request_id] = node_connection
        
        # Reenviar petición a BD con identificador
        forward_message = {
            'type': MessageProtocol.MESSAGE_TYPES['LIST_TABLES'],
            'sender_id': self.node_id,
            'timestamp': datetime.now().isoformat(),
            'data': {
                'request_id': request_id
            }
        }
        
        # bd_boss.send_message(forward_message)
        bd_boss.connection.send_message(forward_message)
        logging.info(f"Solicitud de lista de tablas reenviada a BD jefe")

    def _handle_get_table_data_request(self, node_connection, message):
        """
        Handler para petición de datos de tabla desde cliente.
        Reenvía la petición al jefe BD.
        
        Args:
            node_connection: Conexión con el cliente
            message: Mensaje con la solicitud
        """
        data = message.get('data', {})
        table_name = data.get('table_name')
        logging.info(f"Solicitud de datos de tabla '{table_name}' recibida de {node_connection.node_id}")
        
        # Verificar conexión con BD
        bd_boss = self.external_bosses.get('bd')
        if not bd_boss or not bd_boss.is_connected():
            error_response = {
                'type': MessageProtocol.MESSAGE_TYPES['GET_TABLE_DATA_RESPONSE'],
                'sender_id': self.node_id,
                'timestamp': datetime.now().isoformat(),
                'data': {
                    'success': False,
                    'error': 'BD no disponible'
                }
            }
            node_connection.send_message(error_response)
            return
        
        # Guardar referencia del cliente para responder después
        request_id = f"table_data_{table_name}_{datetime.now().timestamp()}"
        if not hasattr(self, '_pending_db_requests'):
            self._pending_db_requests = {}
        self._pending_db_requests[request_id] = node_connection
        
        # Reenviar petición a BD con identificador
        forward_message = {
            'type': MessageProtocol.MESSAGE_TYPES['GET_TABLE_DATA'],
            'sender_id': self.node_id,
            'timestamp': datetime.now().isoformat(),
            'data': {
                'request_id': request_id,
                'table_name': data.get('table_name'),
                'page': data.get('page', 1),
                'page_size': data.get('page_size', 50)
            }
        }
        
        bd_boss.connection.send_message(forward_message)
        logging.info(f"Solicitud de datos de tabla '{table_name}' reenviada a BD jefe")

    def _handle_list_tables_response(self, node_connection, message):
        """
        Handler para respuesta de lista de tablas desde BD.
        Reenvía la respuesta al cliente que la solicitó.
        
        Args:
            node_connection: Conexión con BD
            message: Mensaje con la respuesta
        """
        data = message.get('data', {})
        request_id = data.get('request_id')
        
        if not hasattr(self, '_pending_db_requests'):
            logging.warning("No hay peticiones pendientes de BD")
            return
        
        client_connection = self._pending_db_requests.pop(request_id, None)
        if not client_connection:
            logging.warning(f"No se encontró cliente para request_id {request_id}")
            return
        
        # Reenviar respuesta al cliente
        client_connection.send_message(message)
        logging.info(f"Lista de tablas enviada a {client_connection.node_id}")

    def _handle_get_table_data_response(self, node_connection, message):
        """
        Handler para respuesta de datos de tabla desde BD.
        Reenvía la respuesta al cliente que la solicitó.
        
        Args:
            node_connection: Conexión con BD
            message: Mensaje con la respuesta
        """
        data = message.get('data', {})
        request_id = data.get('request_id')
        
        if not hasattr(self, '_pending_db_requests'):
            logging.warning("No hay peticiones pendientes de BD")
            return
        
        client_connection = self._pending_db_requests.pop(request_id, None)
        if not client_connection:
            logging.warning(f"No se encontró cliente para request_id {request_id}")
            return
        
        # Reenviar respuesta al cliente
        client_connection.send_message(message)
        table_name = data.get('table_name', 'unknown')
        logging.info(f"Datos de tabla '{table_name}' enviados a {client_connection.node_id}")


if __name__ == "__main__":
    try:
        # Crear y arrancar nodo scrapper
        router = RouterNode()
        router.start()  # Hereda el método start() de Node
        
    except KeyboardInterrupt:
        logging.info("Deteniendo nodo Router...")
        try:
            if 'router' in locals():
                router.stop()
        except Exception as e:
            logging.error(f"Error al detener nodo Router: {e}")
    except Exception as e:
        logging.error(f"Error fatal: {e}")
        import traceback
        traceback.print_exc()