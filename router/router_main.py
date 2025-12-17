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
from base_node.utils import MessageProtocol, NodeConnection


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
    
    def __init__(self, scrapper_port=8080, bd_port=7070):
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
        
        # Conexiones con jefes externos
        self.bd_port = bd_port
        self.scrapper_port = scrapper_port
         
        self.scrapper_boss_connection = None  # Conexión con jefe Scrapper
        self.bd_boss_connection = None         # Conexión con jefe BD
        
        # Estado de disponibilidad de servicios
        self.bd_available = False
        self.scrapper_available = False
        
        # Lock para operaciones de conexión
        self.external_connections_lock = threading.Lock()
        
        # Registrar handlers específicos del router
        self._register_router_handlers()
    
    def _register_router_handlers(self):
        """Registrar handlers para mensajes específicos del router"""
        # Handler para peticiones de clientes (conexión temporal)
        # self.temporary_message_handler['client_request'] = self._handle_client_request
        self.add_temporary_message_handler(
            MessageProtocol.MESSAGE_TYPES['CLIENT_REQUEST'],
            self._handle_client_request
        )
        
        # Handlers para respuestas de BD y Scrapper (conexión persistente)
        # self.persistent_message_handler['bd_query_response'] = self._handle_bd_response
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['BD_QUERY_RESPONSE'],
            self._handle_bd_response
        )
        # self.persistent_message_handler['scrapper_result'] = self.
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['SCRAPPER_RESULT'],
            self._handle_scrapper_result
        )
        
        logging.debug("Handlers del router registrados")
    
    def _handle_client_request(self, sock, client_ip, message):
        """
        Handler para peticiones de clientes (conexión temporal).
        
        Args:
            sock: Socket del cliente
            client_ip: IP del cliente
            message: Mensaje con la petición
        """
        data = message.get('data', {})
        task_id = data.get('task_id')
        url = data.get('url')
        
        if not task_id or not url:
            logging.warning(f"Petición inválida de {client_ip}: falta task_id o url")
            sock.close()
            return
        
        logging.info(f"Petición recibida de {client_ip}: task_id={task_id}, url={url}")
        
        # Guardar información del cliente para responder después
        client_info = {
            'ip': client_ip,
            'socket': sock  # Mantenemos el socket abierto para responder
        }
        
        # Agregar tarea a la cola
        self.task_queue.add_task(task_id, client_info, url)
        
        # El socket se mantiene abierto hasta que respondamos
    
    def _handle_bd_response(self, node_connection, data):
        """
        Handler para respuestas de BD sobre consultas de URLs.
        
        Args:
            node_connection: Conexión con el nodo BD
            data: Datos de la respuesta
        """
        task_id = data.get('task_id')
        found = data.get('found', False)
        result = data.get('result')
        
        logging.info(f"Respuesta de BD para task {task_id}: found={found}")
        
        if found and result:
            # La información ya existe en BD, responder al cliente
            self._respond_to_client(task_id, result)
        else:
            # No existe en BD, delegar a Scrapper
            self._delegate_to_scrapper(task_id)
    
    def _handle_scrapper_result(self, node_connection, data):
        """
        Handler para resultados de scrapping del jefe Scrapper.
        
        Args:
            node_connection: Conexión con el nodo Scrapper
            data: Datos del resultado
        """
        task_id = data.get('task_id')
        result = data.get('result')
        success = data.get('success', False)
        
        logging.info(f"Resultado de Scrapper para task {task_id}: success={success}")
        
        if success:
            self._respond_to_client(task_id, result)
        else:
            self._respond_to_client(task_id, {'error': 'Scrapping falló'})
    
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
            client_socket = client_info.get('socket')
            
            if not client_socket:
                logging.error(f"No hay socket para responder task {task_id}")
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
            # Enviar respuesta
            import json
            response_bytes = json.dumps(response).encode()
            client_socket.send(len(response_bytes).to_bytes(2, 'big'))
            client_socket.send(response_bytes)
            logging.info(f"Respuesta enviada al cliente para task {task_id}")
            
        except Exception as e:
            logging.error(f"Error enviando respuesta al cliente: {e}")
        finally:
            client_socket.close()
        
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
        
        if not self.scrapper_available or not self.scrapper_boss_connection:
            logging.error(f"Scrapper no disponible, no se puede procesar task {task_id}")
            self._respond_to_client(task_id, {'error': 'Servicio de scrapping no disponible'})
            return
        
        # Enviar tarea al jefe Scrapper
        message = self._create_message(
            MessageProtocol.MESSAGE_TYPES['NEW_TASK'],
            {
                'task_id': task_id,
                'url': url,
                'source': 'router'
            }
        )
        
        self.scrapper_boss_connection.send_message(message)
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
                if self.bd_available and self.bd_boss_connection:
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
        if not self.bd_boss_connection:
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
        
        self.bd_boss_connection.send_message(message)
        logging.debug(f"Consulta enviada a BD para task {task_id}")
    
    def _connect_to_external_bosses(self):
        """Conecta con los jefes de BD y Scrapper"""
        logging.info("Conectando con jefes externos (BD y Scrapper)...")
        
        # Conectar con jefe BD
        bd_ips = self.discover_nodes('bd', self.bd_port)
        if bd_ips:
            bd_boss_ip = self._find_boss_in_list(bd_ips, 'bd')
            if bd_boss_ip:
                self._connect_to_bd_boss(bd_boss_ip)
        
        # Conectar con jefe Scrapper
        scrapper_ips = self.discover_nodes('scrapper', self.scrapper_port)
        if scrapper_ips:
            scrapper_boss_ip = self._find_boss_in_list(scrapper_ips, 'scrapper')
            if scrapper_boss_ip:
                self._connect_to_scrapper_boss(scrapper_boss_ip)
    
    def _find_boss_in_list(self, ip_list, node_type):
        """
        Encuentra el jefe en una lista de IPs consultando temporalmente.
        
        Args:
            ip_list: Lista de IPs a consultar
            node_type: Tipo de nodo ('bd', 'scrapper')
        
        Returns:
            str: IP del jefe o None
        """
        # Determinar el puerto correcto según el tipo de nodo
        target_port = self.scrapper_port if node_type == 'scrapper' else self.bd_port
        
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
                target_port, 
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
    
    def _connect_to_bd_boss(self, bd_ip):
        """Conecta con el jefe BD"""
        with self.external_connections_lock:
            if self.bd_boss_connection and self.bd_boss_connection.is_connected():
                logging.warning("Ya existe conexión con jefe BD")
                return
            
            self.bd_boss_connection = NodeConnection(
                'bd',
                bd_ip,
                self.bd_port,
                on_message_callback=self._handle_message_from_node,
                sender_node_type=self.node_type,
                sender_id=self.node_id
            )
            
            if self.bd_boss_connection.connect():
                logging.info(f"Conectado con jefe BD en {bd_ip}")
                
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
                self.bd_boss_connection.send_message(identification)
                
                self.bd_available = True
                
                # Iniciar heartbeats
                threading.Thread(
                    target=self._heartbeat_loop,
                    args=(self.bd_boss_connection,),
                    daemon=True
                ).start()
            else:
                logging.error(f"No se pudo conectar con jefe BD en {bd_ip}")
                self.bd_boss_connection = None
    
    def _connect_to_scrapper_boss(self, scrapper_ip):
        """Conecta con el jefe Scrapper"""
        with self.external_connections_lock:
            if self.scrapper_boss_connection and self.scrapper_boss_connection.is_connected():
                logging.warning("Ya existe conexión con jefe Scrapper")
                return
            
            self.scrapper_boss_connection = NodeConnection(
                'scrapper',
                scrapper_ip,
                self.scrapper_port,
                on_message_callback=self._handle_message_from_node,
                sender_node_type=self.node_type,
                sender_id=self.node_id
            )
            
            if self.scrapper_boss_connection.connect():
                logging.info(f"Conectado con jefe Scrapper en {scrapper_ip}")
                
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
                self.scrapper_boss_connection.send_message(identification)
                
                self.scrapper_available = True
                
                # Iniciar heartbeats
                threading.Thread(
                    target=self._heartbeat_loop,
                    args=(self.scrapper_boss_connection,),
                    daemon=True
                ).start()
            else:
                logging.error(f"No se pudo conectar con jefe Scrapper en {scrapper_ip}")
                self.scrapper_boss_connection = None
    
    def start_boss_tasks(self):
        """
        Tareas específicas del jefe Router.
        Override del método base.
        """
        logging.info("=== INICIANDO TAREAS DEL JEFE ROUTER ===")
        
        # Conectar con jefes externos
        self._connect_to_external_bosses()
        
        # Iniciar loop de procesamiento de tareas
        threading.Thread(
            target=self._process_tasks_loop,
            daemon=True
        ).start()
        
        logging.info("✓ Jefe Router operativo")


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