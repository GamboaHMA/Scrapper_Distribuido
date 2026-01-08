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
        self.add_temporary_message_handler(
            MessageProtocol.MESSAGE_TYPES['GET_ROUTER_LEADER'],
            self._get_leader
        )
        # self.persistent_message_handler['scrapper_result'] = self.
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['SCRAPPER_RESULT'],
            self._handle_scrapper_result
        )

        logging.debug("Handlers del router registrados")

    def _get_leader(self, sock, client_ip, message):
        """Responde con la información del líder del router (sí mismo)"""

        data = message.get('data')
        client_ip = message.get('sender_id')
        client_port = data.get('port')

        if self.boss_connection is None:
            logging.warning("No hay líder Router disponible para responder")
            return
        
        else:
            response = self._create_message(
                MessageProtocol.MESSAGE_TYPES['LEADER_RESPONSE'],
                {
                    'leader_ip': self.boss_connection.ip,
                    'leader_port': self.port,
                    'is_boss': self.i_am_boss
                }
            )

            try:
                response_bytes = response.encode()
                length = len(response_bytes).to_bytes(2, 'big')
                sock.sendall(length)
                sock.sendall(response_bytes)
                logging.info(f"Enviada información del líder Router a {client_ip}:{client_port}")
            except sock.timeout as e:
                logging.error(f"Timeout enviando líder al cliente {client_ip}:{client_port}: {e}")
            except Exception as e:
                logging.error(f"Error enviando líder al cliente {client_ip}:{client_port}: {e}")
    
    def _handle_get_subordinates(self, node_connection, message):
        """Responde con lista de subordinados en cache"""

        subordinates_info = {}
    
        # Router subordinados (mismo tipo)
        for ip, info in self.nodes_cache.get('router', {}).items():
            if info['is_boss'] == False:  # Solo subordinados
                subordinates_info[ip] = info
    
        # Jefes externos (BD, Scrapper)
        for node_type, info in self.external_bosses_cache.items():
            subordinates_info[f"{node_type}_boss"] = {
                'ip': info['ip'],
                'port': info['port'],
                'type': node_type,
                'is_boss': True
            }
    
        response = self._create_message(
            MessageProtocol.MESSAGE_TYPES['SUBORDINATES_LIST'],
            {'subordinates': subordinates_info}
        )
        node_connection.send_message(response)
        logging.info(f"Lista de {len(subordinates_info)} subordinados enviada")



    def _handle_client_request(self, sock, message):
        """
        Handler para peticiones de clientes (conexión temporal).
        
        Args:
            sock: Socket del cliente
            client_ip: IP del cliente
            message: Mensaje con la petición
        """
        data = message.get('data', {})
        task_id = data.get('task_id')
        client_ip = message.get('sender_id')
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
    
    # TODO: COORDINAR CON DATA BASE
    def _handle_bd_response(self, node_connection, data):
        """
        Handler para respuestas de BD sobre consultas de URLs.
        
        Args:
            node_connection: Conexión con el nodo BD
            data: Datos de la respuesta
        """
        data = data.get('data') # acceder a la data de message
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
                'url': url,
                'source': 'router'
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
        logging.debug(f"Consulta enviada a BD para task {task_id}")
    
    def _connect_to_external_bosses(self):
        """Conecta con los jefes de BD y Scrapper"""
        logging.info("Conectando con jefes externos (BD y Scrapper)...")
        
        for node_type in self.external_bosses.keys():
            threading.Thread(
                target=self._periodic_boss_search,
                args=(node_type,),
                daemon=True
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
        Una vez conectado, detiene la búsqueda.
        
        Args:
            node_type: Tipo de nodo a buscar ('bd' o 'scrapper')
        """
        retry_interval = 5  # segundos entre intentos
        boss_profile = self.external_bosses[node_type]
        
        logging.info(f"Iniciando búsqueda periódica del jefe {node_type}...")
        
        while self.running:
            # Si ya estamos conectados, detener búsqueda
            if boss_profile.is_connected():
                logging.debug(f"Jefe {node_type} ya conectado, deteniendo búsqueda")
                break
            
            # Intentar descubrir nodos
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
                        break
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
        
        with boss_profile.lock:
            # Verificar si ya existe conexión
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
                
                # Actualizar perfil
                boss_profile.set_connection(new_connection)
                
                # Iniciar heartbeats
                threading.Thread(
                    target=self._heartbeat_loop,
                    args=(new_connection,),
                    daemon=True
                ).start()
            else:
                logging.error(f"No se pudo conectar con jefe {node_type} en {boss_ip}")
                boss_profile.clear_connection()
    
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