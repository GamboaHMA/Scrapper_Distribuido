import time
import threading
import logging
import os
from datetime import datetime
import queue

# Importar funciones de scrapping
from .scrapper import get_html_from_url
# Importar utilidades compartidas
from base_node.utils import NodeConnection, MessageProtocol, BossProfile
from base_node.node import compare_ips


# Por defecto INFO, pero se puede cambiar con LOG_LEVEL=DEBUG
log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class TaskQueue:
    """
    Gestiona la cola de tareas con estados y asignación Round-Robin.
    Estados posibles: pending, assigned, completed, failed
    """
    def __init__(self):
        self.tasks = {}  # {task_id: {'task_data': {...}, 'status': 'pending/assigned/completed/failed', 
                         #             'assigned_to': node_id, 'timestamp': datetime, 'attempts': int}}
        self.pending_queue = queue.Queue()  # Cola FIFO de task_ids pendientes
        self.lock = threading.Lock()
        self.round_robin_index = 0  # Índice para round-robin
        
    def add_task(self, task_id, task_data):
        """Añade una nueva tarea a la cola"""
        with self.lock:
            if task_id in self.tasks:
                logging.warning(f"Tarea {task_id} ya existe en la cola")
                return False
            
            self.tasks[task_id] = {
                'task_data': task_data,
                'status': 'pending',
                'assigned_to': None,
                'timestamp': datetime.now(),
                'attempts': 0
            }
            self.pending_queue.put(task_id)
            logging.info(f"Tarea {task_id} añadida a la cola (total: {len(self.tasks)})")
            return True
    
    def get_next_task(self):
        """Obtiene la siguiente tarea pendiente sin bloquear"""
        try:
            task_id = self.pending_queue.get_nowait()
            with self.lock:
                if task_id in self.tasks:
                    return task_id, self.tasks[task_id]['task_data']
            return None, None
        except queue.Empty:
            return None, None
    
    def assign_task(self, task_id, node_id):
        """Marca una tarea como asignada a un nodo"""
        with self.lock:
            if task_id in self.tasks:
                self.tasks[task_id]['status'] = 'assigned'
                self.tasks[task_id]['assigned_to'] = node_id
                self.tasks[task_id]['attempts'] += 1
                logging.info(f"Tarea {task_id} asignada a {node_id} (intento {self.tasks[task_id]['attempts']})")
                return True
            return False
    
    def complete_task(self, task_id, result=None):
        """Marca una tarea como completada"""
        with self.lock:
            if task_id in self.tasks:
                self.tasks[task_id]['status'] = 'completed'
                self.tasks[task_id]['result'] = result
                self.tasks[task_id]['completed_at'] = datetime.now()
                logging.info(f"Tarea {task_id} completada exitosamente")
                return True
            return False
    
    def fail_task(self, task_id):
        """Marca una tarea como fallida y la devuelve a la cola"""
        with self.lock:
            if task_id in self.tasks:
                self.tasks[task_id]['status'] = 'pending'
                self.tasks[task_id]['assigned_to'] = None
                self.pending_queue.put(task_id)
                logging.warning(f"Tarea {task_id} devuelta a la cola por fallo")
                return True
            return False
    
    def get_tasks_by_node(self, node_id):
        """Obtiene todas las tareas asignadas a un nodo específico
        NOTA: El llamador debe tener el lock"""
        return [task_id for task_id, info in self.tasks.items() 
               if info.get('assigned_to') == node_id and info.get('status') == 'assigned']
    
    def reassign_node_tasks(self, node_id):
        """Reasigna todas las tareas de un nodo que se desconectó"""
        with self.lock:
            failed_tasks = self.get_tasks_by_node(node_id)
            for task_id in failed_tasks:
                self.tasks[task_id]['status'] = 'pending'
                self.tasks[task_id]['assigned_to'] = None
                self.pending_queue.put(task_id)
            if failed_tasks:
                logging.warning(f"Reasignadas {len(failed_tasks)} tareas del nodo {node_id}")
            return len(failed_tasks)
    
    def get_stats(self):
        """Retorna estadísticas de la cola"""
        with self.lock:
            stats = {
                'total': len(self.tasks),
                'pending': sum(1 for t in self.tasks.values() if t['status'] == 'pending'),
                'assigned': sum(1 for t in self.tasks.values() if t['status'] == 'assigned'),
                'completed': sum(1 for t in self.tasks.values() if t['status'] == 'completed'),
                'failed': sum(1 for t in self.tasks.values() if t['status'] == 'failed')
            }
            return stats

NODE_TYPE_SCRAPPER = "scrapper"

import sys
import os
# Agregar el directorio base_node al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from base_node.node import Node

class ScrapperNode(Node):
    """
    Nodo Scrapper que hereda de la clase base Node.
    Añade funcionalidad específica de scrapping: gestión de tareas, conexiones a BD y Router.
    """
    
    def __init__(self, bd_port=9090, router_port=7070):
        # Inicializar clase base con node_type='scrapper'
        super().__init__(node_type='scrapper')
        
        # Puertos adicionales
        self.bd_port = bd_port
        self.router_port = router_port
        
        # Estado del nodo scrapper
        self.is_busy = False
        self.status_lock = threading.Lock()
        self.current_task = {}
        
        # Sistema de gestión de tareas (solo para el jefe)
        self.task_queue = TaskQueue()
        self.task_assignment_thread = None
        self.round_robin_lock = threading.Lock()
        
        # Registrar handlers específicos de scrapper
        self._register_scrapper_handlers()

        self.external_bosses = {
            'bd': BossProfile('bd', bd_port),
            'router': BossProfile('router', router_port)
        }
    
    def _register_scrapper_handlers(self):
        """Registra handlers específicos para mensajes de scrapper"""
        # Handlers para conexiones persistentes
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['TASK_ASSIGNMENT'],
            self._handle_task_assignment_persistent
        )
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['NEW_TASK'],
            self._handle_new_task_persistent
        )
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['TASK_RESULT'],
            self._handle_task_result_persistent
        )
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['TASK_ACCEPTED'],
            self._handle_task_accepted_persistent
        )
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['TASK_REJECTION'],
            self._handle_task_rejected_persistent
        )
    
    # ============= OVERRIDES DE MÉTODOS DE Node BASE =============
    
    def reassign_tasks_from_subordinate(self, node_id):
        """
        Override: Reasigna tareas del subordinado muerto.
        
        Args:
            node_id (str): ID del subordinado muerto
        
        Returns:
            int: Número de tareas reasignadas
        """
        reassigned = self.task_queue.reassign_node_tasks(node_id)
        if reassigned > 0:
            logging.info(f"Reasignadas {reassigned} tareas del subordinado {node_id}")
            # Intentar asignar las tareas devueltas
            self._try_assign_pending_tasks()
        return reassigned
    
    def _handle_external_bosses_info(self, node_connection, message_dict):
        """
        Override: Cuando recibe info de jefes externos, actualiza cache Y se conecta.
        Esto es crítico para que subordinados se conecten al router después de elecciones.
        
        Args:
            node_connection: Conexión con mi jefe scrapper
            message_dict: Mensaje con info de jefes externos (router, bd)
        """
        # Llamar al método base para actualizar cache
        super()._handle_external_bosses_info(node_connection, message_dict)
        
        # Solo el jefe scrapper necesita conectarse a jefes externos
        if not self.i_am_boss:
            logging.debug("No soy jefe, ignorando intento de conectar a jefes externos")
            return
        
        # Ahora conectarse a los jefes externos
        data = message_dict.get('data', {})
        bosses_info = data.get('bosses', {})
        
        for node_type, info in bosses_info.items():
            boss_ip = info.get('ip')
            boss_port = info.get('port')
            
            if node_type in self.external_bosses:
                boss_profile = self.external_bosses[node_type]
                
                # Solo conectar si no estamos conectados o si la IP cambió
                current_ip = boss_profile.connection.ip if boss_profile.connection else None
                
                if not boss_profile.is_connected() or current_ip != boss_ip:
                    logging.info(f"🔌 Conectando a jefe externo {node_type} en {boss_ip}:{boss_port}...")
                    self._connect_to_boss(node_type, boss_ip)
                    
                    if boss_profile.is_connected():
                        logging.info(f"✓ Conexión con jefe externo {node_type} establecida")
                    else:
                        logging.warning(f"✗ No se pudo conectar con jefe externo {node_type}")
                else:
                    logging.debug(f"Ya conectado a jefe externo {node_type} en {boss_ip}")
    
    # ============= HANDLERS ESPECÍFICOS DE SCRAPPER =============
    
    def _handle_task_assignment_persistent(self, node_connection, message_dict):
        """Handler para TASK_ASSIGNMENT (subordinado recibe tarea del jefe)"""
        if self.i_am_boss:
            logging.warning("Recibí TASK_ASSIGNMENT pero soy jefe. Ignorando.")
            return
        
        self._handle_task_message(node_connection, message_dict)
    
    def _handle_new_task_persistent(self, node_connection, message_dict):
        """Handler para NEW_TASK (jefe recibe tarea del router)"""
        if not self.i_am_boss:
            logging.warning("Recibí NEW_TASK pero no soy jefe. Ignorando.")
            return
        
        self._handle_new_task_from_router(node_connection, message_dict)
    
    def _handle_task_result_persistent(self, node_connection, message_dict):
        """Handler para TASK_RESULT (jefe recibe resultado de subordinado)"""
        if not self.i_am_boss:
            logging.warning("Recibí TASK_RESULT pero no soy jefe. Ignorando.")
            return
        
        self._handle_task_result_from_subordinate(node_connection, message_dict)
    
    def _handle_task_accepted_persistent(self, node_connection, message_dict):
        """Handler para TASK_ACCEPTED (jefe recibe confirmación)"""
        if not self.i_am_boss:
            logging.warning("Recibí TASK_ACCEPTED pero no soy jefe. Ignorando.")
            return
        
        self._handle_task_accepted(node_connection, message_dict)
    
    def _handle_task_rejected_persistent(self, node_connection, message_dict):
        """Handler para TASK_REJECTION (jefe recibe rechazo)"""
        if not self.i_am_boss:
            logging.warning("Recibí TASK_REJECTION pero no soy jefe. Ignorando.")
            return
        
        self._handle_task_rejected(node_connection, message_dict)
    
    # ============= LÓGICA DE TAREAS =============
    
    def _handle_task_message(self, node_connection, message_dict):
        """Procesa un mensaje de tarea (subordinado)"""
        data = message_dict.get('data', {})
        task_id = data.get("task_id")
        task_data = data.get("task_data")
        
        if not task_id or not task_data:
            logging.error(f"Mensaje de tarea inválido: {message_dict}")
            return
        
        if self.is_busy:
            logging.warning(f"Ocupado, rechazando tarea {task_id}")
            rejection_msg = self._create_message(
                MessageProtocol.MESSAGE_TYPES['TASK_REJECTION'],
                {
                    "task_id": task_id,
                    "reason": "busy"
                }
            )
            node_connection.send_message(rejection_msg)
            return
        
        # Aceptar tarea
        acceptance_msg = self._create_message(
            MessageProtocol.MESSAGE_TYPES['TASK_ACCEPTED'],
            {"task_id": task_id}
        )
        node_connection.send_message(acceptance_msg)
        
        # Ejecutar en hilo separado
        threading.Thread(
            target=self._execute_task,
            args=(node_connection, task_id, task_data),
            daemon=True
        ).start()
    
    def _execute_task(self, node_connection, task_id, task_data):
        """Ejecuta una tarea de scraping (subordinado)"""
        self.update_busy_status(True)
        
        try:
            if not isinstance(task_data, dict) or 'url' not in task_data:
                raise Exception("Formato de tarea inválido")
            
            url = task_data['url']
            logging.info(f"Scraping: {url}")
            
            scrape_result = get_html_from_url(url)
            
            # Validar que el scraping fue exitoso y tiene contenido real
            html_content = scrape_result.get('html', '')
            links = scrape_result.get('links', [])
            
            # Validación estricta: HTML debe tener contenido significativo
            if not html_content or len(html_content) < 100:
                raise Exception(f"HTML vacío o sin contenido suficiente (solo {len(html_content)} caracteres)")
            
            html_length = len(html_content)
            links_count = len(links)
            
            result = {
                'url': scrape_result['url'],
                'html_length': html_length,
                'links_count': links_count,
                'links': links[:10],
                'status': 'success'
            }
            
            logging.info(f"Scraping exitoso: {url} - {html_length} chars, {links_count} links")
            
        except Exception as e:
            logging.error(f"Error en scraping: {e}")
            result = {
                'url': task_data.get('url', 'unknown'),
                'status': 'error',
                'error': str(e)
            }
        
        # Enviar resultado
        result_msg = self._create_message(
            MessageProtocol.MESSAGE_TYPES['TASK_RESULT'],
            {
                'task_id': task_id,
                'result': result,
                'completed_at': datetime.now().isoformat()
            }
        )
        node_connection.send_message(result_msg)
        
        self.update_busy_status(False)
    
    def _execute_task_as_boss(self, task_id, task_data):
        """Ejecuta una tarea de scraping (jefe se auto-asigna)"""
        self.update_busy_status(True)
        
        try:
            if not isinstance(task_data, dict) or 'url' not in task_data:
                raise Exception("Formato de tarea inválido")
            
            url = task_data['url']
            logging.info(f"[JEFE] Scraping: {url}")
            
            scrape_result = get_html_from_url(url)
            
            # Validar que el scraping fue exitoso y tiene contenido real
            html_content = scrape_result.get('html', '')
            links = scrape_result.get('links', [])
            
            # Validación estricta: HTML debe tener contenido significativo
            if not html_content or len(html_content) < 100:
                raise Exception(f"HTML vacío o sin contenido suficiente (solo {len(html_content)} caracteres)")
            
            html_length = len(html_content)
            links_count = len(links)
            
            result = {
                'url': scrape_result['url'],
                'html_length': html_length,
                'links_count': links_count,
                'links': links[:10],
                'status': 'success'
            }
            
            logging.info(f"[JEFE] Scraping exitoso: {url} - {html_length} chars, {links_count} links")
            
        except Exception as e:
            logging.error(f"[JEFE] Error en scraping: {e}")
            result = {
                'url': task_data.get('url', 'unknown'),
                'status': 'error',
                'error': str(e)
            }
        
        # Completar tarea internamente (sin enviar mensaje a sí mismo)
        self.task_queue.complete_task(task_id, result)
        logging.info(f"[JEFE] Tarea {task_id} completada internamente")
        
        # Enviar resultado a BD
        self._send_result_to_database(task_id, result)
        
        # Notificar al router
        self._notify_router_task_completed(task_id, result)
        
        # Actualizar estado (esto también intentará asignar más tareas)
        self.update_busy_status(False)
        
        # Si soy jefe y ahora estoy libre, intentar asignar más tareas
        if self.i_am_boss and not self.is_busy:
            # Pequeño delay para asegurar que el estado se actualizó
            threading.Timer(0.1, self._try_assign_pending_tasks).start()
    
    def update_busy_status(self, is_busy):
        """Actualiza el estado de ocupado"""
        with self.status_lock:
            self.is_busy = is_busy
            logging.info(f"Estado actualizado a: {'ocupado' if is_busy else 'libre'}")
            
            # Notificar al jefe (si soy subordinado)
            if self.boss_connection and self.boss_connection.is_connected():
                status_msg = self._create_message(
                    MessageProtocol.MESSAGE_TYPES['STATUS_UPDATE'],
                    {'is_busy': self.is_busy}
                )
                self.boss_connection.send_message(status_msg)
        
        
    
    def _handle_new_task_from_router(self, node_connection, message_dict):
        """
        Handler para cuando el router envía una nueva tarea (solo jefe).
        IMPORTANTE: Este handler se ejecuta en el thread de recepción de mensajes,
        por lo que NO debe bloquearse. Delega el procesamiento a un thread separado.
        """
        data = message_dict.get('data', {})
        task_id = data.get('task_id')
        task_data = data.get('task_data')
        
        if not task_id or not task_data:
            logging.error(f"Tarea inválida recibida del router: {message_dict}")
            return
        
        logging.info(f"Nueva tarea recibida del router: {task_id}")
        
        # Delegar el procesamiento a un thread separado para no bloquear el thread de recepción
        threading.Thread(
            target=self._process_new_task_async,
            args=(task_id, task_data),
            daemon=True,
            name=f"ProcessTask-{task_id}"
        ).start()
    
    def _process_new_task_async(self, task_id, task_data):
        """
        Procesa una nueva tarea en un thread separado.
        Puede bloquearse esperando locks sin afectar la recepción de mensajes.
        """
        try:
            logging.debug(f"[DEBUG] Procesando tarea {task_id} en thread separado")
            
            # Logging detallado de subordinados (ahora seguro obtener el lock)
            with self.subordinates_lock:
                total_subs = len(self.subordinates)
                connected_subs = [node_id for node_id, conn in self.subordinates.items() if conn.is_connected()]
                disconnected_subs = [node_id for node_id, conn in self.subordinates.items() if not conn.is_connected()]
            
            # Lock liberado - log inmediato
            logging.debug(f"[DEBUG] Lock de subordinados liberado")
            
            logging.info(f"📊 Estado de subordinados: {len(connected_subs)}/{total_subs} conectados")
            if connected_subs:
                logging.info(f"  ✓ Conectados: {', '.join(connected_subs)}")
            if disconnected_subs:
                logging.info(f"  ✗ Desconectados: {', '.join(disconnected_subs)}")
            
            logging.debug(f"[DEBUG] Obteniendo stats de task_queue...")
            stats = self.task_queue.get_stats()
            logging.debug(f"[DEBUG] Stats obtenidos: {stats}")
            logging.debug(f"Estado actual - Jefe ocupado: {self.is_busy}, Tareas pendientes: {stats['pending']}")
            
            # Añadir a la cola
            logging.debug(f"[DEBUG] Añadiendo tarea {task_id} a la cola...")
            self.task_queue.add_task(task_id, task_data)
            logging.debug(f"[DEBUG] Tarea añadida correctamente")
            
            # Intentar asignar inmediatamente
            self._try_assign_pending_tasks()
            
        except Exception as e:
            logging.error(f"[ERROR] Excepción en _process_new_task_async: {e}")
            import traceback
            logging.error(traceback.format_exc())
    
    def _handle_task_result_from_subordinate(self, node_connection, message_dict):
        """
        Handler para cuando un subordinado completa una tarea (solo jefe).
        Delega el procesamiento a thread separado para evitar bloquear recepción de mensajes.
        """
        data = message_dict.get('data', {})
        task_id = data.get('task_id')
        result = data.get('result')
        
        if not task_id:
            logging.error(f"Resultado de tarea sin task_id: {message_dict}")
            return
        
        logging.info(f"Resultado de tarea {task_id} recibido de {node_connection.node_id}")
        
        # Procesar en thread separado
        threading.Thread(
            target=self._process_task_result_async,
            args=(task_id, result),
            daemon=True,
            name=f"ProcessResult-{task_id}"
        ).start()
    
    def _process_task_result_async(self, task_id, result):
        """Procesa el resultado de una tarea en thread separado"""
        try:
            # Marcar tarea como completada
            self.task_queue.complete_task(task_id, result)
            
            # Enviar resultado a BD
            self._send_result_to_database(task_id, result)
            
            # Notificar al router
            self._notify_router_task_completed(task_id, result)
            
            # Intentar asignar más tareas
            self._try_assign_pending_tasks()
            
        except Exception as e:
            logging.error(f"Error procesando resultado de tarea {task_id}: {e}")
            import traceback
            logging.error(traceback.format_exc())
    
    def _handle_task_accepted(self, node_connection, message_dict):
        """Handler para cuando un subordinado acepta una tarea (solo jefe)"""
        data = message_dict.get('data', {})
        task_id = data.get('task_id')
        
        if not task_id:
            logging.error(f"Aceptación de tarea sin task_id: {message_dict}")
            return
        
        logging.info(f"Tarea {task_id} aceptada por {node_connection.node_id}")
    
    def _handle_task_rejected(self, node_connection, message_dict):
        """Handler para cuando un subordinado rechaza una tarea (solo jefe)"""
        data = message_dict.get('data', {})
        task_id = data.get('task_id')
        reason = data.get('reason', 'unknown')
        
        if not task_id:
            logging.error(f"Rechazo de tarea sin task_id: {message_dict}")
            return
        
        logging.warning(f"Tarea {task_id} rechazada por {node_connection.node_id} (razón: {reason})")
        
        # Marcar subordinado como disponible
        node_connection.is_busy = False
        
        # Devolver tarea a la cola
        success = self.task_queue.fail_task(task_id)
        
        if success:
            logging.info(f"Tarea {task_id} devuelta a la cola para reasignación")
            self._try_assign_pending_tasks()
        else:
            logging.error(f"No se pudo devolver tarea {task_id} a la cola")
    
    def _try_assign_pending_tasks(self):
        """Asigna tareas pendientes a subordinados disponibles y al jefe (Round-Robin)"""
        if not self.i_am_boss:
            return
        
        # Verificar si hay tareas pendientes primero
        stats = self.task_queue.get_stats()
        if stats['pending'] == 0:
            return
        
        logging.debug(f"Intentando asignar tareas. Pendientes: {stats['pending']}")
        
        # Obtener lista de trabajadores disponibles (subordinados + jefe)
        available_workers = []
        
        # Agregar subordinados disponibles
        with self.subordinates_lock:
            total_subordinates = len(self.subordinates)
            connected_subordinates = 0
            busy_subordinates = 0
            available_subordinate_ids = []
            
            for node_id, conn in self.subordinates.items():
                if conn.is_connected():
                    connected_subordinates += 1
                    if not conn.is_busy:
                        available_workers.append(('subordinate', node_id, conn))
                        available_subordinate_ids.append(node_id)
                    else:
                        busy_subordinates += 1
        
        logging.info(f"🔍 Subordinados: {total_subordinates} totales, {connected_subordinates} conectados, "
                     f"{busy_subordinates} ocupados, {len(available_subordinate_ids)} disponibles")
        if available_subordinate_ids:
            logging.info(f"  ✓ Disponibles para asignar: {', '.join(available_subordinate_ids)}")
        
        # Agregar el jefe si está disponible
        boss_available = False
        if not self.is_busy:
            available_workers.append(('boss', self.node_id, None))
            boss_available = True
        
        logging.debug(f"Jefe disponible: {boss_available} (is_busy={self.is_busy})")
        
        if not available_workers:
            logging.warning(f"No hay trabajadores disponibles para asignar {stats['pending']} tareas pendientes")
            return
        
        logging.info(f"Asignando tareas con {len(available_workers)} trabajadores disponibles")
        
        # Asignar tareas
        assigned_count = 0
        while available_workers:
            task_id, task_data = self.task_queue.get_next_task()
            
            if not task_id:
                break
            
            # Round-robin entre todos los trabajadores
            with self.round_robin_lock:
                worker_list = list(available_workers)
                if not worker_list:
                    break
                
                index = assigned_count % len(worker_list)
                worker_type, node_id, conn = worker_list[index]
            
            # Asignar tarea
            self.task_queue.assign_task(task_id, node_id)
            
            if worker_type == 'boss':
                # El jefe se auto-asigna la tarea
                logging.info(f"Tarea {task_id} auto-asignada al jefe (Round-Robin)")
                assigned_count += 1
                
                # Ejecutar en hilo separado
                threading.Thread(
                    target=self._execute_task_as_boss,
                    args=(task_id, task_data),
                    daemon=True
                ).start()
                
                # Quitar al jefe de disponibles
                available_workers = [
                    (wt, nid, c) for wt, nid, c in available_workers if wt != 'boss'
                ]
            else:
                # Asignar a subordinado
                task_msg = self._create_message(
                    MessageProtocol.MESSAGE_TYPES['TASK_ASSIGNMENT'],
                    {
                        'task_id': task_id,
                        'task_data': task_data
                    }
                )
                
                success = conn.send_message(task_msg)
                
                if success:
                    conn.is_busy = True
                    logging.info(f"Tarea {task_id} asignada a {node_id} (Round-Robin)")
                    assigned_count += 1
                    
                    # Quitar de disponibles
                    available_workers = [
                        (wt, nid, c) for wt, nid, c in available_workers if nid != node_id
                    ]
                else:
                    logging.error(f"No se pudo enviar tarea {task_id} a {node_id}")
                    self.task_queue.fail_task(task_id)
        
        if assigned_count > 0:
            stats = self.task_queue.get_stats()
            logging.info(f"✅ Asignadas {assigned_count} tareas. Cola actualizada: {stats}")
        else:
            logging.warning(f"⚠️ No se pudo asignar ninguna tarea")
    
    def _send_result_to_database(self, task_id, result):
        """Envía resultado al jefe de BD"""
        # Si soy jefe, buscar en bosses_connections (conexión entrante del BD)
        # Si soy subordinado, buscar en external_bosses (conexión saliente al BD)
        bd_conn = None
        
        if self.i_am_boss:
            # Jefe: BD se conectó a mí, buscar en bosses_connections
            bd_conn = self.bosses_connections.get('bd')
        else:
            # Subordinado: yo me conecté al BD, buscar en external_bosses
            bd_profile = self.external_bosses.get('bd')
            bd_conn = bd_profile.connection if bd_profile else None
        
        if not bd_conn or not bd_conn.is_connected():
            logging.warning(f"No hay conexión con BD para enviar resultado de tarea {task_id}")
            return
        
        save_msg = self._create_message(
            MessageProtocol.MESSAGE_TYPES['SAVE_DATA'],
            {
                'task_id': task_id,
                'result': result,
                'timestamp': datetime.now().isoformat()
            }
        )
        
        if bd_conn.send_message(save_msg):
            logging.info(f"Resultado de tarea {task_id} enviado a BD")
        else:
            logging.error(f"No se pudo enviar resultado de tarea {task_id} a BD")
    
    def _notify_router_task_completed(self, task_id, result):
        """Notifica al router que una tarea fue completada"""
        # Si soy jefe, buscar en bosses_connections (conexión entrante del Router)
        # Si soy subordinado, buscar en external_bosses (conexión saliente al Router)
        router_conn = None
        
        if self.i_am_boss:
            # Jefe: Router se conectó a mí, buscar en bosses_connections
            router_conn = self.bosses_connections.get('router')
        else:
            # Subordinado: yo me conecté al Router, buscar en external_bosses
            router_profile = self.external_bosses.get('router')
            router_conn = router_profile.connection if router_profile else None
        
        if not router_conn or not router_conn.is_connected():
            logging.warning(f"No hay conexión con Router para notificar tarea {task_id}")
            return
        
        completion_msg = self._create_message(
            MessageProtocol.MESSAGE_TYPES['TASK_RESULT'],
            {
                'task_id': task_id,
                'result': result,
                'success': result.get('status') == 'success' if result else False,
                'timestamp': datetime.now().isoformat()
            }
        )
        
        if router_conn.send_message(completion_msg):
            logging.info(f"Resultado de tarea {task_id} enviado a Router")
        else:
            logging.error(f"No se pudo enviar resultado de tarea {task_id} al Router")
    
    def _start_task_assignment_thread(self):
        """Inicia hilo de asignación periódica de tareas"""
        if self.task_assignment_thread and self.task_assignment_thread.is_alive():
            logging.debug("Hilo de asignación de tareas ya está ejecutándose")
            return
        
        self.task_assignment_thread = threading.Thread(
            target=self._task_assignment_loop,
            name="TaskAssignment",
            daemon=True
        )
        self.task_assignment_thread.start()
        logging.info("Hilo de asignación de tareas iniciado")
    
    def _task_assignment_loop(self):
        """Loop que periódicamente intenta asignar tareas pendientes"""
        while self.running and self.i_am_boss:
            try:
                time.sleep(5)
                
                stats = self.task_queue.get_stats()
                if stats['pending'] > 0:
                    logging.debug(f"Intentando asignar {stats['pending']} tareas pendientes...")
                    self._try_assign_pending_tasks()
                    
            except Exception as e:
                logging.error(f"Error en loop de asignación de tareas: {e}")

    #============= PARA DESCUBRIR A LOS OTROS JEFES ==============

    def _connect_to_external_bosses(self):
        """
        El Scrapper NO busca activamente a Router/BD.
        Espera a que el Router se conecte a él (modo pasivo).
        El Router es el único que hace búsquedas DNS activas.
        """
        logging.info("Scrapper en modo pasivo: esperando conexiones de Router...")
        # El Scrapper NO busca activamente a Router/BD.
        # La conexión se establece cuando:
        # 1. El Router se conecta al Scrapper (modo pasivo)
        # 2. El jefe Scrapper replica la info de Router a subordinados
        # 3. Los subordinados se conectan al Router usando _handle_external_bosses_info

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
        
        # Verificar si ya existe conexión (is_connected() tiene su propio lock)
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
                    'is_boss': True,  # _connect_to_boss es siempre jefe-a-jefe
                    'is_temporary': False
                }
            )
            new_connection.send_message(identification)
            
            # Actualizar perfil (set_connection ya tiene su propio lock)
            boss_profile.set_connection(new_connection)
            
            # Iniciar heartbeats
            # threading.Thread(
            #     target=self._heartbeat_loop,
            #     args=(new_connection,),
            #     daemon=True
            # ).start()
        else:
            logging.error(f"No se pudo conectar con jefe {node_type} en {boss_ip}")
            boss_profile.clear_connection()

    def start_boss_tasks(self):
        """
        Tareas específicas del jefe Scrapper.
        Override del método base.
        """
        logging.info("=== INICIANDO TAREAS DEL JEFE SCRAPPER ===")
        
        # Conectar con jefes externos
        # self._connect_to_external_bosses()
        self._start_task_assignment_thread()
        
        # Loop de reunificación: detecta otros jefes scrapper cuando la red se reconecta
        # threading.Thread(target=self._scrapper_reunification_loop, daemon=True, name="ScrapperReunification").start()
        
        logging.info("✓ Jefe Scrapper operativo")
    
    def _scrapper_reunification_loop(self):
        """
        Loop periódico que busca otros jefes scrapper en la red.
        Si detecta otro jefe scrapper, inicia elecciones para resolver el conflicto.
        Solo se ejecuta cuando este nodo es jefe.
        """
        logging.info("🔄 Iniciando hilo de reunificación de red scrapper...")
        check_interval = 30
        
        while self.running and self.i_am_boss:
            try:
                time.sleep(check_interval)
                
                if not self.i_am_boss:
                    break
                
                # Descubrir todos los scrappers en la red
                discovered_ips = self.discover_nodes(self.node_type, self.port)
                
                if not discovered_ips:
                    continue
                
                # Obtener IPs de mis subordinados actuales
                subordinate_ips = set()
                with self.subordinates_lock:
                    for conn in self.subordinates.values():
                        subordinate_ips.add(conn.ip)
                
                # Buscar scrappers que no son yo ni mis subordinados
                unknown_scrapers = [ip for ip in discovered_ips
                                    if ip != self.ip and ip not in subordinate_ips]
                
                if not unknown_scrapers:
                    continue
                
                logging.info(f"🔍 Detectados {len(unknown_scrapers)} scrapper(s) desconocido(s): {unknown_scrapers}")
                
                for scrapper_ip in unknown_scrapers:
                    
                    # # Intentar adoptar directamente como subordinado primero
                    # # Esto evita usar ELECTION cuando simplemente el nodo no se registró aún
                    # logging.info(f"🔗 Intentando adoptar scrapper desconocido {scrapper_ip} como subordinado...")
                    # if self.add_subordinate(scrapper_ip):
                    #     logging.info(f"✅ Scrapper {scrapper_ip} adoptado como subordinado")
                    #     continue
                    
                    # Si no se pudo conectar directamente, verificar si es otro jefe usando ELECTION
                    election_msg = self._create_message(
                        MessageProtocol.MESSAGE_TYPES['ELECTION'],
                        {
                            'ip': self.ip,
                            'port': self.port
                        }
                    )
                    
                    logging.info(f"🗳️ No se pudo adoptar {scrapper_ip}, enviando elección para verificar su estado...")
                    response = self.send_temporary_message(
                        scrapper_ip, self.port, election_msg,
                        expect_response=True, timeout=3.0, node_type=self.node_type
                    )
                    
                    if response and isinstance(response, dict) and response.get('type') == MessageProtocol.MESSAGE_TYPES['ELECTION_RESPONSE']:
                        other_ip = response.get('data', {}).get('ip', scrapper_ip)
                        
                        if compare_ips(other_ip, self.ip) > 0:
                            # El otro tiene IP mayor → yo debo ceder jefatura
                            logging.warning(f"⚠️ Otro jefe scrapper {other_ip} tiene IP mayor. Cediendo jefatura directamente...")
                            threading.Thread(target=self._demote_to_subordinate, args=(other_ip,), daemon=True).start()
                            break
                        else:
                            # Mi IP es mayor y ya intenté adoptarlo sin éxito → reenviar NEW_BOSS
                            logging.info(f"Scrapper {other_ip} tiene IP menor pero no se pudo conectar. Enviando NEW_BOSS...")
                            new_boss_msg = self._create_message(
                                MessageProtocol.MESSAGE_TYPES['NEW_BOSS'],
                                {'ip': self.ip, 'port': self.port}
                            )
                            self.send_temporary_message(
                                scrapper_ip, self.port, new_boss_msg,
                                expect_response=False, node_type=self.node_type
                            )
                            
                    elif response is None:
                        logging.debug(f"Scrapper {scrapper_ip} no respondió - puede estar caído")
                    
            except Exception as e:
                logging.error(f"Error en loop de reunificación scrapper: {e}")
                time.sleep(5)
        
        logging.info("🔄 Hilo de reunificación scrapper detenido")
    
    def stop_boss_tasks(self):
        """
        Detiene las tareas específicas del jefe Scrapper.
        Se llama cuando el nodo cede el rol de jefe.
        """
        logging.info("=== DETENIENDO TAREAS DEL JEFE SCRAPPER ===")
        
        # El thread task_assignment_thread es daemon y verifica self.i_am_boss
        # Al cambiar i_am_boss a False, el loop se detendrá automáticamente
        if self.task_assignment_thread and self.task_assignment_thread.is_alive():
            logging.info("Esperando a que termine el hilo de asignación de tareas...")
            # El loop verificará i_am_boss=False y terminará
        
        # Limpiar la cola de tareas pendientes (ya no soy responsable)
        pending_count = self.task_queue.get_stats()['pending']
        if pending_count > 0:
            logging.warning(f"Dejando {pending_count} tareas pendientes (el nuevo jefe las manejará)")
        
        logging.info("✓ Tareas de jefe Scrapper detenidas")

        self.i_am_boss = False





# ============= FUNCIÓN MAIN =============

if __name__ == "__main__":
    try:
        # Crear y arrancar nodo scrapper
        scrapper = ScrapperNode()
        scrapper.start()  # Hereda el método start() de Node
        
    except KeyboardInterrupt:
        logging.info("Deteniendo scrapper...")
        if 'scrapper' in locals():
            scrapper.stop()
    except Exception as e:
        logging.error(f"Error fatal: {e}")
        import traceback
        traceback.print_exc()