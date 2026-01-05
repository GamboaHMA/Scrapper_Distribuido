import time
import threading
import logging
import os
from datetime import datetime
import queue

# Importar funciones de scrapping
from .scrapper import get_html_from_url
# Importar utilidades compartidas
from base_node.utils import NodeConnection, MessageProtocol


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
        """Obtiene todas las tareas asignadas a un nodo específico"""
        with self.lock:
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
    
    def start_boss_tasks(self):
        """
        Override: Inicia tareas específicas del jefe scrapper.
        - Conecta con BD y Router jefes
        - Inicia hilo de asignación de tareas
        """
        if not self.i_am_boss:
            return
        
        logging.info("🔧 Iniciando tareas de jefe scrapper...")
        
        # 1. Descubrir y conectar con jefe de BD
        # logging.info("Buscando jefe de BD...")
        # bd_ips = self.discover_nodes('bd', self.bd_port)
        # if bd_ips:
        #     bd_boss_found = self.broadcast_identification('bd')
        #     if bd_boss_found:
        #         logging.info("✓ Conectado a jefe de BD")
        #     else:
        #         logging.warning("✗ No se encontró jefe de BD")
        # else:
        #     logging.warning("No se encontraron nodos BD en la red")
        
        # 2. Descubrir y conectar con jefe de Router
        # logging.info("Buscando jefe de Router...")
        # router_ips = self.discover_nodes('router', self.router_port)
        # if router_ips:
        #     router_boss_found = self.broadcast_identification('router')
        #     if router_boss_found:
        #         logging.info("✓ Conectado a jefe de Router")
        #     else:
        #         logging.warning("✗ No se encontró jefe de Router")
        # else:
        #     logging.warning("No se encontraron nodos Router en la red")
        
        # 3. Iniciar hilo de asignación de tareas
        self._start_task_assignment_thread()
    
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
    
    def update_busy_status(self, is_busy):
        """Actualiza el estado de ocupado"""
        with self.status_lock:
            self.is_busy = is_busy
            logging.info(f"Estado actualizado a: {'ocupado' if is_busy else 'libre'}")
            
            # Notificar al jefe
            if self.boss_connection and self.boss_connection.is_connected():
                status_msg = self._create_message(
                    MessageProtocol.MESSAGE_TYPES['STATUS_UPDATE'],
                    {'is_busy': self.is_busy}
                )
                self.boss_connection.send_message(status_msg)
    
    def _handle_new_task_from_router(self, node_connection, message_dict):
        """Handler para cuando el router envía una nueva tarea (solo jefe)"""
        data = message_dict.get('data', {})
        task_id = data.get('task_id')
        task_data = data.get('task_data')
        
        if not task_id or not task_data:
            logging.error(f"Tarea inválida recibida del router: {message_dict}")
            return
        
        logging.info(f"Nueva tarea recibida del router: {task_id}")
        
        # Añadir a la cola
        self.task_queue.add_task(task_id, task_data)
        
        # Intentar asignar inmediatamente
        self._try_assign_pending_tasks()
    
    def _handle_task_result_from_subordinate(self, node_connection, message_dict):
        """Handler para cuando un subordinado completa una tarea (solo jefe)"""
        data = message_dict.get('data', {})
        task_id = data.get('task_id')
        result = data.get('result')
        
        if not task_id:
            logging.error(f"Resultado de tarea sin task_id: {message_dict}")
            return
        
        logging.info(f"Resultado de tarea {task_id} recibido de {node_connection.node_id}")
        
        # Marcar tarea como completada
        self.task_queue.complete_task(task_id, result)
        
        # Enviar resultado a BD
        self._send_result_to_database(task_id, result)
        
        # Notificar al router
        self._notify_router_task_completed(task_id, result)
        
        # Intentar asignar más tareas
        self._try_assign_pending_tasks()
    
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
        """Asigna tareas pendientes a subordinados disponibles (Round-Robin)"""
        if not self.i_am_boss:
            return
        
        # Obtener subordinados disponibles
        available_subordinates = [
            (node_id, conn) for node_id, conn in self.subordinates.items()
            if conn.is_connected() and not conn.is_busy
        ]
        
        if not available_subordinates:
            logging.debug("No hay subordinados disponibles para asignar tareas")
            return
        
        # Asignar tareas
        assigned_count = 0
        while available_subordinates:
            task_id, task_data = self.task_queue.get_next_task()
            
            if not task_id:
                break
            
            # Round-robin
            with self.round_robin_lock:
                subordinate_list = list(available_subordinates)
                if not subordinate_list:
                    break
                
                index = assigned_count % len(subordinate_list)
                node_id, conn = subordinate_list[index]
            
            # Asignar tarea
            self.task_queue.assign_task(task_id, node_id)
            
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
                available_subordinates = [
                    (nid, c) for nid, c in available_subordinates if nid != node_id
                ]
            else:
                logging.error(f"No se pudo enviar tarea {task_id} a {node_id}")
                self.task_queue.fail_task(task_id)
        
        if assigned_count > 0:
            stats = self.task_queue.get_stats()
            logging.info(f"Asignadas {assigned_count} tareas. Cola: {stats}")
    
    def _send_result_to_database(self, task_id, result):
        """Envía resultado al jefe de BD"""
        bd_conn = self.bosses_connections.get('bd')
        
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
        router_conn = self.bosses_connections.get('router')
        
        if not router_conn or not router_conn.is_connected():
            logging.warning(f"No hay conexión con Router para notificar tarea {task_id}")
            return
        
        completion_msg = self._create_message(
            MessageProtocol.MESSAGE_TYPES['TASK_COMPLETED'],
            {
                'task_id': task_id,
                'status': result.get('status', 'unknown'),
                'timestamp': datetime.now().isoformat()
            }
        )
        
        if router_conn.send_message(completion_msg):
            logging.info(f"Notificación de tarea completada {task_id} enviada a Router")
        else:
            logging.error(f"No se pudo notificar al Router sobre tarea {task_id}")
    
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