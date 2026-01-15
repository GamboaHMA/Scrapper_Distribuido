'''
Nodo Database:
- Encargado de almacenar la informacion
'''

import sys
import os
import logging
import threading
import time
import random
from datetime import datetime
from base_node.node import Node
from base_node.utils import MessageProtocol, NodeConnection, BossProfile
from base_node.node import PORTS
import queue
from pathlib import Path
import sqlite3
import json
import socket
import random

log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class DatabaseNode(Node):
    '''

    Nodo Database:
    -
    -
    -

    '''

    def __init__(self, scrapper_port=8080, router_port=7070):
        super().__init__(node_type='bd')

        #base de datos
        self.name = socket.gethostname()
        self.db_conn = None
        self.db_cursor = None
        self.db_lock = threading.Lock()  # Lock para operaciones de BD
        self.logs_conn = None
        self.logs_cursor = None
        self.data_dir = f"database/{self.name}"
        logging.info("a punto de iniciar base de datos")

        #envio de mensajes
        self.message_queue = queue.Queue()
        self.stop_event = threading.Event()
        self.status_lock = threading.Lock()

        # Perfiles de jefes externos
        self.external_bosses = {
            'router': BossProfile('router', router_port),
            'scrapper': BossProfile('scrapper', scrapper_port)
        }

        #Registrar handlers expecificos del nodo database
        self._register_bd_handlers()
    
        self.init_database()

    def reassign_tasks_from_subordinate(self, node_id):
        """
        Sobrescribe método de Node para manejar desconexión de subordinados BD.
        Cuando un subordinado se desconecta, inicia re-replicación inmediatamente.
        
        Args:
            node_id: ID del subordinado desconectado
        """
        logging.info(f"[REREPLICATE] reassign_tasks_from_subordinate llamado para: {node_id}")
        
        # Llamar implementación base (puede estar vacía pero por si acaso)
        super().reassign_tasks_from_subordinate(node_id)
        
        # Si soy el jefe BD, manejar re-replicación
        if not self.i_am_boss:
            logging.info(f"[REREPLICATE] No soy jefe, ignorando desconexión de {node_id}")
            return
        
        logging.info(f"[REREPLICATE] Soy jefe BD, iniciando re-replicación para: {node_id}")
        
        # Crear cursor separado para evitar conflictos con monitor u otros hilos
        try:
            # Verificar que la BD está disponible
            if not self.db_conn:
                logging.error(f"[REREPLICATE] Base de datos no inicializada, no se puede re-replicar")
                return
            
            # Simplemente llamar la re-replicación con el node_id
            logging.info(f"[REREPLICATE] Iniciando re-replicación para node_id: {node_id}")
            self._handle_subordinate_disconnection(node_id)
                
        except Exception as e:
            logging.error(f"[REREPLICATE] Error iniciando re-replicación para {node_id}: {e}")
            import traceback
            traceback.print_exc()

    def start_boss_tasks(self):
        """
        Sobrescribe el método de Node para iniciar tareas específicas del jefe BD.
        Se llama automáticamente cuando este nodo es elegido como jefe.
        """
        logging.info("Iniciando tareas de jefe BD...")
        
        # Ya no necesitamos registrar al jefe en una tabla databases
        # El node_id es suficiente identificador
        
        # PRIMERO: Reconstruir tablas desde subordinados (recuperación de metadata)
        self._recover_metadata_from_subordinates()
        
        # 2.5: Registrar las URLs que el jefe tiene en su propia url_content
        self._register_own_urls_in_log()
        
        # 2.7: Consolidar URLs duplicadas manteniendo contenido más antiguo
        self._consolidate_duplicate_urls()
        
        # SEGUNDO: Redistribuir URLs que este nodo tenía cuando era subordinado
        self._redistribute_own_urls()
        
        # Iniciar búsqueda y conexión con jefes externos (Router)
        self._connect_to_external_bosses()
        
        # Iniciar monitor de subordinados para re-replicación
        threading.Thread(
            target=self._monitor_subordinates_health,
            daemon=True,
            name="bd-subordinates-monitor"
        ).start()
        
        logging.info("Tareas de jefe BD iniciadas correctamente")
    
    def stop_boss_tasks(self):
        """
        Detiene las tareas específicas del jefe BD.
        Se llama cuando el nodo cede el rol de jefe.
        """
        logging.info("=== DETENIENDO TAREAS DEL JEFE BD ===")
        
        # El thread de monitoreo es daemon y verifica self.i_am_boss
        # Al cambiar i_am_boss a False, el loop se detendrá automáticamente
        logging.info("El monitor de subordinados se detendrá automáticamente")
        
        logging.info("✓ Tareas de jefe BD detenidas")

    def _monitor_subordinates_health(self):
        """Monitor periódico para detectar subordinados desconectados y re-replicar datos"""
        check_interval = 30  # Verificar cada 30 segundos
        
        logging.info("Monitor de salud de subordinados BD iniciado")
        
        while self.running and self.i_am_boss:
            time.sleep(check_interval)
            
            try:
                # Obtener lista de subordinados actualmente conectados
                with self.status_lock:
                    connected_node_ids = set(self.subordinates.keys())
                
                # Obtener node_id del jefe para excluirlo
                # boss_node_id = self.node_id #f"{self.node_type}-{self.ip}:{self.port}"
                
                with self.db_lock:
                    # Consultar subordinados que tienen URLs registradas (excluir al jefe)
                    self.db_cursor.execute('''
                        SELECT DISTINCT node_id 
                        FROM url_db_log 
                        WHERE node_id != ?
                    ''', (self.node_id,))
                    db_subordinates = [row[0] for row in self.db_cursor.fetchall()]
                
                # Detectar subordinados desconectados
                for node_id in db_subordinates:
                    if node_id not in connected_node_ids:
                        logging.warning(f"Subordinado {node_id} está desconectado, iniciando re-replicación")
                        self._handle_subordinate_disconnection(node_id)
                        
            except Exception as e:
                logging.error(f"Error en monitor de subordinados: {e}")
                import traceback
                traceback.print_exc()

    def _handle_subordinate_disconnection(self, disconnected_node_id):
        """
        Maneja la desconexión de un subordinado: actualiza contadores de réplicas y re-replica si es necesario.
        
        Args:
            disconnected_node_id: Node ID del subordinado desconectado
        """
        logging.info(f"Manejando desconexión de subordinado: {disconnected_node_id}")
        
        try:
            with self.db_lock:
                # Obtener todas las URLs que tenía este subordinado
                self.db_cursor.execute('''
                    SELECT DISTINCT url_id 
                    FROM url_db_log 
                    WHERE node_id = ?
                ''', (disconnected_node_id,))
                affected_urls = [row[0] for row in self.db_cursor.fetchall()]
                
                if not affected_urls:
                    logging.info(f"No hay URLs afectadas por desconexión de {disconnected_node_id}")
                    return
                
                logging.info(f"Procesando {len(affected_urls)} URLs afectadas por desconexión")
                
                # Eliminar registros del nodo desconectado de url_db_log
                self.db_cursor.execute('''
                    DELETE FROM url_db_log 
                    WHERE node_id = ?
                ''', (disconnected_node_id,))
            self.db_conn.commit()
            
            logging.info(f"Registros de {disconnected_node_id} eliminados de url_db_log")
            
            # Para cada URL afectada, actualizar contador y re-replicar si es necesario
            for url_id in affected_urls:
                # Contar réplicas actuales (de nodos aún conectados)
                with self.status_lock:
                    connected_nodes = list(self.subordinates.keys())
                
                # Incluir al jefe
                if self.node_id not in connected_nodes:
                    connected_nodes.append(self.node_id)
                
                with self.db_lock:
                    # Contar réplicas en nodos conectados
                    if connected_nodes:
                        placeholders = ','.join(['?'] * len(connected_nodes))
                        self.db_cursor.execute(f'''
                            SELECT COUNT(DISTINCT node_id)
                            FROM url_db_log
                            WHERE url_id = ? AND node_id IN ({placeholders})
                        ''', (url_id, *connected_nodes))
                        current_replicas = self.db_cursor.fetchone()[0]
                    else:
                        current_replicas = 0
                    
                    # Actualizar contador en tabla urls
                    self.db_cursor.execute('''
                        UPDATE urls 
                        SET current_replicas = ?
                        WHERE url_id = ?
                    ''', (current_replicas, url_id))
                    self.db_conn.commit()
                
                logging.info(f"URL {url_id} ahora tiene {current_replicas} réplicas (era 1 más antes)")
                
                # Si necesita más réplicas, intentar re-replicar
                if current_replicas < 3:
                    logging.info(f"URL {url_id} necesita re-replicación")
                    self._check_and_rereplicate_url(url_id)
            
            logging.info(f"Desconexión de {disconnected_node_id} procesada completamente")
                
        except Exception as e:
            logging.error(f"Error manejando desconexión de {disconnected_node_id}: {e}")
            import traceback
            traceback.print_exc()
    
    def _on_subordinate_inherited_from_conflict(self, subordinate_node_id):
        """
        Override: Se llama cuando se hereda un subordinado de otro jefe en conflicto.
        En el caso de BD, necesitamos consolidar URLs duplicadas.
        
        Args:
            subordinate_node_id (str): ID del subordinado heredado
        """
        logging.info(f"🔄 Subordinado BD heredado: {subordinate_node_id}. Iniciando consolidación de URLs...")
        
        # Dar un breve momento para que el subordinado se estabilice
        threading.Timer(2.0, self._consolidate_duplicate_urls).start()

    def _check_and_rereplicate_url(self, url_id, exclude_node_id=None):
        """
        Verifica el número de réplicas activas de una URL y re-replica si es necesario.
        
        Args:
            url_id: ID de la URL a verificar
            exclude_node_id: Node ID a excluir de la lista de disponibles (nodo desconectándose)
        """
        try:
            # Contar réplicas activas actuales (considerando subordinados conectados)
            with self.status_lock:
                connected_nodes = list(self.subordinates.keys())
            
            # Agregar el jefe a la lista de nodos conectados
            # boss_node_id = f"{self.node_type}-{self.ip}:{self.port}"
            if self.node_id not in connected_nodes:
                connected_nodes.append(self.node_id)
            
            # Excluir el nodo desconectándose
            if exclude_node_id:
                connected_nodes = [n for n in connected_nodes if n != exclude_node_id]
            
            with self.db_lock:
                # Contar cuántas de esas nodes tienen la URL
                placeholders = ','.join(['?'] * len(connected_nodes))
                self.db_cursor.execute(f'''
                    SELECT COUNT(DISTINCT node_id)
                    FROM url_db_log
                    WHERE url_id = ? AND node_id IN ({placeholders})
                ''', (url_id, *connected_nodes))
                current_replicas = self.db_cursor.fetchone()[0]
                
                target_replicas = 3
                needed_replicas = target_replicas - current_replicas
                
                if needed_replicas <= 0:
                    # Ya hay suficientes réplicas
                    return
                
                logging.info(f"URL {url_id} tiene {current_replicas} réplicas, necesita {needed_replicas} más")
                
                # Obtener la URL y su contenido de un subordinado activo
                self.db_cursor.execute('''
                    SELECT u.url, udl.node_id
                    FROM urls u
                    JOIN url_db_log udl ON u.url_id = udl.url_id
                    WHERE u.url_id = ?
                    LIMIT 1
                ''', (url_id,))
                url_info = self.db_cursor.fetchone()
                
                if not url_info:
                    logging.error(f"No hay réplicas activas para URL {url_id}, no se puede re-replicar")
                    return
                
                url, source_node_id = url_info
            
            # Obtener subordinados activos actualmente conectados (excluir el que se está desconectando)
            with self.status_lock:
                connected_node_ids = [nid for nid in self.subordinates.keys() if nid != exclude_node_id]
            
            logging.info(f"[REREPLICATE] Subordinados conectados (excluyendo {exclude_node_id}): {connected_node_ids}")
            
            with self.db_lock:
                # Obtener subordinados que NO tienen esta URL Y están conectados
                self.db_cursor.execute('''
                    SELECT node_id 
                    FROM url_db_log 
                    WHERE url_id = ?
                ''', (url_id,))
                nodes_with_url = [row[0] for row in self.db_cursor.fetchall()]
            
            available_subordinates = [nid for nid in connected_node_ids if nid not in nodes_with_url]
            
            if not available_subordinates:
                logging.warning(f"[REREPLICATE] No hay subordinados disponibles para re-replicar URL {url_id}. Conectados: {len(connected_node_ids)}, Disponibles sin URL: 0")
                return
            
            # Seleccionar subordinados para re-replicar (hasta completar 3 réplicas)
            random.shuffle(available_subordinates)
            targets = available_subordinates[:needed_replicas]
            
            logging.info(f"[REREPLICATE] Targets seleccionados para URL {url_id}: {targets}")
            
            # Solicitar contenido al subordinado fuente y enviarlo a los targets
            self._request_and_replicate_content(url, url_id, source_node_id, targets)
            
        except Exception as e:
            logging.error(f"Error verificando/re-replicando URL {url_id}: {e}")
            import traceback
            traceback.print_exc()

    def _request_and_replicate_content(self, url, url_id, source_node_id, target_subordinates):
        """
        Solicita que un subordinado fuente replique contenido a otros subordinados.
        El líder NO maneja el contenido, solo coordina la replicación.
        
        Args:
            url: URL a replicar
            url_id: ID de la URL
            source_node_id: ID del subordinado que tiene el contenido
            target_subordinates: Lista de node_ids para replicar
        """
        try:
            # Buscar conexión con el subordinado fuente
            source_conn = None
            with self.status_lock:
                for node_id, conn in self.subordinates.items():
                    if source_node_id in node_id:
                        source_conn = conn
                        break
            
            if not source_conn:
                logging.error(f"No hay conexión con subordinado fuente {source_node_id}")
                return
            
            # Preparar lista de subordinados destino con sus IPs
            targets_info = []
            for node_id in target_subordinates:
                # Extraer IP del node_id (formato: "bd-IP:PORT")
                if '-' in node_id and ':' in node_id:
                    ip = node_id.split('-')[1].split(':')[0]
                    targets_info.append({
                        'node_id': node_id,
                        'ip': ip
                    })
            
            if not targets_info:
                logging.warning(f"No se pudo obtener info de subordinados destino")
                return
            
            logging.info(f"Solicitando a {source_node_id} que replique URL {url} a {len(targets_info)} subordinados")
            
            # Enviar mensaje REPLICATE_CONTENT al subordinado fuente
            replicate_message = self._create_message(
                MessageProtocol.MESSAGE_TYPES['REPLICATE_CONTENT'],
                {
                    'url': url,
                    'url_id': url_id,
                    'targets': targets_info  # Lista de destinos
                }
            )
            
            source_conn.send_message(replicate_message)
            logging.info(f"REPLICATE_CONTENT enviado a {source_node_id}")
            
            # Registrar optimistamente que los destinos tendrán el contenido
            for target_info in targets_info:
                target_node_id = target_info['node_id']
                self.db_cursor.execute('''
                    INSERT OR IGNORE INTO url_db_log (url_id, node_id)
                    VALUES (?, ?)
                ''', (url_id, target_node_id))
            
            self.db_conn.commit()
            
            # Actualizar contador de réplicas
            self.db_cursor.execute('''
                SELECT COUNT(DISTINCT node_id)
                FROM url_db_log
                WHERE url_id = ?
            ''', (url_id,))
            new_count = self.db_cursor.fetchone()[0]
            
            self.db_cursor.execute('''
                UPDATE urls 
                SET current_replicas = ?
                WHERE url_id = ?
            ''', (new_count, url_id))
            self.db_conn.commit()
            
            logging.info(f"URL {url} ahora tendrá {new_count} réplicas activas")
            
        except Exception as e:
            logging.error(f"Error solicitando replicación de contenido: {e}")
            import traceback
            traceback.print_exc()

    def _handle_replicate_content_request(self, node_connection, message):
        """
        Subordinado fuente recibe solicitud del líder para replicar contenido a otros subordinados.
        Lee el contenido local y lo envía directamente a los destinos.
        
        Args:
            node_connection: Conexión con el líder BD
            message: Mensaje con URL y lista de destinos
        """
        data = message.get('data', {})
        url = data.get('url')
        url_id = data.get('url_id')
        targets = data.get('targets', [])
        
        logging.info(f"REPLICATE_CONTENT recibido: replicar {url} a {len(targets)} subordinados")
        
        try:
            # Obtener contenido de la BD local
            self.db_cursor.execute('SELECT url_id FROM urls WHERE url = ?', (url,))
            url_id_row = self.db_cursor.fetchone()
            
            if not url_id_row:
                logging.error(f"URL {url} no encontrada en subordinado fuente")
                return
            
            local_url_id = url_id_row[0]
            
            # Obtener contenido
            self.db_cursor.execute('SELECT content, scrapped_at FROM urls WHERE url_id = ?', (local_url_id,))
            content_row = self.db_cursor.fetchone()
            
            if not content_row:
                logging.error(f"Contenido no encontrado para {url} en subordinado fuente")
                return
            
            content_json, scrapped_at = content_row
            
            # Deserializar el contenido
            import json
            result = json.loads(content_json)
            
            logging.info(f"Contenido de {url} obtenido, replicando a destinos...")
            
            # Enviar a cada destino
            for target_info in targets:
                target_node_id = target_info.get('node_id')
                
                # Buscar conexión con el subordinado destino
                target_conn = None
                with self.status_lock:
                    for node_id, conn in self.subordinates.items():
                        if target_node_id in node_id:
                            target_conn = conn
                            break
                
                if target_conn:
                    # Enviar SAVE_DATA_NO_LEADER al destino
                    save_message = self._create_message(
                        MessageProtocol.MESSAGE_TYPES['SAVE_DATA_NO_LEADER'],
                        {
                            'url': url,
                            'result': result,
                            'completed_at': scrapped_at,
                            'task_id': f'rereplicate-{url_id}'
                        }
                    )
                    
                    try:
                        target_conn.send_message(save_message)
                        logging.info(f"Contenido replicado a subordinado {target_node_id}")
                    except Exception as e:
                        logging.error(f"Error enviando contenido a {target_node_id}: {e}")
                else:
                    logging.warning(f"No hay conexión con subordinado destino {target_node_id}")
            
            logging.info(f"Replicación de {url} completada")
            
        except Exception as e:
            logging.error(f"Error manejando REPLICATE_CONTENT: {e}")
            import traceback
            traceback.print_exc()

    def _recover_metadata_from_subordinates(self):
        """
        Nuevo jefe BD solicita inventario de URLs a todos los subordinados
        para reconstruir las tablas url_db_log y urls.
        """
        logging.info("=" * 60)
        logging.info("RECUPERACIÓN DE METADATA: Nuevo jefe BD solicitando inventario")
        logging.info("=" * 60)
        
        # Contador para esperar respuestas
        self.inventory_responses = {}
        self.inventory_complete = threading.Event()
        
        with self.status_lock:
            subordinate_count = len(self.subordinates)
            subordinate_list = list(self.subordinates.items())
        
        if subordinate_count == 0:
            logging.info("No hay subordinados, no se necesita recuperación de metadata")
            return
        
        logging.info(f"Solicitando inventario a {subordinate_count} subordinados...")
        
        # Enviar REQUEST_URL_INVENTORY a todos los subordinados
        request_message = self._create_message(
            MessageProtocol.MESSAGE_TYPES['REQUEST_URL_INVENTORY'],
            {}
        )
        
        for node_id, conn in subordinate_list:
            try:
                conn.send_message(request_message)
                self.inventory_responses[node_id] = None  # Marcar como pendiente
                logging.info(f"Inventario solicitado a {node_id}")
            except Exception as e:
                logging.error(f"Error solicitando inventario a {node_id}: {e}")
        
        # Esperar respuestas (timeout 10 segundos)
        self.inventory_complete.wait(timeout=10)
        
        # Reconstruir tablas con las respuestas recibidas
        self._rebuild_tables_from_inventory()

    def _handle_request_url_inventory(self, node_connection, message):
        """
        Subordinado recibe solicitud de inventario del nuevo jefe.
        Responde con lista de todas las URLs que tiene almacenadas.
        """
        logging.info("REQUEST_URL_INVENTORY recibido del nuevo jefe, preparando inventario...")
        
        try:
            # Obtener todas las URLs que tienen contenido
            self.db_cursor.execute('''
                SELECT url_id, url, content, scrapped_at
                FROM urls
                WHERE content IS NOT NULL
            ''')
            urls = [{
                'url_id': row[0], 
                'url': row[1],
                'content': row[2],
                'scrapped_at': row[3]
            } for row in self.db_cursor.fetchall()]
            
            logging.info(f"Inventario preparado: {len(urls)} URLs almacenadas")
            
            # Enviar inventario al jefe
            inventory_message = self._create_message(
                MessageProtocol.MESSAGE_TYPES['REPORT_URL_INVENTORY'],
                {
                    'urls': urls,
                    'node_id': node_connection.node_id  # Mi identificador
                }
            )
            
            node_connection.send_message(inventory_message)
            logging.info(f"Inventario enviado al jefe: {len(urls)} URLs")
            
        except Exception as e:
            logging.error(f"Error preparando inventario: {e}")
            import traceback
            traceback.print_exc()

    def _handle_report_url_inventory(self, node_connection, message):
        """
        Jefe recibe reporte de inventario de un subordinado.
        Almacena temporalmente para reconstruir tablas cuando todos respondan.
        """
        data = message.get('data', {})
        urls = data.get('urls', [])
        reporter_node_id = message.get('sender_id')
        
        logging.info(f"REPORT_URL_INVENTORY recibido de {reporter_node_id}: {len(urls)} URLs")
        
        # Almacenar respuesta
        if hasattr(self, 'inventory_responses'):
            self.inventory_responses[reporter_node_id] = urls
            
            # Verificar si todas las respuestas llegaron
            all_responded = all(v is not None for v in self.inventory_responses.values())
            
            if all_responded:
                logging.info("Todas las respuestas de inventario recibidas")
                self.inventory_complete.set()

    def _rebuild_tables_from_inventory(self):
        """
        Reconstruye las tablas del jefe BD usando los inventarios reportados por subordinados.
        """
        logging.info("=" * 60)
        logging.info("RECONSTRUYENDO TABLAS desde inventarios de subordinados")
        logging.info("=" * 60)
        
        if not hasattr(self, 'inventory_responses'):
            logging.error("No hay respuestas de inventario")
            return
        
        try:
            # Ya no hay tabla databases, simplificar flujo
            logging.info("Reconstruyendo tablas urls y url_db_log desde inventario...")
            
            url_replica_count = {}  # Para contar réplicas por URL
            
            for node_id, urls in self.inventory_responses.items():
                if urls is None:
                    logging.warning(f"Subordinado {node_id} no respondió a tiempo")
                    continue
                
                # Para cada URL reportada por este subordinado
                for url_info in urls:
                    url = url_info['url']
                    content = url_info.get('content')
                    scrapped_at = url_info.get('scrapped_at')
                    
                    # Insertar URL con contenido en tabla urls si no existe
                    # Si ya existe, actualizar content y scrapped_at si vienen con datos
                    self.db_cursor.execute('SELECT url_id FROM urls WHERE url = ?', (url,))
                    existing = self.db_cursor.fetchone()
                    
                    if existing:
                        url_id = existing[0]
                        # Si esta URL ya existe pero no tiene contenido, actualizarlo
                        if content is not None:
                            self.db_cursor.execute('''
                                UPDATE urls 
                                SET content = COALESCE(content, ?),
                                    scrapped_at = COALESCE(scrapped_at, ?)
                                WHERE url_id = ?
                            ''', (content, scrapped_at, url_id))
                    else:
                        # Insertar nueva URL con contenido
                        self.db_cursor.execute('''
                            INSERT INTO urls (url, content, scrapped_at)
                            VALUES (?, ?, ?)
                        ''', (url, content, scrapped_at))
                        url_id = self.db_cursor.lastrowid
                    
                    self.db_conn.commit()
                    
                    # Registrar en url_db_log con node_id directamente
                    self.db_cursor.execute('''
                        INSERT OR IGNORE INTO url_db_log (url_id, node_id)
                        VALUES (?, ?)
                    ''', (url_id, node_id))
                    
                    # Contar réplicas
                    url_replica_count[url_id] = url_replica_count.get(url_id, 0) + 1
            
            self.db_conn.commit()
            logging.info(f"Tablas urls y url_db_log reconstruidas con {len(url_replica_count)} URLs únicas")
            
            # Actualizar contadores de réplicas en urls
            logging.info("Actualizando contadores de réplicas...")
            for url_id, replica_count in url_replica_count.items():
                self.db_cursor.execute('''
                    UPDATE urls
                    SET current_replicas = ?, target_replicas = 3
                    WHERE url_id = ?
                ''', (replica_count, url_id))
            
            self.db_conn.commit()
            logging.info(f"Contadores de réplicas actualizados")
            
            # Resumen
            logging.info("=" * 60)
            logging.info(f"RECUPERACIÓN COMPLETADA:")
            logging.info(f"  - {len(self.inventory_responses)} subordinados conectados")
            logging.info(f"  - {len(url_replica_count)} URLs únicas encontradas")
            logging.info(f"  - Total de réplicas: {sum(url_replica_count.values())}")
            logging.info("=" * 60)
            
        except Exception as e:
            logging.error(f"Error reconstruyendo tablas: {e}")
            import traceback
            traceback.print_exc()

    def _register_own_urls_in_log(self):
        """
        Registra las URLs que el jefe tiene en su propia base de datos en la tabla url_db_log.
        Esto asegura que el contador de réplicas sea correcto.
        """
        logging.info("=" * 60)
        logging.info("REGISTRO PROPIO: URLs del jefe en url_db_log")
        logging.info("=" * 60)
        
        try:
            # Obtener el node_id del jefe
            boss_node_id = f"{self.node_type}-{self.ip}:{self.port}"
            
            # Obtener todas las URLs que el jefe tiene con contenido
            self.db_cursor.execute('''
                SELECT url_id, url
                FROM urls
                WHERE content IS NOT NULL
            ''')
            own_urls = self.db_cursor.fetchall()
            
            if not own_urls:
                logging.info("El jefe no tiene URLs propias con contenido")
                return
            
            logging.info(f"Registrando {len(own_urls)} URLs del jefe en url_db_log...")
            
            registered_count = 0
            for url_id, url in own_urls:
                # Insertar en url_db_log con node_id
                self.db_cursor.execute('''
                    INSERT OR IGNORE INTO url_db_log (url_id, node_id)
                    VALUES (?, ?)
                ''', (url_id, boss_node_id))
                
                if self.db_cursor.rowcount > 0:
                    registered_count += 1
                    
                    # Actualizar contador de réplicas
                    self.db_cursor.execute('''
                        SELECT COUNT(DISTINCT node_id)
                        FROM url_db_log
                        WHERE url_id = ?
                    ''', (url_id,))
                    new_count = self.db_cursor.fetchone()[0]
                    
                    self.db_cursor.execute('''
                        UPDATE urls 
                        SET current_replicas = ?
                        WHERE url_id = ?
                    ''', (new_count, url_id))
                    
                    logging.info(f"URL {url} registrada, ahora tiene {new_count} réplicas")
            
            self.db_conn.commit()
            
            logging.info("=" * 60)
            logging.info(f"REGISTRO COMPLETADO: {registered_count} URLs registradas")
            logging.info("=" * 60)
            
        except Exception as e:
            logging.error(f"Error registrando URLs propias: {e}")
            import traceback
            traceback.print_exc()
    
    def _consolidate_duplicate_urls(self):
        """
        Consolida URLs duplicadas entre subordinados manteniendo el contenido más reciente.
        
        Cuando dos clusters independientes se fusionan, pueden tener la misma URL
        con diferentes contenidos. Este método:
        1. Identifica URLs duplicadas (mismo URL, diferentes scrapped_at)
        2. Mantiene el contenido con la fecha más reciente
        3. Actualiza todos los nodos que tengan versiones más antiguas
        """
        logging.info("=" * 60)
        logging.info("CONSOLIDACIÓN: Detectando y resolviendo URLs duplicadas")
        logging.info("=" * 60)
        
        try:
            # Recolectar información de todas las URLs con contenido de todos los nodos
            # Estructura: {url: [(node_id, scrapped_at, content), ...]}
            url_versions = {}
            
            # 1. Incluir las URLs del jefe
            self.db_cursor.execute('''
                SELECT url, scrapped_at, content
                FROM urls
                WHERE content IS NOT NULL AND scrapped_at IS NOT NULL
            ''')
            
            for url, scrapped_at, content in self.db_cursor.fetchall():
                if url not in url_versions:
                    url_versions[url] = []
                url_versions[url].append((self.node_id, scrapped_at, content))
            
            # 2. Solicitar información detallada de subordinados
            logging.info(f"Solicitando información de URLs a subordinados...")
            
            with self.status_lock:
                subordinate_list = list(self.subordinates.items())
            
            # Usar el mecanismo de inventario existente
            self.inventory_responses = {}
            self.inventory_complete = threading.Event()
            
            request_message = self._create_message(
                MessageProtocol.MESSAGE_TYPES['REQUEST_URL_INVENTORY'],
                {}
            )
            
            for node_id, conn in subordinate_list:
                try:
                    conn.send_message(request_message)
                    self.inventory_responses[node_id] = None
                except Exception as e:
                    logging.error(f"Error solicitando inventario a {node_id}: {e}")
            
            # Esperar respuestas
            self.inventory_complete.wait(timeout=10)
            
            # 3. Agregar información de subordinados
            for node_id, urls in self.inventory_responses.items():
                if urls is None:
                    continue
                
                for url_info in urls:
                    url = url_info['url']
                    scrapped_at = url_info.get('scrapped_at')
                    content = url_info.get('content')
                    
                    if content and scrapped_at:
                        if url not in url_versions:
                            url_versions[url] = []
                        url_versions[url].append((node_id, scrapped_at, content))
            
            # 4. Detectar y resolver duplicados
            duplicates_found = 0
            updates_needed = 0
            
            for url, versions in url_versions.items():
                if len(versions) <= 1:
                    continue  # No hay duplicados
                
                # Ordenar por fecha (más reciente primero)
                versions.sort(key=lambda x: x[1], reverse=True)  # x[1] es scrapped_at
                
                newest_node, newest_date, newest_content = versions[0]
                
                # Verificar si hay versiones diferentes
                has_different_versions = any(
                    version[1] != newest_date or version[2] != newest_content
                    for version in versions[1:]
                )
                
                if not has_different_versions:
                    continue  # Todas las versiones son iguales
                
                duplicates_found += 1
                logging.warning(f"⚠️  URL duplicada detectada: {url}")
                logging.warning(f"   Versión más reciente: {newest_date} (nodo: {newest_node})")
                logging.warning(f"   Versiones más antiguas: {len(versions) - 1}")
                
                # 5. Actualizar todos los nodos que tengan versiones más antiguas
                for node_id, scrapped_at, content in versions[1:]:
                    if scrapped_at != newest_date or content != newest_content:
                        updates_needed += 1
                        logging.info(f"   → Actualizando {node_id} con versión más reciente")
                        
                        # Enviar UPDATE_URL_CONTENT a ese nodo
                        if node_id == self.node_id:
                            # Actualizar mi propia BD
                            self._update_own_url_content(url, newest_content, newest_date)
                        else:
                            # Enviar mensaje al subordinado
                            self._send_url_update_to_subordinate(
                                node_id, url, newest_content, newest_date
                            )
            
            # Resumen
            logging.info("=" * 60)
            logging.info(f"CONSOLIDACIÓN COMPLETADA:")
            logging.info(f"  - URLs analizadas: {len(url_versions)}")
            logging.info(f"  - Duplicados encontrados: {duplicates_found}")
            logging.info(f"  - Actualizaciones realizadas: {updates_needed}")
            logging.info("=" * 60)
            
            # 6. Verificar y balancear réplicas (eliminar excesos)
            logging.info("=" * 60)
            logging.info("BALANCEO: Verificando y ajustando número de réplicas")
            logging.info("=" * 60)
            self._balance_replica_count(url_versions)
            
        except Exception as e:
            logging.error(f"Error consolidando URLs duplicadas: {e}")
            import traceback
            traceback.print_exc()
    
    def _balance_replica_count(self, url_versions):
        """
        Balancea el número de réplicas para cada URL.
        Si hay más de target_replicas (3), elimina réplicas al azar hasta tener exactamente 3.
        
        Args:
            url_versions: Dict {url: [(node_id, scrapped_at, content), ...]}
        """
        target_replicas = 3
        excess_removed = 0
        
        try:
            for url, versions in url_versions.items():
                current_replicas = len(versions)
                
                if current_replicas <= target_replicas:
                    continue  # No hay exceso
                
                excess = current_replicas - target_replicas
                logging.warning(f"⚠️  URL {url} tiene {current_replicas} réplicas (exceso: {excess})")
                
                # Seleccionar nodos al azar para eliminar
                # Hacer una copia para no modificar el original
                import random
                nodes_to_remove = random.sample(versions, excess)
                
                logging.info(f"   → Eliminando {excess} réplicas al azar...")
                
                for node_id, _, _ in nodes_to_remove:
                    excess_removed += 1
                    logging.info(f"   → Eliminando réplica de {node_id}")
                    
                    if node_id == self.node_id:
                        # Eliminar de mi propia BD
                        self._delete_own_url_content(url)
                    else:
                        # Enviar mensaje al subordinado
                        self._send_url_deletion_to_subordinate(node_id, url)
            
            # Resumen
            logging.info("=" * 60)
            logging.info(f"BALANCEO COMPLETADO:")
            logging.info(f"  - Réplicas excesivas eliminadas: {excess_removed}")
            logging.info("=" * 60)
            
        except Exception as e:
            logging.error(f"Error balanceando réplicas: {e}")
            import traceback
            traceback.print_exc()
    
    def _delete_own_url_content(self, url):
        """Elimina una URL de la BD del jefe"""
        try:
            with self.db_lock:
                # Obtener url_id antes de eliminar
                self.db_cursor.execute('SELECT url_id FROM urls WHERE url = ?', (url,))
                result = self.db_cursor.fetchone()
                
                if not result:
                    logging.warning(f"URL {url} no encontrada para eliminar")
                    return
                
                url_id = result[0]
                
                # Eliminar de url_db_log
                self.db_cursor.execute('''
                    DELETE FROM url_db_log
                    WHERE url_id = ? AND node_id = ?
                ''', (url_id, self.node_id))
                
                # Eliminar contenido de urls (pero mantener el registro si hay otras réplicas)
                # Verificar si hay otras réplicas
                self.db_cursor.execute('''
                    SELECT COUNT(*) FROM url_db_log WHERE url_id = ?
                ''', (url_id,))
                remaining_replicas = self.db_cursor.fetchone()[0]
                
                if remaining_replicas == 0:
                    # No hay más réplicas, eliminar completamente
                    self.db_cursor.execute('DELETE FROM urls WHERE url_id = ?', (url_id,))
                    logging.info(f"✓ URL {url} eliminada completamente (última réplica)")
                else:
                    # Hay otras réplicas, solo limpiar contenido local si es necesario
                    logging.info(f"✓ Réplica local de {url} eliminada ({remaining_replicas} réplicas restantes)")
                
                self.db_conn.commit()
                
        except Exception as e:
            logging.error(f"Error eliminando contenido local de {url}: {e}")
    
    def _send_url_deletion_to_subordinate(self, node_id, url):
        """Envía solicitud de eliminación de URL a un subordinado"""
        try:
            with self.status_lock:
                conn = self.subordinates.get(node_id)
            
            if not conn:
                logging.error(f"Subordinado {node_id} no encontrado para eliminar {url}")
                return
            
            delete_message = self._create_message(
                MessageProtocol.MESSAGE_TYPES['DELETE_URL_CONTENT'],
                {'url': url}
            )
            
            conn.send_message(delete_message)
            logging.info(f"✓ Solicitud de eliminación enviada a {node_id} para {url}")
            
        except Exception as e:
            logging.error(f"Error enviando eliminación a {node_id} para {url}: {e}")
    
    def _update_own_url_content(self, url, content, scrapped_at):
        """Actualiza el contenido de una URL en la BD del jefe"""
        try:
            self.db_cursor.execute('''
                UPDATE urls
                SET content = ?, scrapped_at = ?
                WHERE url = ?
            ''', (content, scrapped_at, url))
            self.db_conn.commit()
            logging.info(f"✓ Contenido actualizado localmente para {url}")
        except Exception as e:
            logging.error(f"Error actualizando contenido local de {url}: {e}")
    
    def _send_url_update_to_subordinate(self, node_id, url, content, scrapped_at):
        """Envía actualización de contenido a un subordinado"""
        try:
            with self.status_lock:
                conn = self.subordinates.get(node_id)
            
            if not conn:
                logging.error(f"Subordinado {node_id} no encontrado para actualizar {url}")
                return
            
            update_message = self._create_message(
                MessageProtocol.MESSAGE_TYPES['UPDATE_URL_CONTENT'],
                {
                    'url': url,
                    'content': content,
                    'scrapped_at': scrapped_at
                }
            )
            
            conn.send_message(update_message)
            logging.info(f"✓ Actualización enviada a {node_id} para {url}")
            
        except Exception as e:
            logging.error(f"Error enviando actualización a {node_id} para {url}: {e}")

    def _redistribute_own_urls(self):
        """
        Redistribuye las URLs que el nuevo jefe tenía almacenadas cuando era subordinado.
        Asegura que haya 3 réplicas de cada URL sin duplicar en subordinados.
        """
        logging.info("=" * 60)
        logging.info("REDISTRIBUCIÓN: URLs del jefe a subordinados")
        logging.info("=" * 60)
        
        try:
            # Obtener URLs que este nodo tiene con contenido
            self.db_cursor.execute('''
                SELECT url_id, url, content, scrapped_at
                FROM urls
                WHERE content IS NOT NULL
            ''')
            own_urls = self.db_cursor.fetchall()
            
            if not own_urls:
                logging.info("Este nodo no tenía URLs almacenadas, no se necesita redistribución")
                return
            
            logging.info(f"Redistribuyendo {len(own_urls)} URLs que tenía este nodo...")
            
            for url_id, url, content_json, scrapped_at in own_urls:
                # Contar cuántas réplicas hay en subordinados (sin incluir este nodo)
                self.db_cursor.execute('''
                    SELECT COUNT(DISTINCT node_id)
                    FROM url_db_log
                    WHERE url_id = ?
                ''', (url_id,))
                current_replicas = self.db_cursor.fetchone()[0]
                
                needed_replicas = 3 - current_replicas
                
                if needed_replicas <= 0:
                    logging.info(f"URL {url} ya tiene {current_replicas} réplicas, no se necesita redistribuir")
                    continue
                
                logging.info(f"URL {url} tiene {current_replicas} réplicas, necesita {needed_replicas} más")
                
                # Obtener subordinados que NO tienen esta URL
                with self.status_lock:
                    connected_node_ids = list(self.subordinates.keys())
                
                self.db_cursor.execute('''
                    SELECT node_id 
                    FROM url_db_log 
                    WHERE url_id = ?
                ''', (url_id,))
                nodes_with_url = [row[0] for row in self.db_cursor.fetchall()]
                
                available_subordinates = [nid for nid in connected_node_ids if nid not in nodes_with_url]
                
                if not available_subordinates:
                    logging.warning(f"No hay subordinados disponibles para redistribuir URL {url}")
                    continue
                
                # Seleccionar targets
                random.shuffle(available_subordinates)
                targets = available_subordinates[:needed_replicas]
                
                logging.info(f"Redistribuyendo URL {url} a {len(targets)} subordinados: {targets}")
                
                # Deserializar contenido
                result = json.loads(content_json)
                
                # Enviar a cada subordinado
                for node_id in targets:
                    # Buscar conexión
                    target_conn = None
                    with self.status_lock:
                        for nid, conn in self.subordinates.items():
                            if node_id in nid:
                                target_conn = conn
                                break
                    
                    if target_conn:
                        # Enviar SAVE_DATA_NO_LEADER
                        save_message = self._create_message(
                            MessageProtocol.MESSAGE_TYPES['SAVE_DATA_NO_LEADER'],
                            {
                                'url': url,
                                'result': result,
                                'completed_at': scrapped_at,
                                'task_id': f'redistribute-{url_id}'
                            }
                        )
                        
                        try:
                            target_conn.send_message(save_message)
                            
                            # Registrar en url_db_log
                            self.db_cursor.execute('''
                                INSERT OR IGNORE INTO url_db_log (url_id, node_id)
                                VALUES (?, ?)
                            ''', (url_id, node_id))
                            
                            logging.info(f"URL {url} redistribuida a {node_id}")
                        except Exception as e:
                            logging.error(f"Error redistribuyendo a {node_id}: {e}")
                    else:
                        logging.warning(f"No hay conexión con subordinado {node_id}")
                
                self.db_conn.commit()
                
                # Actualizar contador de réplicas
                self.db_cursor.execute('''
                    SELECT COUNT(DISTINCT node_id)
                    FROM url_db_log
                    WHERE url_id = ?
                ''', (url_id,))
                new_count = self.db_cursor.fetchone()[0]
                
                self.db_cursor.execute('''
                    UPDATE urls
                    SET current_replicas = ?, target_replicas = 3
                    WHERE url_id = ?
                ''', (new_count, url_id))
                self.db_conn.commit()
            
            # El jefe ahora también almacena contenido, NO borrar url_content
            logging.info("=" * 60)
            logging.info(f"REDISTRIBUCIÓN COMPLETADA: {len(own_urls)} URLs procesadas")
            logging.info("=" * 60)
            
        except Exception as e:
            logging.error(f"Error redistribuyendo URLs propias: {e}")
            import traceback
            traceback.print_exc()

    def add_subordinate(self, node_ip, existing_socket=None):
        """
        Sobrescribe add_subordinate para iniciar replicación automática cuando se une un nuevo BD.
        
        Args:
            node_ip (str): IP del nodo subordinado
            existing_socket (socket.socket, optional): Socket ya conectado
        
        Returns:
            bool: True si se agregó exitosamente, False en caso contrario
        """
        # Llamar a la implementación base
        success = super().add_subordinate(node_ip, existing_socket)
        
        if not success:
            return False
        
        # Si soy jefe BD y el subordinado se agregó exitosamente, iniciar replicación
        if self.i_am_boss:
            node_id = f"{self.node_type}-{node_ip}:{self.port}"
            logging.info(f"[NEW_SUB] Nuevo subordinado BD {node_id} agregado, iniciando replicación automática")
            
            # Ejecutar replicación en un thread separado para no bloquear
            threading.Thread(
                target=self._replicate_to_new_subordinate,
                args=(node_id,),
                daemon=True
            ).start()
        
        return True

    def _replicate_to_new_subordinate(self, new_subordinate_id):
        """
        Replica datos que necesiten más réplicas hacia el nuevo subordinado.
        
        Args:
            new_subordinate_id: ID del nuevo subordinado BD
        """
        try:
            # Esperar un momento para que el subordinado termine de inicializar
            time.sleep(2)
            
            logging.info(f"[REPLICATE_NEW_SUB] Iniciando replicación hacia nuevo subordinado {new_subordinate_id}")
            
            # Obtener todas las URLs que necesitan más réplicas
            self.db_cursor.execute('''
                SELECT url_id, url
                FROM urls
                WHERE current_replicas < target_replicas
            ''')
            urls_needing_replication = self.db_cursor.fetchall()
            
            if not urls_needing_replication:
                logging.info(f"[REPLICATE_NEW_SUB] No hay URLs que necesiten replicación")
                return
            
            logging.info(f"[REPLICATE_NEW_SUB] Encontradas {len(urls_needing_replication)} URLs que necesitan replicación")
            
            # Para cada URL que necesita replicación
            replicated_count = 0
            for url_id, url in urls_needing_replication:
                try:
                    # Verificar si el nuevo subordinado ya tiene esta URL
                    self.db_cursor.execute('''
                        SELECT 1 FROM url_db_log 
                        WHERE url_id = ? AND node_id = ?
                    ''', (url_id, new_subordinate_id))
                    
                    if self.db_cursor.fetchone():
                        # Ya tiene la URL, saltar
                        continue
                    
                    # Obtener el contenido directamente de la tabla urls
                    self.db_cursor.execute('''
                        SELECT content, scrapped_at
                        FROM urls
                        WHERE url_id = ?
                    ''', (url_id,))
                    
                    content_row = self.db_cursor.fetchone()
                    if not content_row:
                        logging.warning(f"[REPLICATE_NEW_SUB] No se encontró contenido para url_id={url_id}")
                        continue
                    
                    content_json, scrapped_at = content_row
                    
                    # Enviar contenido al nuevo subordinado
                    with self.status_lock:
                        if new_subordinate_id in self.subordinates:
                            subordinate_conn = self.subordinates[new_subordinate_id]
                            
                            import json
                            result = json.loads(content_json)
                            
                            # Usar SAVE_DATA_NO_LEADER para que el subordinado guarde
                            replicate_msg = self._create_message(
                                MessageProtocol.MESSAGE_TYPES['SAVE_DATA_NO_LEADER'],
                                {
                                    'url': url,
                                    'result': result,
                                    'completed_at': scrapped_at,
                                    'task_id': f'replicate-{url_id}'
                                }
                            )
                            
                            subordinate_conn.send_message(replicate_msg)
                            
                            # Registrar en url_db_log con node_id
                            self.db_cursor.execute('''
                                INSERT INTO url_db_log (url_id, node_id, added_at)
                                VALUES (?, ?, datetime('now'))
                            ''', (url_id, new_subordinate_id))
                            
                            # Actualizar contador de réplicas
                            self.db_cursor.execute('''
                                UPDATE urls 
                                SET current_replicas = current_replicas + 1
                                WHERE url_id = ?
                            ''', (url_id,))
                            
                            self.db_conn.commit()
                            replicated_count += 1
                            
                            logging.info(f"[REPLICATE_NEW_SUB] URL {url} replicada a {new_subordinate_id}")
                        else:
                            logging.warning(f"[REPLICATE_NEW_SUB] Subordinado {new_subordinate_id} ya no está conectado")
                            break
                
                except Exception as e:
                    logging.error(f"[REPLICATE_NEW_SUB] Error replicando url_id={url_id}: {e}")
                    continue
            
            logging.info(f"[REPLICATE_NEW_SUB] Replicación completada: {replicated_count} URLs replicadas a {new_subordinate_id}")
                
        except Exception as e:
            logging.error(f"[REPLICATE_NEW_SUB] Error en replicación automática: {e}")
            import traceback
            traceback.print_exc()

    def _register_bd_handlers(self):
        '''Registrar handlers para mensajes especificos de bd'''
        
        # Handler para query de url que le manda el Router (conexion persistente)
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['BD_QUERY'],
            self._url_query_leader
        )

        # Handler para query de url entre nodos BD (líder -> subordinado)
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['URL_QUERY'],
            self._url_query_noleader
        )

        # Handler para guardar datos desde Scrapper (solo líder)
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['SAVE_DATA'],
            self._recive_task_result
        )

        # Handler para guardar datos desde líder BD (solo subordinados)
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['SAVE_DATA_NO_LEADER'],
            self._recive_task_result_no_leader
        )

        # Handler para replicar contenido entre subordinados
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['REPLICATE_CONTENT'],
            self._handle_replicate_content_request
        )

        # Handler para solicitud de inventario (subordinados)
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['REQUEST_URL_INVENTORY'],
            self._handle_request_url_inventory
        )

        # Handler para recibir inventario (jefe)
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['REPORT_URL_INVENTORY'],
            self._handle_report_url_inventory
        )

        # Handler para listar tablas (visualización)
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['LIST_TABLES'],
            self._handle_list_tables
        )

        # Handler para obtener datos paginados de tabla
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['GET_TABLE_DATA'],
            self._handle_get_table_data
        )
        
        # Handler para actualizar contenido de URL (consolidación)
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['UPDATE_URL_CONTENT'],
            self._handle_update_url_content
        )
        
        # Handler para eliminar contenido de URL (balanceo de réplicas)
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['DELETE_URL_CONTENT'],
            self._handle_delete_url_content
        )

    def init_database(self):
        '''Inicializa la base de datos'''
        try:
            # crear directorio si no existe
            Path(self.data_dir).mkdir(parents=None, exist_ok=True)

            # base de datos para las url
            db_path = f"{self.data_dir}/{self.name}.db"
            self.db_conn = sqlite3.connect(db_path, check_same_thread=False)
            self.db_cursor = self.db_conn.cursor()

            # Tabla única de urls con todo el contenido y contadores de réplicas
            self.db_cursor.execute('''
                CREATE TABLE IF NOT EXISTS urls (
                    url_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE NOT NULL,
                    content TEXT,
                    firstseen DATETIME DEFAULT CURRENT_TIMESTAMP,
                    scrapped_at DATETIME,
                    current_replicas INTEGER DEFAULT 0,
                    target_replicas INTEGER DEFAULT 3
                )
            ''')

            # tabla para guardar la tupla url-database, que nos dira en cuales bases de datos se guardo una url
            # tabla para guardar la tupla url-node_id
            self.db_cursor.execute('''
                CREATE TABLE IF NOT EXISTS url_db_log (
                    location_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url_id INTEGER NOT NULL,
                    node_id TEXT NOT NULL,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(url_id) REFERENCES urls(url_id) ON DELETE CASCADE,
                    UNIQUE(url_id, node_id)
                )
            ''')

            self.db_conn.commit()
            self.db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tablas = self.db_cursor.fetchall()
            logging.info(f"Base de datos inicializada en {self.data_dir}")
            logging.info(f"Tablas: {tablas}")
            
        
        except Exception as e:
            logging.error(f"Error inicilizando la base de datos: {e}")

    def _register_url_in_subordinates(self, url, node_ids):
        '''Registra en las tablas de log que ciertos subordinados tienen el contenido de una URL'''
        try:
            # Insertar URL en tabla urls si no existe
            self.db_cursor.execute('INSERT OR IGNORE INTO urls (url) VALUES (?)', (url,))
            self.db_conn.commit()

            # Obtener url_id
            self.db_cursor.execute('SELECT url_id FROM urls WHERE url = ?', (url,))
            url_id_row = self.db_cursor.fetchone()
            if not url_id_row:
                raise Exception(f"No se pudo obtener url_id para {url}")
            url_id = url_id_row[0]

            # Registrar en url_db_log cada subordinado que tiene el contenido
            for node_id in node_ids:
                # Registrar directamente con node_id (ya no necesitamos database_id)
                self.db_cursor.execute('''
                    INSERT OR IGNORE INTO url_db_log (url_id, node_id)
                    VALUES (?, ?)
                ''', (url_id, node_id))
            
            self.db_conn.commit()

            # Actualizar contador de réplicas consultando cuántos nodos tienen realmente el contenido
            self.db_cursor.execute('''
                SELECT COUNT(DISTINCT node_id)
                FROM url_db_log
                WHERE url_id = ?
            ''', (url_id,))
            actual_replicas = self.db_cursor.fetchone()[0]
            
            self.db_cursor.execute('''
                UPDATE urls
                SET current_replicas = ?, target_replicas = 3
                WHERE url_id = ?
            ''', (actual_replicas, url_id))
            self.db_conn.commit()

            logging.info(f"Líder BD registró URL {url} en {len(node_ids)} subordinados: {node_ids} (total réplicas: {actual_replicas})")
        
        except Exception as e:
            logging.error(f"Error registrando URL en logs: {e}")
            raise

    def _url_query_leader(self, node_connection, message):
        '''Metodo que ejecutara el nodo cuando es lider, revisa en el log de las urls, si esta toma una base
           de datos aleatoria de todas las que tienen la info y les pide el content de la url, en caso de que 
           no este en la tabla de logs, entonces devuelve False al Router'''
        
        data = message.get('data', {})
        sender_id = message.get('sender_id')
        task_id = data.get('task_id')
        url = data.get('url')

        if not url or not task_id:
            logging.error(f"BD_QUERY inválido: falta url o task_id")
            return

        logging.info(f"BD Query recibida para task {task_id}, URL: {url}")

        try:
            with self.db_lock:
                # Buscar si la URL ya está registrada
                self.db_cursor.execute('SELECT url_id FROM urls WHERE url = ?', (url,))
                result = self.db_cursor.fetchone()

                if result is None:
                    # URL no encontrada en BD
                    logging.info(f"URL {url} NO encontrada en BD, responder negativo al Router")
                    self._send_bd_query_response(node_connection, task_id, found=False)
                    return

                url_id = result[0]
                logging.info(f"URL {url} encontrada con url_id={url_id}")

                # Verificar si hay contenido disponible en subordinados
                self.db_cursor.execute('''
                    SELECT node_id
                    FROM url_db_log
                    WHERE url_id = ?
                    ORDER BY node_id
                ''', (url_id,))

                databases_with_content = self.db_cursor.fetchall()

            if len(databases_with_content) == 0:
                # URL registrada pero sin contenido disponible
                logging.warning(f"URL {url} registrada pero sin contenido en subordinados")
                self._send_bd_query_response(node_connection, task_id, found=False)
                return

            # Hay contenido disponible, pedir a un subordinado
            subordinate_id = databases_with_content[0][0]
            logging.info(f"Solicitando contenido de URL al subordinado {subordinate_id}")
            
            # Buscar conexión con el subordinado
            subordinate_conn = None
            with self.status_lock:
                for node_id, conn in self.subordinates.items():
                    if subordinate_id in node_id:
                        subordinate_conn = conn
                        break

            if subordinate_conn:
                # Enviar query al subordinado
                url_query_message = self._create_message(
                    MessageProtocol.MESSAGE_TYPES['URL_QUERY'],
                    {
                        'router_node_id': node_connection.node_id,  # ID del Router para responder
                        'url': url,
                        'task_id': task_id
                    }
                )
                subordinate_conn.send_message(url_query_message)
                logging.info(f"URL_QUERY enviada al subordinado {subordinate_id} para URL {url}")
            else:
                # No hay conexión con el subordinado que tiene el contenido
                logging.warning(f"No hay conexión con subordinado {subordinate_id}, responder negativo")
                self._send_bd_query_response(node_connection, task_id, found=False)

        except Exception as e:
            logging.error(f"Error en _url_query_leader: {e}")
            import traceback
            traceback.print_exc()
            self._send_bd_query_response(node_connection, task_id, found=False)

    def _send_bd_query_response(self, router_connection, task_id, found, result=None):
        '''Envía respuesta al Router sobre una query de BD'''
        response = self._create_message(
            MessageProtocol.MESSAGE_TYPES['BD_QUERY_RESPONSE'],
            {
                'task_id': task_id,
                'found': found,
                'result': result if result else {}
            }
        )
        
        try:
            router_connection.send_message(response)
            logging.info(f"BD_QUERY_RESPONSE enviada al Router: task={task_id}, found={found}")
        except Exception as e:
            logging.error(f"Error enviando BD_QUERY_RESPONSE: {e}")

    def _url_query_noleader(self, node_connection, message):
        '''El subordinado BD recibe query del líder, busca contenido y responde directamente al Router'''
        data = message.get('data', {})
        url = data.get('url')
        router_node_id = data.get('router_node_id')
        task_id = data.get('task_id')

        logging.info(f"URL_QUERY recibida de líder BD: task={task_id}, url={url}")

        try:
            with self.db_lock:
                # Buscar contenido en BD local
                self.db_cursor.execute('SELECT url_id FROM urls WHERE url = ?', (url,))
                url_id_row = self.db_cursor.fetchone()

                if url_id_row is None:
                    logging.error(f"URL {url} no encontrada en tabla urls del subordinado")
                    # Responder al Router que no se encontró
                    self._send_bd_query_response_to_router(task_id, found=False)
                    return
                
                url_id = url_id_row[0]

                # Obtener contenido
                self.db_cursor.execute('SELECT content FROM urls WHERE url_id = ?', (url_id,))
                content_row = self.db_cursor.fetchone()

                if content_row is None:
                    logging.error(f"Contenido no encontrado para url_id={url_id} en subordinado")
                    self._send_bd_query_response_to_router(task_id, found=False)
                    return
                
                import json
                content_json = content_row[0]
                result = json.loads(content_json)
            
            logging.info(f"Contenido encontrado para {url}, enviando al Router")
            self._send_bd_query_response_to_router(task_id, found=True, result=result)
            
        except Exception as e:
            logging.error(f"Error consultando URL en BD subordinado: {e}")
            import traceback
            traceback.print_exc()
            self._send_bd_query_response_to_router(task_id, found=False)

    def _send_bd_query_response_to_router(self, task_id, found, result=None):
        '''Subordinado envía respuesta al Router usando socket temporal'''
        try:
            # Obtener info del Router desde el cache
            if 'router' not in self.external_bosses_cache:
                logging.error("Subordinado BD no tiene info del Router en cache")
                return
            
            router_info = self.external_bosses_cache['router']
            router_ip = router_info.get('ip')
            router_port = router_info.get('port')
            
            if not router_ip or not router_port:
                logging.error("Info del Router incompleta en cache")
                return

            # Crear mensaje
            response_data = {
                'task_id': task_id,
                'found': found,
                'result': result if found else None
            }
            
            message = MessageProtocol.create_message(
                msg_type=MessageProtocol.MESSAGE_TYPES['BD_QUERY_RESPONSE'],
                sender_id=self.ip,
                node_type=self.node_type,
                data=response_data
            )
            
            # Enviar usando socket temporal
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            
            try:
                sock.connect((router_ip, router_port))
                
                # Enviar longitud del mensaje (2 bytes como espera el Router)
                message_bytes = message.encode('utf-8')
                message_length = len(message_bytes)
                sock.sendall(message_length.to_bytes(2, byteorder='big'))
                
                # Enviar mensaje
                sock.sendall(message_bytes)
                
                logging.info(f"Subordinado BD envió BD_QUERY_RESPONSE al Router {router_ip}:{router_port}: task={task_id}, found={found}")
            finally:
                sock.close()
            
        except Exception as e:
            logging.error(f"Error enviando BD_QUERY_RESPONSE desde subordinado: {e}")
            import traceback
            traceback.print_exc()

    def _notify_router_of_error_result(self, task_id, result):
        """Notifica al Router de un resultado con error (sin guardar en BD)"""
        router_profile = self.external_bosses.get('router')
        
        if not router_profile or not router_profile.is_connected():
            logging.warning(f"No hay conexión con Router para notificar error de task {task_id}")
            return
        
        router_conn = router_profile.connection
        
        # Enviar BD_QUERY_RESPONSE con el resultado de error
        # Esto permitirá que el Router notifique al cliente del error
        response_msg = self._create_message(
            MessageProtocol.MESSAGE_TYPES['BD_QUERY_RESPONSE'],
            {
                'task_id': task_id,
                'found': True,  # "Encontrado" pero con error
                'result': result  # Incluye status: 'error' y error: '...'
            }
        )
        
        if router_conn.send_message(response_msg):
            logging.info(f"Resultado con error notificado al Router: task={task_id}")
        else:
            logging.error(f"No se pudo notificar error al Router: task={task_id}")

    def _recive_task_result(self, node_connection, message):
        '''Recibe lo escrapeado de la url directo de un nodo scrapper, y le manda la info
           a tres de las base de datos conectadas (incluyendo el líder), para garantizar replicabilidad de 3'''
        
        sender_id = message.get('sender_id')
        node_type = message.get('node_type')

        data = message.get('data', {})
        task_id = data.get('task_id')
        result = data.get('result', {})
        completed_at = message.get('timestamp')
        url = result.get('url')

        logging.info(f"SAVE_DATA recibido del Scrapper: task={task_id}, url={url}")

        # Verificar si el resultado es un error
        if result.get('status') == 'error':
            logging.warning(f"Resultado con error para task {task_id}: {result.get('error')}")
            # No guardar en BD, pero notificar al Router para que informe al cliente
            self._notify_router_of_error_result(task_id, result)
            return

        # El líder TAMBIÉN guarda el contenido y se registra a sí mismo
        try:
            import json
            
            with self.db_lock:
                # Insertar URL en tabla urls si no existe
                self.db_cursor.execute('INSERT OR IGNORE INTO urls (url) VALUES (?)', (url,))
                self.db_conn.commit()

                # Obtener url_id
                self.db_cursor.execute('SELECT url_id FROM urls WHERE url = ?', (url,))
                url_id_row = self.db_cursor.fetchone()
                if url_id_row:
                    url_id = url_id_row[0]
                    
                    # Serializar resultado como JSON
                    content_json = json.dumps(result)

                    # Actualizar contenido en tabla urls
                    self.db_cursor.execute('''
                        UPDATE urls 
                        SET content = ?, scrapped_at = ?
                        WHERE url_id = ?
                    ''', (content_json, completed_at, url_id))
                    self.db_conn.commit()

                    logging.info(f"Líder BD guardó contenido de {url} (url_id={url_id})")
                    
                    # Registrar al jefe en url_db_log (el jefe también almacena contenido)
                    # boss_node_id = f"{self.node_type}-{self.ip}:{self.port}"
                    self.db_cursor.execute('''
                        INSERT OR IGNORE INTO url_db_log (url_id, node_id)
                        VALUES (?, ?)
                    ''', (url_id, self.node_id))
                    self.db_conn.commit()
                    
                    logging.info(f"Líder BD se registró en url_db_log para {url}")
        except Exception as e:
            logging.error(f"Error guardando contenido en líder: {e}")

        subordinados = list(self.subordinates.items())
        
        if len(subordinados) == 0:
            logging.warning(f"No hay subordinados BD. Solo el líder tiene el contenido.")
            # Actualizar contador de réplicas a 1 (solo el jefe)
            try:
                self.db_cursor.execute('''
                    UPDATE urls
                    SET current_replicas = 1, target_replicas = 3
                    WHERE url_id = ?
                ''', (url_id,))
                self.db_conn.commit()
            except:
                pass
            return
        
        # Seleccionar hasta 2 subordinados más (el jefe ya cuenta como 1 réplica)
        random.shuffle(subordinados)
        selected_subordinates = subordinados[:2]

        message_to_subordinate = self._create_message(
            MessageProtocol.MESSAGE_TYPES['SAVE_DATA_NO_LEADER'],
            {'url': url, 'result': result, 'completed_at': completed_at, 'task_id': task_id}
        )

        # Lista para trackear qué subordinados recibieron el contenido
        successful_saves = []

        for node_id, conn in selected_subordinates:
            try:
                conn.send_message(message_to_subordinate)
                successful_saves.append(node_id)
                logging.info(f"SAVE_DATA_NO_LEADER enviado a subordinado {node_id}")
            except Exception as e:
                logging.error(f"Error enviando a subordinado {node_id}: {e}")
        
        # Registrar en logs del líder qué subordinados tienen el contenido
        # NOTA: El jefe ya se registró a sí mismo arriba
        if successful_saves:
            try:
                self._register_url_in_subordinates(url, successful_saves)
            except Exception as e:
                logging.error(f"Error registrando URL en logs del líder: {e}")

    def _recive_task_result_no_leader(self, node_connection, message):
        '''Recibe el resultado del líder y lo almacena en su base de datos'''

        data = message.get('data', {})
        url = data.get('url')
        result = data.get('result', {})
        completed_at = data.get('completed_at')
        task_id = data.get('task_id')

        logging.info(f"SAVE_DATA_NO_LEADER recibido: task={task_id}, url={url}")

        try:
            import json
            
            with self.db_lock:
                # Insertar URL en tabla urls si no existe
                self.db_cursor.execute('INSERT OR IGNORE INTO urls (url) VALUES (?)', (url,))
                self.db_conn.commit()

                # Obtener url_id
                self.db_cursor.execute('SELECT url_id FROM urls WHERE url = ?', (url,))
                url_id_row = self.db_cursor.fetchone()
                if not url_id_row:
                    logging.error(f"No se pudo obtener url_id para {url}")
                    return
                url_id = url_id_row[0]

                # Serializar resultado como JSON
                content_json = json.dumps(result)

                # Actualizar contenido en tabla urls
                self.db_cursor.execute('''
                    UPDATE urls 
                    SET content = ?, scrapped_at = ?
                    WHERE url_id = ?
                ''', (content_json, completed_at, url_id))
                self.db_conn.commit()

                logging.info(f"Subordinado BD guardó contenido de {url} (url_id={url_id})")
        except Exception as e:
            logging.error(f"Error en subordinado almacenando URL {url}: {e}")
        
    



    #====================Para el envio de mensajes=================
    def send_worker(self):
        '''Hilo que envia los mensajes de la cola'''
        while not self.stop_event.is_set():
            try:
                # esperar mensaje con timeout para poder verificar stop_event
                message, conn = self.message_queue.get(timeout=1.0)
                ip, port = conn.getsockname()
                peer_ip, peer_port = conn.getpeername()
                msg_type = message.get('type')

                if message is None:
                    logging.info('message is none')
                    break

                try:
                    # enviar longitud
                    lenght = len(message)
                    conn.send(lenght.to_bytes(2, 'big'))
                    
                    # enviar mensaje completo
                    conn.send(message)
                    logging.info(f"mensaje enviado a {peer_ip} de tipo {msg_type}")


                except Exception as e:
                    logging.error(f"Error enviando mensaje: a {ip}, {port} {e}")
                    
                    if not self.stop_event.is_set():
                        self.message_queue.put(message)
                    break

                self.message_queue.task_done()
            
            except queue.Empty:
                # timeout, verificar si se debe continuar
                continue

            except Exception as e:
                logging.error(f"Error en hilo de envio: {e}")

    def _enqueue_message(self, message_dict, ip, conn):
        '''encola un mensaje a enviar'''
        
        try:
            message_bytes = json.dumps(message_dict).encode()
            self.message_queue.put((message_bytes, conn))
            return True
        
        except Exception as e:
            logging.error(f"Error encolando mensaje: {message_bytes}: {e}")
            return False                
            
    #====================para el envio de mensajes=================


    #============= PARA DESCUBRIR A LOS OTROS JEFES ==============

    def _connect_to_external_bosses(self):
        """Conecta con el jefe Router cuando este nodo BD es jefe"""
        if not self.i_am_boss:
            logging.debug("No soy jefe, no necesito conectar con jefes externos")
            return
            
        logging.info("Conectando con jefe externo Router...")
        
        threading.Thread(
            target=self._periodic_boss_search,
            args=('router',),
            daemon=True
        ).start()

    def _periodic_boss_search(self, node_type):
        """
        Busca periódicamente al jefe Router hasta encontrarlo y mantener la conexión.
        
        Args:
            node_type: Tipo de nodo a buscar ('router')
        """
        retry_interval = 5  # segundos entre intentos
        boss_profile = self.external_bosses[node_type]

        logging.info(f"Iniciando búsqueda periódica del jefe {node_type}...")

        while self.running:
            # Si ya estamos conectados, solo monitorear
            if boss_profile.is_connected():
                # Esperar y verificar conexión
                time.sleep(retry_interval)
                continue
            
            # No conectado, buscar
            logging.debug(f"Buscando jefe {node_type}...")
            
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
                else:
                    logging.debug(f"Nodos {node_type} encontrados pero ninguno es jefe")
            else:
                logging.debug(f"No se encontraron nodos {node_type} en el DNS")
            
            # Esperar antes del siguiente intento
            time.sleep(retry_interval)

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
        
        logging.debug(f"Buscando jefe {node_type} en lista de {len(ip_list)} IPs: {ip_list}")
        
        for ip in ip_list:
            if ip == self.ip:
                logging.debug(f"Saltando mi propia IP: {ip}")
                continue
            
            logging.debug(f"Consultando si {ip} es jefe {node_type}...")
            
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
                logging.debug(f"Respuesta de {ip}: is_boss={is_boss}")
                if is_boss:
                    logging.info(f"✓ {ip} es el jefe {node_type}")
                    return ip
                else:
                    logging.debug(f"✗ {ip} no es jefe {node_type}")
            else:
                logging.debug(f"✗ No hubo respuesta de {ip}")
        
        logging.debug(f"No se encontró jefe {node_type} en la lista")
        return None

    def _connect_to_boss(self, node_type, boss_ip):
        """
        Establece conexión persistente con el jefe externo.
        
        Args:
            node_type: Tipo de nodo jefe
            boss_ip: IP del jefe
        """
        boss_profile = self.external_bosses[node_type]
        
        try:
            # Crear NodeConnection
            connection = NodeConnection(
                node_type=node_type,
                ip=boss_ip,
                port=boss_profile.port,
                on_message_callback=self._handle_message_from_node,
                sender_node_type=self.node_type,
                sender_id=self.node_id
            )
            
            # Conectar
            if connection.connect():
                logging.debug(f"Socket conectado exitosamente con {node_type} en {boss_ip}")
                
                # Enviar identificación
                identification_msg = self._create_message(
                    MessageProtocol.MESSAGE_TYPES['IDENTIFICATION'],
                    {
                        'is_temporary': False,
                        'is_boss': self.i_am_boss,
                        'port': self.port
                    }
                )
                
                logging.debug(f"Enviando identificación a {node_type}...")
                if connection.send_message(identification_msg):
                    logging.debug(f"Identificación enviada exitosamente a {node_type}")
                else:
                    logging.error(f"Error enviando identificación a {node_type}")
                
                # Actualizar BossProfile (set_connection ya tiene su propio lock)
                logging.debug(f"Actualizando BossProfile para {node_type}...")
                boss_profile.set_connection(connection)
                logging.debug(f"BossProfile actualizado. is_connected={boss_profile.is_connected()}")
                
                # Actualizar cache de jefes externos
                self.external_bosses_cache[node_type] = {
                    'ip': boss_ip,
                    'port': boss_profile.port
                }
                
                logging.info(f"✓ Conexión establecida con jefe {node_type} en {boss_ip}")
                
                # Replicar información a subordinados
                if self.i_am_boss:
                    self.replicate_external_bosses_info()
                    logging.info(f"Información de jefe {node_type} replicada a subordinados")
            else:
                logging.error(f"No se pudo conectar con jefe {node_type} en {boss_ip}")
                
        except Exception as e:
            logging.error(f"Error conectando con jefe {node_type}: {e}")
            boss_profile.clear_connection()

    #============= PARA DESCUBRIR A LOS OTROS JEFES ==============

    def _handle_list_tables(self, node_connection, message):
        """
        Handler para listar todas las tablas disponibles en la BD.
        Devuelve solo los nombres de las tablas.
        
        Args:
            node_connection: Conexión con el cliente (a través de router)
            message: Mensaje con la solicitud
        """
        data = message.get('data', {})
        request_id = data.get('request_id')
        
        try:
            cursor = self.db_conn.cursor()
            
            # Obtener solo los nombres de las tablas
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = [row[0] for row in cursor.fetchall()]
            
            # Crear respuesta
            response = {
                'type': MessageProtocol.MESSAGE_TYPES['LIST_TABLES_RESPONSE'],
                'sender_id': self.node_id,
                'timestamp': datetime.now().isoformat(),
                'data': {
                    'request_id': request_id,
                    'tables': tables,
                    'success': True
                }
            }
            
            node_connection.send_message(response)
            logging.info(f"Lista de {len(tables)} tablas enviada a {node_connection.node_id}")
            
        except Exception as e:
            logging.error(f"Error listando tablas: {e}")
            response = {
                'type': MessageProtocol.MESSAGE_TYPES['LIST_TABLES_RESPONSE'],
                'sender_id': self.node_id,
                'timestamp': datetime.now().isoformat(),
                'data': {
                    'request_id': request_id,
                    'tables': [],
                    'success': False,
                    'error': str(e)
                }
            }
            node_connection.send_message(response)

    def _handle_get_table_data(self, node_connection, message):
        """
        Handler para obtener datos paginados de una tabla específica.
        
        Args:
            node_connection: Conexión con el cliente (a través de router)
            message: Mensaje con la solicitud {request_id, table_name, page, page_size}
        """
        data = message.get('data', {})
        request_id = data.get('request_id')
        table_name = data.get('table_name')
        page = data.get('page', 1)
        page_size = data.get('page_size', 50)
        
        if not table_name:
            response = {
                'type': MessageProtocol.MESSAGE_TYPES['GET_TABLE_DATA_RESPONSE'],
                'sender_id': self.node_id,
                'timestamp': datetime.now().isoformat(),
                'data': {
                    'request_id': request_id,
                    'success': False,
                    'error': 'table_name requerido'
                }
            }
            node_connection.send_message(response)
            return
        
        try:
            cursor = self.db_conn.cursor()
            
            # Verificar que la tabla existe
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            if not cursor.fetchone():
                raise ValueError(f"Tabla {table_name} no existe")
            
            # Obtener total de registros
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_rows = cursor.fetchone()[0]
            
            # Calcular offset
            offset = (page - 1) * page_size
            
            # Obtener datos paginados
            cursor.execute(f"SELECT * FROM {table_name} LIMIT ? OFFSET ?", (page_size, offset))
            rows = cursor.fetchall()
            
            # Obtener nombres de columnas
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [col[1] for col in cursor.fetchall()]
            
            # Convertir filas a diccionarios
            data_rows = []
            for row in rows:
                row_dict = {}
                for idx, col in enumerate(columns):
                    row_dict[col] = row[idx]
                data_rows.append(row_dict)
            
            # Calcular información de paginación
            total_pages = (total_rows + page_size - 1) // page_size
            
            response = {
                'type': MessageProtocol.MESSAGE_TYPES['GET_TABLE_DATA_RESPONSE'],
                'sender_id': self.node_id,
                'timestamp': datetime.now().isoformat(),
                'data': {
                    'request_id': request_id,
                    'success': True,
                    'table_name': table_name,
                    'columns': columns,
                    'rows': data_rows,
                    'pagination': {
                        'page': page,
                        'page_size': page_size,
                        'total_rows': total_rows,
                        'total_pages': total_pages
                    }
                }
            }
            
            node_connection.send_message(response)
            logging.info(f"Datos de tabla {table_name} (página {page}/{total_pages}) enviados a {node_connection.node_id}")
            
        except Exception as e:
            logging.error(f"Error obteniendo datos de tabla {table_name}: {e}")
            response = {
                'type': MessageProtocol.MESSAGE_TYPES['GET_TABLE_DATA_RESPONSE'],
                'sender_id': self.node_id,
                'timestamp': datetime.now().isoformat(),
                'data': {
                    'request_id': request_id,
                    'success': False,
                    'error': str(e)
                }
            }
            node_connection.send_message(response)
    
    def _handle_update_url_content(self, node_connection, message):
        """
        Handler para recibir actualizaciones de contenido de URLs.
        Se usa durante la consolidación de URLs duplicadas.
        
        Args:
            node_connection: Conexión con el nodo que envía la actualización (jefe)
            message: Mensaje con la actualización
        """
        data = message.get('data', {})
        url = data.get('url')
        content = data.get('content')
        scrapped_at = data.get('scrapped_at')
        
        if not url or not content or not scrapped_at:
            logging.error("UPDATE_URL_CONTENT recibido con datos incompletos")
            return
        
        logging.info(f"UPDATE_URL_CONTENT recibido para URL: {url}")
        
        try:
            with self.db_lock:
                # Actualizar el contenido de la URL
                self.db_cursor.execute('''
                    UPDATE urls
                    SET content = ?, scrapped_at = ?
                    WHERE url = ?
                ''', (content, scrapped_at, url))
                
                affected = self.db_cursor.rowcount
                self.db_conn.commit()
                
                if affected > 0:
                    logging.info(f"✓ Contenido de {url} actualizado exitosamente (fecha: {scrapped_at})")
                else:
                    logging.warning(f"URL {url} no encontrada en la BD, insertándola...")
                    # Si no existe, insertarla
                    self.db_cursor.execute('''
                        INSERT INTO urls (url, content, scrapped_at)
                        VALUES (?, ?, ?)
                    ''', (url, content, scrapped_at))
                    self.db_conn.commit()
                    logging.info(f"✓ URL {url} insertada con contenido actualizado")
        
        except Exception as e:
            logging.error(f"Error actualizando contenido de {url}: {e}")
            import traceback
            traceback.print_exc()
    
    def _handle_delete_url_content(self, node_connection, message):
        """
        Handler para recibir solicitudes de eliminación de URLs.
        Se usa durante el balanceo de réplicas cuando hay exceso.
        
        Args:
            node_connection: Conexión con el nodo que envía la solicitud (jefe)
            message: Mensaje con la solicitud de eliminación
        """
        data = message.get('data', {})
        url = data.get('url')
        
        if not url:
            logging.error("DELETE_URL_CONTENT recibido sin URL")
            return
        
        logging.info(f"DELETE_URL_CONTENT recibido para URL: {url}")
        
        try:
            with self.db_lock:
                # Obtener url_id
                self.db_cursor.execute('SELECT url_id FROM urls WHERE url = ?', (url,))
                result = self.db_cursor.fetchone()
                
                if not result:
                    logging.warning(f"URL {url} no encontrada para eliminar")
                    return
                
                url_id = result[0]
                
                # Eliminar de url_db_log para este nodo
                self.db_cursor.execute('''
                    DELETE FROM url_db_log
                    WHERE url_id = ? AND node_id = ?
                ''', (url_id, self.node_id))
                
                # Eliminar contenido de urls
                self.db_cursor.execute('''
                    DELETE FROM urls WHERE url_id = ?
                ''', (url_id,))
                
                affected = self.db_cursor.rowcount
                self.db_conn.commit()
                
                if affected > 0:
                    logging.info(f"✓ URL {url} eliminada exitosamente (balanceo de réplicas)")
                else:
                    logging.warning(f"URL {url} ya estaba eliminada")
        
        except Exception as e:
            logging.error(f"Error eliminando {url}: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    try:
        # Crear y arrancar nodo scrapper
        router = DatabaseNode()
        router.start()  # Hereda el método start() de Node
        
    except KeyboardInterrupt:
        logging.info("Deteniendo nodo Database...")
        try:
            if 'router' in locals():
                router.stop()
        except Exception as e:
            logging.error(f"Error al detener nodo Database: {e}")
    except Exception as e:
        logging.error(f"Error fatal: {e}")
        import traceback
        traceback.print_exc()