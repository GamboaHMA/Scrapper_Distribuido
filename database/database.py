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
            
            # Crear conexión y cursor separados
            db_path = self.db_conn.execute('PRAGMA database_list').fetchone()[2]
            conn = sqlite3.connect(db_path, check_same_thread=False)
            cursor = conn.cursor()
            
            try:
                # Buscar database_id correspondiente al node_id
                cursor.execute('''
                    SELECT database_id 
                    FROM databases 
                    WHERE node_id = ? AND is_active = 1
                ''', (node_id,))
                
                result = cursor.fetchone()
                
                if result:
                    database_id = result[0]
                    logging.info(f"[REREPLICATE] Database_id encontrado: {database_id} para node_id: {node_id}")
                    # Activar re-replicación (pasar node_id para excluir de lista)
                    self._handle_subordinate_disconnection(node_id, database_id, exclude_node_id=node_id)
                else:
                    logging.warning(f"[REREPLICATE] No se encontró database_id activo para {node_id} en la tabla databases")
                    # Intentar buscar sin filtro is_active para debug
                    cursor.execute('SELECT database_id, is_active FROM databases WHERE node_id = ?', (node_id,))
                    debug_result = cursor.fetchall()
                    logging.info(f"[REREPLICATE] Registros encontrados para {node_id}: {debug_result}")
            finally:
                cursor.close()
                conn.close()
                
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
        
        # PRIMERO: Reconstruir tablas desde subordinados (recuperación de metadata)
        self._recover_metadata_from_subordinates()
        
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
                
                # Consultar subordinados que deberían estar activos en la BD
                self.db_cursor.execute('''
                    SELECT node_id, database_id 
                    FROM databases 
                    WHERE is_active = 1
                ''')
                db_subordinates = self.db_cursor.fetchall()
                
                # Detectar subordinados desconectados
                for node_id, db_id in db_subordinates:
                    if node_id not in connected_node_ids:
                        logging.warning(f"Subordinado {node_id} está desconectado, iniciando re-replicación")
                        self._handle_subordinate_disconnection(node_id, db_id)
                        
            except Exception as e:
                logging.error(f"Error en monitor de subordinados: {e}")
                import traceback
                traceback.print_exc()

    def _handle_subordinate_disconnection(self, disconnected_node_id, database_id, exclude_node_id=None):
        """
        Maneja la desconexión de un subordinado: marca como inactivo y re-replica datos.
        
        Args:
            disconnected_node_id: ID del nodo desconectado
            database_id: ID en la tabla databases
            exclude_node_id: Node ID a excluir de subordinados disponibles (el que se está desconectando)
        """
        # Usar cursor separado para evitar conflictos con monitor
        conn = sqlite3.connect(self.db_conn.execute('PRAGMA database_list').fetchone()[2], check_same_thread=False)
        cursor = conn.cursor()
        
        try:
            # Marcar subordinado como inactivo
            cursor.execute('''
                UPDATE databases 
                SET is_active = 0 
                WHERE database_id = ?
            ''', (database_id,))
            conn.commit()
            
            logging.info(f"Subordinado {disconnected_node_id} marcado como inactivo")
            
            # Obtener todas las URLs que tenía este subordinado
            cursor.execute('''
                SELECT DISTINCT url_id 
                FROM url_db_log 
                WHERE database_id = ?
            ''', (database_id,))
            affected_urls = [row[0] for row in cursor.fetchall()]
            
            if not affected_urls:
                logging.info(f"No hay URLs afectadas por desconexión de {disconnected_node_id}")
                return
            
            logging.info(f"Verificando {len(affected_urls)} URLs afectadas por desconexión")
            
            # Para cada URL, verificar si necesita re-replicación
            for url_id in affected_urls:
                self._check_and_rereplicate_url(url_id, cursor, conn, exclude_node_id=exclude_node_id)
                
        except Exception as e:
            logging.error(f"Error manejando desconexión de {disconnected_node_id}: {e}")
            import traceback
            traceback.print_exc()
        finally:
            cursor.close()
            conn.close()

    def _check_and_rereplicate_url(self, url_id, cursor=None, conn=None, exclude_node_id=None):
        """
        Verifica el número de réplicas activas de una URL y re-replica si es necesario.
        
        Args:
            url_id: ID de la URL a verificar
            cursor: Cursor de SQLite (opcional, usa self.db_cursor si no se provee)
            conn: Conexión de SQLite (opcional, usa self.db_conn si no se provee)
            exclude_node_id: Node ID a excluir de la lista de disponibles (nodo desconectándose)
        """
        # Usar cursor provisto o el cursor principal
        db_cursor = cursor if cursor else self.db_cursor
        db_conn = conn if conn else self.db_conn
        
        try:
            # Contar réplicas activas actuales
            db_cursor.execute('''
                SELECT COUNT(*)
                FROM url_db_log udl
                JOIN databases d ON udl.database_id = d.database_id
                WHERE udl.url_id = ? AND d.is_active = 1
            ''', (url_id,))
            current_replicas = db_cursor.fetchone()[0]
            
            target_replicas = 3
            needed_replicas = target_replicas - current_replicas
            
            if needed_replicas <= 0:
                # Ya hay suficientes réplicas
                return
            
            logging.info(f"URL {url_id} tiene {current_replicas} réplicas, necesita {needed_replicas} más")
            
            # Obtener la URL y su contenido de un subordinado activo
            db_cursor.execute('''
                SELECT u.url, d.node_id
                FROM urls u
                JOIN url_db_log udl ON u.url_id = udl.url_id
                JOIN databases d ON udl.database_id = d.database_id
                WHERE u.url_id = ? AND d.is_active = 1
                LIMIT 1
            ''', (url_id,))
            url_info = db_cursor.fetchone()
            
            if not url_info:
                logging.error(f"No hay réplicas activas para URL {url_id}, no se puede re-replicar")
                return
            
            url, source_node_id = url_info
            
            # Obtener subordinados activos actualmente conectados (excluir el que se está desconectando)
            with self.status_lock:
                connected_node_ids = [nid for nid in self.subordinates.keys() if nid != exclude_node_id]
            
            logging.info(f"[REREPLICATE] Subordinados conectados (excluyendo {exclude_node_id}): {connected_node_ids}")
            
            # Obtener subordinados que NO tienen esta URL Y están conectados
            db_cursor.execute('''
                SELECT d.node_id, d.database_id
                FROM databases d
                WHERE d.is_active = 1
                AND d.database_id NOT IN (
                    SELECT database_id 
                    FROM url_db_log 
                    WHERE url_id = ?
                )
            ''', (url_id,))
            available_subordinates = [row for row in db_cursor.fetchall() if row[0] in connected_node_ids]
            
            if not available_subordinates:
                logging.warning(f"[REREPLICATE] No hay subordinados disponibles para re-replicar URL {url_id}. Conectados: {len(connected_node_ids)}, Disponibles sin URL: 0")
                return
            
            # Seleccionar subordinados para re-replicar (hasta completar 3 réplicas)
            random.shuffle(available_subordinates)
            targets = available_subordinates[:needed_replicas]
            
            logging.info(f"[REREPLICATE] Targets seleccionados para URL {url_id}: {[t[0] for t in targets]}")
            
            # Solicitar contenido al subordinado fuente y enviarlo a los targets
            self._request_and_replicate_content(url, url_id, source_node_id, targets, db_cursor, db_conn)
            
        except Exception as e:
            logging.error(f"Error verificando/re-replicando URL {url_id}: {e}")
            import traceback
            traceback.print_exc()

    def _request_and_replicate_content(self, url, url_id, source_node_id, target_subordinates, cursor=None, conn=None):
        """
        Solicita que un subordinado fuente replique contenido a otros subordinados.
        El líder NO maneja el contenido, solo coordina la replicación.
        
        Args:
            url: URL a replicar
            url_id: ID de la URL
            source_node_id: ID del subordinado que tiene el contenido
            target_subordinates: Lista de tuplas (node_id, database_id) para replicar
            cursor: Cursor de SQLite (opcional)
            conn: Conexión de SQLite (opcional)
        """
        # Usar cursor provisto o el cursor principal
        db_cursor = cursor if cursor else self.db_cursor
        db_conn = conn if conn else self.db_conn
        
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
            for node_id, database_id in target_subordinates:
                # Extraer IP del node_id (formato: "bd-IP:PORT")
                if '-' in node_id and ':' in node_id:
                    ip = node_id.split('-')[1].split(':')[0]
                    targets_info.append({
                        'node_id': node_id,
                        'database_id': database_id,
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
                database_id = target_info['database_id']
                db_cursor.execute('''
                    INSERT OR IGNORE INTO url_db_log (url_id, database_id)
                    VALUES (?, ?)
                ''', (url_id, database_id))
            
            db_conn.commit()
            
            # Actualizar contador de réplicas
            db_cursor.execute('''
                SELECT COUNT(*)
                FROM url_db_log udl
                JOIN databases d ON udl.database_id = d.database_id
                WHERE udl.url_id = ? AND d.is_active = 1
            ''', (url_id,))
            new_count = db_cursor.fetchone()[0]
            
            db_cursor.execute('''
                UPDATE urls_replicas 
                SET current_replicas = ?
                WHERE url_id = ?
            ''', (new_count, url_id))
            db_conn.commit()
            
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
            self.db_cursor.execute('SELECT content, scrapped_at FROM url_content WHERE url_id = ?', (local_url_id,))
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
        para reconstruir las tablas url_db_log, databases, urls, y urls_replicas.
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
            # Obtener todas las URLs de la tabla url_content
            self.db_cursor.execute('''
                SELECT u.url_id, u.url
                FROM urls u
                JOIN url_content uc ON u.url_id = uc.url_id
            ''')
            urls = [{'url_id': row[0], 'url': row[1]} for row in self.db_cursor.fetchall()]
            
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
            # 1. Reconstruir tabla databases con subordinados activos
            logging.info("Reconstruyendo tabla databases...")
            for node_id, urls in self.inventory_responses.items():
                if urls is None:
                    logging.warning(f"Subordinado {node_id} no respondió a tiempo")
                    continue
                
                # Insertar o actualizar subordinado en tabla databases
                self.db_cursor.execute('''
                    INSERT OR REPLACE INTO databases (node_id, is_active)
                    VALUES (?, 1)
                ''', (node_id,))
            
            self.db_conn.commit()
            logging.info(f"Tabla databases reconstruida con {len(self.inventory_responses)} subordinados")
            
            # 2. Reconstruir tablas urls y url_db_log
            logging.info("Reconstruyendo tablas urls y url_db_log...")
            
            url_replica_count = {}  # Para contar réplicas por URL
            
            for node_id, urls in self.inventory_responses.items():
                if urls is None:
                    continue
                
                # Obtener database_id
                self.db_cursor.execute('SELECT database_id FROM databases WHERE node_id = ?', (node_id,))
                db_row = self.db_cursor.fetchone()
                if not db_row:
                    logging.error(f"No se encontró database_id para {node_id}")
                    continue
                
                database_id = db_row[0]
                
                # Para cada URL reportada por este subordinado
                for url_info in urls:
                    url = url_info['url']
                    
                    # Insertar URL en tabla urls si no existe
                    self.db_cursor.execute('INSERT OR IGNORE INTO urls (url) VALUES (?)', (url,))
                    self.db_conn.commit()
                    
                    # Obtener url_id
                    self.db_cursor.execute('SELECT url_id FROM urls WHERE url = ?', (url,))
                    url_id = self.db_cursor.fetchone()[0]
                    
                    # Registrar en url_db_log
                    self.db_cursor.execute('''
                        INSERT OR IGNORE INTO url_db_log (url_id, database_id)
                        VALUES (?, ?)
                    ''', (url_id, database_id))
                    
                    # Contar réplicas
                    url_replica_count[url_id] = url_replica_count.get(url_id, 0) + 1
            
            self.db_conn.commit()
            logging.info(f"Tablas urls y url_db_log reconstruidas con {len(url_replica_count)} URLs únicas")
            
            # 3. Reconstruir tabla urls_replicas
            logging.info("Reconstruyendo tabla urls_replicas...")
            for url_id, replica_count in url_replica_count.items():
                self.db_cursor.execute('''
                    INSERT OR REPLACE INTO urls_replicas (url_id, current_replicas, target_replicas)
                    VALUES (?, ?, 3)
                ''', (url_id, replica_count))
            
            self.db_conn.commit()
            logging.info(f"Tabla urls_replicas reconstruida")
            
            # Resumen
            logging.info("=" * 60)
            logging.info(f"RECUPERACIÓN COMPLETADA:")
            logging.info(f"  - {len(self.inventory_responses)} subordinados registrados")
            logging.info(f"  - {len(url_replica_count)} URLs únicas encontradas")
            logging.info(f"  - Total de réplicas: {sum(url_replica_count.values())}")
            logging.info("=" * 60)
            
        except Exception as e:
            logging.error(f"Error reconstruyendo tablas: {e}")
            import traceback
            traceback.print_exc()
            logging.error(f"Error manejando REPLICATE_CONTENT: {e}")
            import traceback
            traceback.print_exc()

    def _redistribute_own_urls(self):
        """
        Redistribuye las URLs que el nuevo jefe tenía almacenadas cuando era subordinado.
        Asegura que haya 3 réplicas de cada URL sin duplicar en subordinados.
        """
        logging.info("=" * 60)
        logging.info("REDISTRIBUCIÓN: URLs del nuevo jefe a subordinados")
        logging.info("=" * 60)
        
        try:
            # Obtener URLs que este nodo tiene en su tabla url_content
            self.db_cursor.execute('''
                SELECT u.url_id, u.url, uc.content, uc.scrapped_at
                FROM urls u
                JOIN url_content uc ON u.url_id = uc.url_id
            ''')
            own_urls = self.db_cursor.fetchall()
            
            if not own_urls:
                logging.info("Este nodo no tenía URLs almacenadas, no se necesita redistribución")
                return
            
            logging.info(f"Redistribuyendo {len(own_urls)} URLs que tenía este nodo...")
            
            for url_id, url, content_json, scrapped_at in own_urls:
                # Contar cuántas réplicas hay en subordinados (sin incluir este nodo)
                self.db_cursor.execute('''
                    SELECT COUNT(*)
                    FROM url_db_log udl
                    JOIN databases d ON udl.database_id = d.database_id
                    WHERE udl.url_id = ? AND d.is_active = 1
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
                    SELECT d.node_id, d.database_id
                    FROM databases d
                    WHERE d.is_active = 1
                    AND d.database_id NOT IN (
                        SELECT database_id 
                        FROM url_db_log 
                        WHERE url_id = ?
                    )
                ''', (url_id,))
                available_subordinates = [row for row in self.db_cursor.fetchall() if row[0] in connected_node_ids]
                
                if not available_subordinates:
                    logging.warning(f"No hay subordinados disponibles para redistribuir URL {url}")
                    continue
                
                # Seleccionar targets
                random.shuffle(available_subordinates)
                targets = available_subordinates[:needed_replicas]
                
                logging.info(f"Redistribuyendo URL {url} a {len(targets)} subordinados: {[t[0] for t in targets]}")
                
                # Deserializar contenido
                result = json.loads(content_json)
                
                # Enviar a cada subordinado
                for node_id, database_id in targets:
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
                                INSERT OR IGNORE INTO url_db_log (url_id, database_id)
                                VALUES (?, ?)
                            ''', (url_id, database_id))
                            
                            logging.info(f"URL {url} redistribuida a {node_id}")
                        except Exception as e:
                            logging.error(f"Error redistribuyendo a {node_id}: {e}")
                    else:
                        logging.warning(f"No hay conexión con subordinado {node_id}")
                
                self.db_conn.commit()
                
                # Actualizar contador de réplicas
                self.db_cursor.execute('''
                    SELECT COUNT(*)
                    FROM url_db_log udl
                    JOIN databases d ON udl.database_id = d.database_id
                    WHERE udl.url_id = ? AND d.is_active = 1
                ''', (url_id,))
                new_count = self.db_cursor.fetchone()[0]
                
                self.db_cursor.execute('''
                    INSERT OR REPLACE INTO urls_replicas (url_id, current_replicas, target_replicas)
                    VALUES (?, ?, 3)
                ''', (url_id, new_count))
                self.db_conn.commit()
            
            # Limpiar tabla url_content del jefe (ya no debe almacenar contenido)
            logging.info("Limpiando tabla url_content del jefe (el jefe solo guarda metadata)...")
            self.db_cursor.execute('DELETE FROM url_content')
            self.db_conn.commit()
            
            logging.info("=" * 60)
            logging.info(f"REDISTRIBUCIÓN COMPLETADA: {len(own_urls)} URLs procesadas")
            logging.info("=" * 60)
            
        except Exception as e:
            logging.error(f"Error redistribuyendo URLs propias: {e}")
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

    def init_database(self):
        '''Inicializa la base de datos'''
        try:
            # crear directorio si no existe
            Path(self.data_dir).mkdir(parents=None, exist_ok=True)

            # base de datos para las url
            db_path = f"{self.data_dir}/{self.name}.db"
            self.db_conn = sqlite3.connect(db_path, check_same_thread=False)
            self.db_cursor = self.db_conn.cursor()

            # tabla de urls y su id
            self.db_cursor.execute('''
                CREATE TABLE IF NOT EXISTS urls (
                    url_id INTEGER PRIMARY KEY,
                    url TEXT UNIQUE NOT NULL,
                    firstseen DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            '''
            )

            # tabla de databases
            self.db_cursor.execute('''
                CREATE TABLE IF NOT EXISTS databases (
                    database_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id TEXT UNIQUE NOT NULL,
                    is_active BOOLEAN DEFAULT 1,
                    last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(node_id)    
                )

            '''
            )

            # tabla de urls para almacenar: cantidad de replicas
            self.db_cursor.execute('''
                CREATE TABLE IF NOT EXISTS urls_replicas (
                    url_id INTEGER PRIMARY KEY,
                    current_replicas INTEGER DEFAULT 0,
                    target_replicas INTEGER DEFAULT 3,
                    FOREIGN KEY (url_id) REFERENCES urls(url_id) ON DELETE CASCADE            
                )
            '''
            )

            # tabla para guardar los contenidos de las urls (nodos no jefe)
            self.db_cursor.execute('''
                CREATE TABLE IF NOT EXISTS url_content (
                    content_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url_id INTEGER NOT NULL,
                    content TEXT,
                    scrapped_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (url_id) REFERENCES urls(url_id) ON DELETE CASCADE,
                    UNIQUE(url_id)
                )
            '''
            )

            # tabla para guardar la tupla url-database, que nos dira en cuales bases de datos se guardo una url
            self.db_cursor.execute('''
                CREATE TABLE IF NOT EXISTS url_db_log (
                    location_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url_id INTEGER NOT NULL,
                    database_id INTEGER NOT NULL,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(url_id) REFERENCES urls(url_id) ON DELETE CASCADE,
                    FOREIGN KEY(database_id) REFERENCES databases(database_id) ON DELETE CASCADE,
                    UNIQUE(url_id, database_id)
                )
          '''
          )

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
                # Buscar o crear database_id para este node_id
                self.db_cursor.execute('SELECT database_id FROM databases WHERE node_id = ?', (node_id,))
                db_row = self.db_cursor.fetchone()
                
                if not db_row:
                    # Crear entrada para este subordinado si no existe
                    self.db_cursor.execute('INSERT INTO databases (node_id) VALUES (?)', (node_id,))
                    self.db_conn.commit()
                    database_id = self.db_cursor.lastrowid
                else:
                    database_id = db_row[0]

                # Registrar que este subordinado tiene la URL
                self.db_cursor.execute('''
                    INSERT OR IGNORE INTO url_db_log (url_id, database_id)
                    VALUES (?, ?)
                ''', (url_id, database_id))
            
            self.db_conn.commit()

            # Actualizar contador de réplicas
            self.db_cursor.execute('''
                INSERT INTO urls_replicas (url_id, current_replicas, target_replicas)
                VALUES (?, ?, 3)
                ON CONFLICT(url_id) DO UPDATE SET current_replicas = ?
            ''', (url_id, len(node_ids), len(node_ids)))
            self.db_conn.commit()

            logging.info(f"Líder BD registró URL {url} en {len(node_ids)} subordinados: {node_ids}")
        
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
                SELECT d.node_id
                FROM url_db_log udl
                JOIN databases d ON udl.database_id = d.database_id
                WHERE udl.url_id = ? AND d.is_active = 1
                ORDER BY d.database_id
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
            self.db_cursor.execute('SELECT content FROM url_content WHERE url_id = ?', (url_id,))
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

        # El líder NO guarda contenido, solo delega a subordinados
        subordinados = list(self.subordinates.items())
        
        if len(subordinados) == 0:
            logging.warning(f"No hay subordinados BD para guardar. Contenido se perderá.")
            return
        
        # Seleccionar hasta 3 subordinados aleatorios para replicación
        random.shuffle(subordinados)
        selected_subordinates = subordinados[:3]

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

            # Insertar contenido en tabla url_content
            self.db_cursor.execute('''
                INSERT OR REPLACE INTO url_content (url_id, content, scrapped_at)
                VALUES (?, ?, ?)
            ''', (url_id, content_json, completed_at))
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


    #=====Para comprobar los subordinados que se incorporan========

    def _add_new_sub_to_database(self):
        '''Agregar nuevos subordinados bd a la base de datos de databases'''
        
        while(True):
            self.db_cursor.execute("SELECT * FROM databases")
            databases = self.db_cursor.fetchall()

            databases_id = [database[0] for database in databases]

            for database_id, conn in self.subordinates.items():
                if database_id in databases_id:
                    continue
                else:
                    self.db_cursor.execute('''
                        INSERT OR IGNORE INTO databases (node_id)
                        VALUES (?)
                    ''', (databases_id)
                    )
                    self.db_conn.commit()
                    logging.info(f"database {database_id} registrada en databases")
            
            time.sleep(0.1)
                    
    #=====para comprobar los subordinados que se incorporan========


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