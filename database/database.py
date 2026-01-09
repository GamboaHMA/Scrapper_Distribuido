'''
Nodo Database:
- Encargado de almacenar la informacion
'''

import sys
import os
import logging
import threading
import time
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

        # Perfiles de jefes externos
        self.external_bosses = {
            'router': BossProfile('router', router_port),
            'scrapper': BossProfile('scrapper', scrapper_port)
        }

        #Registrar handlers expecificos del nodo database
        self._register_bd_handlers()
    
        self.init_database()

    def _register_bd_handlers(self):
        '''Registrar handlers para mensajes especificos de bd'''
        
        # Handler para query de url que le manda el scrapper (conexion persistente)
        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['BD_QUERY'],
            self._url_query_leader
        )

        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['URL_QUERY'],
            self._url_query_noleader
        )

        self.add_persistent_message_handler(
            MessageProtocol.MESSAGE_TYPES['SAVE_DATA'],
            self._recive_task_result
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

    def _url_query_leader(self, message):
        '''Metodo que ejecutara el nodo cuando es lider, revisa en el log de las urls, si esta toma una base
           de datos aleatoria de todas las que tienen la info y les pide el content de la url, en caso de que 
           no este en la tabla de logs, entonces devuelve False'''
        
        data = message.get('data')
        sender_id = message.get('sender_id')
        task_id = data.get('task_id')
        node_type = message.get('node_type')

        url = data.get('url')

        self.db_cursor.execute('''
            SELECT url_id FROM urls 
            WHERE url = ?
        '''
        ,(url))

        url_id = self.db_cursor.fetchone()[0]

        self.db_cursor.execute('''
            SELECT d.database_id
            FROM url_db_log udl
            JOIN databases d ON udl.database_id = d.database
            WHERE udl.url_id = ?
            ORDER BY d.database_id
        '''
        ,(url_id))

        databases_ips = self.db_cursor.fetchall()

        if len(databases_ips) != 0:
            for (database_ip, conn) in self.subordinates.items():
                with self.status_lock:
                    url_query_message = MessageProtocol.create_message(
                        msg_type=MessageProtocol.MESSAGE_TYPES['URL_QUERY'],
                        sender_id=self.ip,
                        node_type=self.node_type,
                        timestamp=datetime.now(),
                        data={'id_to_send':sender_id, 'url': url, 'task_id': task_id}
                    )
                    self._enqueue_message(url_query_message, database_ip, conn)
                    break
                    
        else:
            # mandar mensaje de que no hay registro al que consulto
            url_query_response = MessageProtocol.create_message(
                msg_type=MessageProtocol.MESSAGE_TYPES['BD_QUERY_RESPONSE'],
                sender_id=self.ip,
                node_type=self.node_type,
                timestamp=datetime.now(),
                data={'found':False, 'task_id':'', 'result':False}
            )
            try:
                router_conn:NodeConnection = self.bosses_connections['router']
                if not router_conn or not router_conn.is_connected():
                    logging.warning(f"No hay conexion con router para mensaje tipo BD_QUERY_RESPONSE a {router_conn.ip}")
                    return
                else:
                    self._enqueue_message(url_query_response, sender_id, conn)
            except Exception as e:
                logging.error(f"fallo al acceder al socket de {sender_id} con {self.ip}: {e}")

    def _url_query_noleader(self, message):
        '''Metodo que ejecutara el nodo cuando no es lider, revisa en la base de datos la url, y devuelve el contenido'''
        data = message.get('data')
        sender_id = message.get('sender_id')
        url = data.get('url')
        id_to_send = data.get('id_to_send')
        task_id = data.get('task_id')

        self.db_cursor.execute('''
            SELECT url_id
            FROM urls 
            WHERE url = ?
        '''
        ,(url))

        url_id = self.db_cursor.fetchall()

        if len(url_id) == 0:
            logging.error(f"url: {url} no se encuentra en la tabla urls de nodo: {self.ip}")
            return
        else:
            url_id = url_id[0] 

        self.db_cursor.execute('''
            SELECT content
            FROM url_content
            WHERE url_id = ?
        '''
        ,(url_id))

        content = self.db_cursor.fetchall()

        if len(content) == 0:
            logging.error(f"url_id: {url_id} no se encuentra en la tabla url_content del nodo: {self.ip}")
            return
        else:
            result = content[0]

        #crear mensaje de respuesta
        content_message = MessageProtocol.create_message(
            msg_type=MessageProtocol.MESSAGE_TYPES['BD_QUERY_RESPONSE'],
            sender_id=self.ip,
            node_type=self.node_type,
            data={'found':True, 'result':result, 'task_id':task_id}
        )

        #hacer un socket temporal para enviar el mensaje, esta vez mandamos el mensaje directo sin usar la cola
        try:
            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn.settimeout(30)
            conn.connect((id_to_send, PORTS['router']))

            try:
                #enviar longitud
                lenght = len(content_message)
                conn.send(lenght.to_bytes(2, 'big'))

                #enviar mensaje completo
                conn.send(content_message)
                logging.info(f"mensaje enviado a {id_to_send} de tipo BD_QUERY_RESPONSE")
            
            except Exception as e:
                logging.error(f"error enviando mensaje a {id_to_send} de tipo BD_QUERY_RESPONSE: {e}")


            logging.info(f"mensaje de bd_query_response a {id_to_send} encolado, cerrando socket temporal...")

            conn.close()
        
        except Exception as e:
            logging.error(f"error al enviar mensaje de tipo url_query_response a {id_to_send}: {e}")

    def _recive_task_result(self, message):
        '''Recibe lo escrapeado de la url directo de un nodo scrapper, y le manda la info
           a tres de las base de datos conectadas, para garantizar la replicabilidad de 3,
           el que recibe el mensaje es el lider'''
        
        #poner un if para que en caso de que les llegue a un no lider lo ignore

        sender_id = message.get('sender_id')
        node_type = message.get('node_type')

        data = message.get('data')
        task_id = data.get('task_id')
        result = data.get('result')
        completed_at = data.get('timestamp')
        url = result.get('url')

        message_to_no_leaders = MessageProtocol.create_message(
            msg_type=MessageProtocol.MESSAGE_TYPES['SAVE_DATA_NO_LEADER'],
            sender_id=sender_id,
            node_type=node_type,
            timestamp=datetime.now().isoformat(),
            data={'url': url, 'result': result, 'completed_at': completed_at, 'task_id': task_id}
        )

        subordinados:list[tuple] = self.subordinates.items()
        if len(subordinados) == 0:
            logging.warning(f"No hay bases de datos no lideres para almacenar la info de {sender_id}")
            return
        subordinados = random.shuffle(subordinados)
        subordinados = subordinados[:3] # no importa que no haya 3

        for database_id, conn in subordinados:
            self._enqueue_message(message_to_no_leaders, database_id, conn)
            logging.info(f"mensaje de task_result_no_leader a {database_id} encolado")

    def _recive_task_result_no_leader(self, message):
        '''Recibe el resultado del lider y lo almacena en su base de datos'''

        data = message.get('data')
        url = data.get('url')
        content = data.get('result')
        completed_at = data.get('completed_at')

        try:
            # insertar url en tabla urls
            self.db_cursor.execute('''
                INSERT OR IGNORE INTO urls (url) VALUES (?);
            ''', (url,))
            self.db_conn.commit()

            # obtener url_id
            self.db_cursor.execute('''
                SELECT url_id FROM urls WHERE url = ?;
            ''', (url,))
            url_id = self.db_cursor.fetchone()[0]

            # insertar contenido en tabla url_content
            self.db_cursor.execute('''
                INSERT OR REPLACE INTO url_content (url_id, content, scrapped_at)
                VALUES (?, ?, ?);
            ''', (url_id, content, completed_at))
            self.db_conn.commit()

            logging.info(f"Contenido de URL {url} almacenado correctamente en la base de datos.")
        
        except Exception as e:
            logging.error(f"Error almacenando contenido de URL {url}: {e}")
        
    



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