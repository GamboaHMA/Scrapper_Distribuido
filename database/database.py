import socket
import time
import json
import threading
import random
import logging
import os
from datetime import datetime
import struct
import queue
import sqlite3
from pathlib import Path


#========================Utils======================
class MessageProtocol:
    # tipos de mensaje
    MESSAGE_TYPES = {
        'DISCOVERY': 'discovery',
        'LEADER_QUERY': 'leader_query',
        'LEADER_RESPONSE': 'leader_response',
        'HEARTBEAT_RESPONSE': 'heartbeat_response',
        'HEARTBEAT': 'heartbeat',
        'URL_QUERY': 'url_query',  # en el data viene 'url': url
        'URL_QUERY_LEADER': 'url_query_leader',
        'URL_QUERY_RESPONSE': 'url_query_response',
        'TASK_RESULT': 'task_result',
        'ELECTION': 'election',
        'ANSWER': 'answer',
        'COORDINATOR': 'coordinator',
        'NODE_LIST': 'node_list',
        'JOIN_NETWORK': 'join_network',
        'LEAVE_NETWORK': 'leave_network',
        'LEADER_ANNOUNCE': 'leader_announce'
    }

    @staticmethod
    def create_message(msg_type, sender_id, node_type, data=None, timestamp=None):
        """Crea un mensaje JSON estandarizado"""
        message = {
            'type': msg_type,
            'sender_id': sender_id,
            'node_type': node_type,
            'timestamp': timestamp or time.time(),
            'data': data or {}
        }
        return json.dumps(message)
    
    @staticmethod
    def parse_message(json_str):
        """Parsea un mensaje JSON"""
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            return None

def comparar_lista_ips(ips:list):
    '''Método que devuelve la mayor de las IPs en una lista'''
    if not ips:
        return None
    
    # Inicializamos con la primera IP como la mayor
    mayor_ip = ips[0]
    partes_mayor = mayor_ip.split('.')
    
    # Comparamos con cada una de las otras IPs
    for ip in ips[1:]:
        partes_actual = ip.split('.')
        
        for p_mayor, p_actual in zip(partes_mayor, partes_actual):
            num_mayor = int(p_mayor)
            num_actual = int(p_actual)
            
            if num_actual > num_mayor:
                # La IP actual es mayor
                mayor_ip = ip
                partes_mayor = partes_actual
                break
            elif num_actual < num_mayor:
                # La IP actual es menor
                break
            # Si son iguales, continuamos con la siguiente parte
    
    return mayor_ip

NODE_TYPE = 'db'
PORT = 8080

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class DatabaseNode:
    def __init__(self, port=PORT):
        self.node_type = NODE_TYPE
        self.name = socket.gethostname()
        self.ip = socket.gethostbyname(self.name)
        self.port = port
        self.sock_escucha = None
        self.escuchando = False
        self.conexiones_activas = {}  # diccionario de key: (ip,port), value: conn
        self.control_de_latidos:dict[str, datetime] = {}  # una ip con una fecha
        self.control_de_latidos[self.ip] = datetime.now()  # tiempo en que el nodo se queda sin lider, inicialmente entra sin lider
        self.lider = None  # ip del nodo lider actual
        # cola de mensajes a enviar, a diferencia del centralizado, aqui guardaremos la tupla, mensaje-socket, para enviar el mensaje al receptor correcto
        self.message_queue = queue.Queue()
        self.stop_event = threading.Event()
        # hilo de envio
        self.send_thread = None
        self.status_lock = threading.Lock()
        #base de datos
        self.db_conn = None
        self.db_cursor = None
        self.logs_conn = None
        self.logs_cursor = None
        self.data_dir = f"/{self.name}"
        logging.info("a punto de iniciar base de datos")
        self.init_database()

    #========================Database=========================

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
                    database_name TEXT,
                    is_active BOOLEAN DEFAULT 1,
                    last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(node_id, database_name)    
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

    def leader_url_query(self, message, sender_id, node_type, timestamp, data):
        '''Metodo que ejecutara el nodo cuando es lider, revisa en el log de las urls, si esta toma una base
           de datos aleatoria de todas las que tienen la info y les pide el content de la url, en caso de que 
           no este en la tabla de logs, entonces devuelve False'''
        
        url = data.get('url')

        self.db_cursor.execute('''
            SELECT url_id FROM urls WHERE url = ?;
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
            for database_ip in databases_ips:
                with self.status_lock:
                    if database_ip in self.conexiones_activas.keys():
                        url_query_message = MessageProtocol.create_message(
                            msg_type=MessageProtocol.MESSAGE_TYPES['URL_QUERY'],
                            sender_id=self.ip,
                            node_type=self.node_type,
                            timestamp=datetime.now(),
                            data={'id_to_send':sender_id, 'url': url}
                        )
                        conn = self.conexiones_activas[database_ip]
                        self._enqueue_message(url_query_message, database_ip, conn)
                        break
                    
        else:
            # mandar mensaje de que no hay registro al que consulto
            url_query_response = MessageProtocol.create_message(
                msg_type=MessageProtocol.MESSAGE_TYPES['URL_QUERY_RESPONSE'],
                sender_id=self.ip,
                node_type=self.node_type,
                timestamp=datetime.now(),
                data={'response':False}
            )
            try:
                conn = self.conexiones_activas[sender_id]
                self._enqueue_message(url_query_response, sender_id, conn)
            except Exception as e:
                logging.error(f"fallo al acceder al socket de {sender_id} con {self.ip}: {e}")

    def no_leader_url_query(self, message, sender_id, node_type, timestamp, data):
        '''Metodo que ejecutara el nodo cuando no es lider, revisa en la base de datos la url, y devuelve el contenido'''
        url = data.get('url')
        id_to_send = data.get('id_to_send')

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
            content = content[0]
        
        #crear mensaje de respuesta
        content_message = MessageProtocol.create_message(
            msg_type=MessageProtocol.MESSAGE_TYPES['URL_QUERY_RESPONSE'],
            sender_id=self.ip,
            node_type=self.node_type,
            data={'response':True, 'content':content}
        )

        # hacer un socket temporal para mandar el mensaje
        try:
            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn.settimeout(30)
            conn.connect((id_to_send, self.port))

            if self._enqueue_message(content_message, id_to_send, conn):
                logging.info(f"mensaje de url_query_response a {id_to_send} encolado")
        
        except Exception as e:
            logging.error(f"error al enviar mensaje de tipo url_query_response a {id_to_send}: {e}")
            
    def recive_task_result(self, message, sender_id, node_type, timestamp, data):
        '''Recibe lo escrapeado de la url directo de un nodo scrapper, y le manda la info
           a tres de las base de datos conectadas, para garantizar la replicabilidad de 3,
           el que recibe el mensaje es el lider'''
        
        result = data.get('result')
        url = result.get('url')
        content = result.get('content')
        completed_at = data.get('completed_at')

        message_to_no_leaders = MessageProtocol.create_message(
            msg_type=MessageProtocol.MESSAGE_TYPES['TASK_RESULT_NO_LEADER'],
            sender_id=sender_id,
            node_type=node_type,
            timestamp=datetime.now().isoformat(),
            data={'url': url, 'content': content, 'completed_at': completed_at}
        )

        # enviar a tres bases de datos aleatorias conectadas
        databases_conectadas = list(self.conexiones_activas.keys())
        random.shuffle(databases_conectadas)
        databases_seleccionadas = databases_conectadas[:3] # no importa si son menos de tres, se envian a las que haya

        for database_ip in databases_seleccionadas:
            conn = self.conexiones_activas[database_ip]
            self._enqueue_message(message_to_no_leaders, database_ip, conn)
            logging.info(f"mensaje de task_result_no_leader a {database_ip} encolado")
        
    def recive_task_result_no_leader(self, message, sender_id, node_type, timestamp, data):
        '''Recibe el resultado del lider y lo almacena en su base de datos'''
        url = data.get('url')
        content = data.get('content')
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
        
            


    #========================database=========================

        

    #=========================Escucha=========================

    def iniciar_escucha(self):
        '''Escuchando conexiones entrantes'''
        self.sock_de_servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock_de_servidor.bind((self.ip, self.port))
        self.sock_de_servidor.listen(5)  # sin pasarle parametro es alrededor de 5, en caso de que no alcance para el trafico se aumenta
        self.escuchando = True
        logging.info(f"Escuchando en {self.ip}:{self.port}")

        try:
            # hilo de escucha
            threading.Thread(target=self.aceptar_conexiones, daemon=True).start()
        
        except KeyboardInterrupt :
            logging.info("Deteniendo nodo")

    def aceptar_conexiones(self):
        '''Recibe una conexion entrante'''
        while self.escuchando:
            try:
                conn, addr = self.sock_de_servidor.accept()
                logging.info(f"Conexion entrante de {addr}")
                if addr not in self.conexiones_activas.keys():
                    self.conexiones_activas[addr[0]] = conn
                    self.control_de_latidos[addr[0]] = datetime.now()
                    threading.Thread(target=self.manejar_conexion, args=(conn, addr), daemon=True).start()
                    threading.Thread(target=self.handle_heartbeat, daemon=True, args=(addr[0],)).start()
            
            except Exception as e:
                logging.error(f"Error aceptando conexion: {e}")

    def manejar_conexion(self, conn, addr):
        '''Recibe los mensajes dado un socket con otro nodo'''
        try:
            while True:
                try:
                    message = self.recibir_mensaje(conn)
                    if not message:
                        break
                    try:
                        message_dict = json.loads(message)
                        self.procesar_mensaje(message_dict, addr[0])
                    except Exception as e:
                        logging.error(f"Error procesando mensaje de {addr[0]}: {e}\nmessage: {message}")
                except Exception as e:
                    continue        
                #logging.info(f"Recibido de {addr}: {message}")
        except Exception as e:
            logging.error(f"Error manejando conexion con {addr}: {e}")
        
        finally:
            logging.info(f"Conexion cerrada con {addr}")
            conn.close()
            if addr[0] in self.conexiones_activas.keys():
                del self.conexiones_activas[addr[0]]

    def procesar_mensaje(self, message, ip):
        '''Encargado de tomar decision segun el mensaje que entra'''
        msg_type = message.get('type')
        sender_id = message.get('sender_id')
        node_type = message.get('node_type')
        timestamp = message.get('timestamp')
        data = message.get('data')

        handlers = {
            MessageProtocol.MESSAGE_TYPES['HEARTBEAT']: self.receive_heartbeat,
            MessageProtocol.MESSAGE_TYPES['LEADER_QUERY']: self.enviar_lider_actual,
            MessageProtocol.MESSAGE_TYPES['LEADER_RESPONSE']: self.recibir_respuesta_de_lider_query,
            MessageProtocol.MESSAGE_TYPES['URL_QUERY_LEADER']: self.leader_url_query,
            MessageProtocol.MESSAGE_TYPES['URL_QUERY']: self.no_leader_url_query,
            MessageProtocol.MESSAGE_TYPES['TASK_RESULT']: self.recive_task_result,
            #MessageProtocol.MESSAGE_TYPES['ELECTION']: self.handle_election,
            #MessageProtocol.MESSAGE_TYPES['ANSWER']: self.handle_answer,
            #MessageProtocol.MESSAGE_TYPES['COORDINATOR']: self.handle_coordinator,
            #MessageProtocol.MESSAGE_TYPES['HEARTBEAT_RESPONSE']: self.handle_heartbeat_response,
            #MessageProtocol.MESSAGE_TYPES['JOIN_NETWORK']: self.handle_join_network,
            #MessageProtocol.MESSAGE_TYPES['LEAVE_NETWORK']: self.handle_leave_network,
            #MessageProtocol.MESSAGE_TYPES['NODE_LIST']: self.handle_node_list,
        }

        logging.info(f"recibiendo mensaje de {sender_id} tipo {node_type} de tipo {msg_type}")
        handler = handlers.get(msg_type)
        
        if handler:
            try:
                handler(message, sender_id, node_type, timestamp, data)
            except Exception as e:
                logging.error(f"error en handler: {e}")
        else:
            logging.error(f"Tipo de mensaje: {message.get('type')} desconocido")


    def recibir_mensaje(self, socket_):
        '''Encargado de recibir el mensaje con protocolo longitud-mensaje'''
        #recibir longitud del mensaje
        lenght_bytes = socket_.recv(2)
        if not lenght_bytes:
            return False
        
        lenght = int.from_bytes(lenght_bytes, 'big')

        #recibir mensaje completo
        data = b""
        while(len(data) < lenght):
            len_data = len(data)
            chunk = socket_.recv(min(1024, lenght - len_data))
            if not chunk:
                break
            data += chunk
            
            if len(data) == lenght:
                try:
                    #message = MessageProtocol.parse_message(data.decode())
                    message = json.loads(data.decode())
                    return message
                except json.JSONDecodeError as e:
                    logging.error(f"Error al cargar json: {e}")
            
    #==========================escucha==========================

    #================Para conectarse con otros nodos==============

    def buscar_semejantes(self, intervalo=10):
        '''Busca los ips de los contenedores del mismo tipo, usando dns de docker'''
        
        def rm_nums(cadena:str):
            return ''.join(car for car in cadena if not car.isdigit())

        # el --name de los contenedores es el mismo, variando al terminacion en digitos, quitandole los digitos se queda igual que el --network-alias
        while True:
            try:
                ips = socket.getaddrinfo(rm_nums(self.name), self.port, proto=socket.IPPROTO_TCP)
                ip_addresses = [item[4][0] for item in ips]
                logging.info(f"Contenedores {rm_nums(self.name)} encontrados: {ip_addresses}")
                
                # intentar conectar con cada ip
                for ip in ip_addresses:
                    if ip == self.ip:
                        continue

                    try:
                        self.conectar_a(ip)

                    except (socket.timeout, ConnectionRefusedError) as e:
                        logging.error(f"No se pudo conectar a {ip}:{self.port}: {e}")

                time.sleep(10)
            
            except socket.gaierror as e:
                logging.error(f"No se pudo resolver el name: {self.name}")
                logging.info(f"Intentando de nuevo dentro de {intervalo} segundos")
                time.sleep(intervalo)
                continue

    def conectar_a(self, destino):
        if destino in self.conexiones_activas.keys():
            logging.info(f"Ya existe conexion con {destino}")
            return self.conexiones_activas[destino]
        
        try:
            sock_cliente = socket.create_connection((destino, self.port), timeout=5)
            logging.info(f"Conectado a {destino}:{self.port}")
            self.conexiones_activas[destino] = sock_cliente
            self.control_de_latidos[destino] = datetime.now()
            # iniciar hilo de timeout para la posible desconexion
            logging.info("antes de entrar al hilo de handle_heartbeat")
            threading.Thread(target=self.manejar_conexion, daemon=True, args=(sock_cliente, sock_cliente.getsockname())).start()
            threading.Thread(target=self.handle_heartbeat, daemon=True, args=(destino,)).start()
            return sock_cliente
        except socket.timeout:
            logging.error(f"Timeout error al conectarse a {destino}")
        except socket.error as e:
            logging.error(f"Error conectando a {destino[0]}:{destino[1]} - {e}")
                          
        return None

    #=================para conectarse a otros nodos==================


    #===================Para el envio de mensajes==================== 

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
        if conn not in self.conexiones_activas.values():
            logging.error(f"El nodo: {ip} no esta conectado a nodo {self.ip} actual")
            return False
        
        try:
            message_bytes = json.dumps(message_dict).encode()
            self.message_queue.put((message_bytes, conn))
            return True
        
        except Exception as e:
            logging.error(f"Error encolando mensaje: {message_bytes}: {e}")
            return False                
            
    #====================para el envio de mensajes=================

    #====================Funcionalidades del nodo==================

    def send_heartbeat(self):
        '''envia senial periodica a todos los similares a los que esta conectado''' # como el proyecto es a fallas de 2 nodos, tendra dos companieros
        while True:
            try:

                heartbeat_msg = MessageProtocol.create_message(
                    msg_type=MessageProtocol.MESSAGE_TYPES['HEARTBEAT'],
                    sender_id=self.ip,
                    node_type=self.node_type,
                    timestamp=datetime.now().isoformat()
                )

                logging.info(f"conexiones activas {self.conexiones_activas}")
                connections_copy = self.conexiones_activas.copy()
                for ip, conn in connections_copy.items():
                    logging.info(f"ip: {ip}")
                    if self._enqueue_message(heartbeat_msg, ip, conn):
                        logging.info(f"mensaje de latido a {ip} encolado")

                time.sleep(10)
            
            except Exception as e:
                logging.error(f"Error al enviar un latido: {e}")
    
    def receive_heartbeat(self, message, sender_id, node_type, timestamp, data):
        '''Actualiza el ultimo latido para el control de latidos'''
        self.control_de_latidos[sender_id] = datetime.now()
        logging.info(f"latido de {sender_id} recibido con exito")

    def handle_heartbeat(self, node_ip, timeout=15):
        '''Hilo encargado de verificar que el nodo node_ip este dando latidos'''
        while(True):
            #logging.info(f"Entrando al ciclo de handle_heartbeat y durmiendo proceso por {timeout} segundos ...")
            time.sleep(timeout)
            #logging.info("Saliendo del suenio del proceso ...")
            #logging.info(f"{self.control_de_latidos}")
            dif = abs((datetime.now() - self.control_de_latidos[node_ip]).total_seconds())
            #logging.info(f"Diferencia de {dif} segundos")
            if dif > 35:
                logging.error(f"El nodo {node_ip} no responde, cerrando socket...")
                self.conexiones_activas[node_ip].close()
                del(self.conexiones_activas[node_ip])
                break
            
            #logging.info(f"Diferencia de {dif}\nEl nodo esta activo, continuando ciclo ...")

    

    #===================funcionalidades del nodo===================

    #======================Eleccion de lider=======================

    def verificar_conec_de_lider(self, timeout = 15):
        '''hilo encargado de verificar la conectividad del lider actual'''
        while(True):
            if self.lider != None:
                if self.lider not in self.conexiones_activas and self.lider != self.ip:
                    self.lider = None
                    continue
            else:
                self.preguntar_por_lider()    

                time.sleep(15)

    def preguntar_por_lider(self):
        '''proceso encargado de elegir a un lider en caso de no tener'''
        if self.lider == None:
            if len(self.conexiones_activas) == 0:
                self.lider = self.ip
                logging.info("Asignandose a si mismo como lider")
                logging.info(f"lider actual: {self.lider}")
            
            dif = abs((datetime.now() - self.control_de_latidos[self.ip]).total_seconds())
            if dif > 15:
                ips_actuales = list(self.conexiones_activas.keys())
                ips_actuales.append(self.ip)
                self.lider = comparar_lista_ips(ips_actuales)
                #self.lider = self.ip
                logging.info(f"asignando como lider al de mayor ip de todos los nodos {self.node_type}: {self.lider}")
                logging.info("enviando lider actual al resto de los nodos ...")
                self.enviar_lider_actual()
            
            else:
                lider_query_message = MessageProtocol.create_message(
                    msg_type=MessageProtocol.MESSAGE_TYPES['LEADER_QUERY'],
                    sender_id=self.ip,
                    node_type=self.node_type,
                    timestamp=datetime.now().isoformat()
                )

                conexiones_act_copia = self.conexiones_activas.copy()
                for ip, conn in conexiones_act_copia.items():
                    if self._enqueue_message(lider_query_message, ip, conn):
                        logging.info(f"mensaje de lider_query a {ip} encolado")

    def recibir_respuesta_de_lider_query(self, message, sender_id, node_type, timestamp, data):
        '''proceso encargado de asignar el lider, o actualizar el lider'''
        try:
            leader = data.get('leader')
            if self.lider == None:
                self.lider = leader
                logging.info(f"recibiendo respuesta de lider_response, asignando como lider a: {leader}")
            
            elif self.lider != leader:
                logging.info(f"lider antes {self.lider}, lider recibido en response: {leader}")
                self.lider = comparar_lista_ips([self.lider, leader])
                logging.info(f"lider despues de comparar: {self.lider}")
                #enviar lider a todos los analogos ya que hay discordancia en quien es el lider
                logging.info("enviando lider actual al resto de los nodos")
                self.enviar_lider_actual()
        except Exception as e:
            logging.error(f"error recibiendo respuesta de lider_query: {e}")

    def enviar_lider_actual(self, message=None, sender_id=None, node_type=None, timestamp=None, data=None):
        '''proceso encargado a enviar el lider actual, ya sea para responder al mensaje leader_query o desde otro metodo'''
        if self.lider == None:
            return
        
        leader_message = MessageProtocol.create_message(
            msg_type=MessageProtocol.MESSAGE_TYPES['LEADER_RESPONSE'],
            sender_id=self.ip,
            node_type=self.node_type,
            timestamp=datetime.now().isoformat(),
            data={'leader':self.lider}
        )

        if sender_id != None:
            try:
                conn:socket.socket = self.conexiones_activas[sender_id]
                self._enqueue_message(leader_message, sender_id, conn)
                logging.info(f"Mensaje de lider a {sender_id} encolado")
            
            except Exception as e:
                logging.error(f"Error al encolar mensaje de lider: {e}")

        else:
            for ip, conn in self.conexiones_activas.items():    
                self._enqueue_message(leader_message, ip, conn)
                logging.info(f"mensaje de lider a {ip} encolado")

            

    #======================eleccion de lider=======================

    def cerrar_nodo(self):
        conex_activas_copy = self.conexiones_activas.copy()
        for ip, conn in conex_activas_copy.items():
            try:
                conn.close()
            except Exception:
                pass
        if self.db_conn:
            self.db_conn.close()
        self.stop_event.set()
        logging.info(f"cerrando nodo {self.name} ip: {self.ip}")

    #=======================Bucle principal========================

    def iniciar_nodo(self):
        '''echa a andar todos los procesos del nodo'''
        self.iniciar_escucha()
        threading.Thread(target=self.send_worker, daemon=True).start()
        threading.Thread(target=self.buscar_semejantes, daemon=True).start() 
        threading.Thread(target=self.send_heartbeat, daemon=True).start()
        time.sleep(2)
        threading.Thread(target=self.verificar_conec_de_lider, daemon=True).start()

        while(True):
            logging.info('entrando al ciclo principal')
            logging.info(f"lider actual: {self.lider}")
            time.sleep(20)
            
    #======================bucle principal=========================



if __name__ == "__main__":#
    databaseNode = DatabaseNode()
    try:
        databaseNode.iniciar_nodo()
    except KeyboardInterrupt:
        databaseNode.cerrar_nodo()