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
        'LEADER_ANNOUNCE': 'leader_announce',
        'LEADER_QUERY_TO_OTHER_BOSS': 'leader_query_to_other_boss',
        'LEADER_RESPONSE_TO_OTHER_BOSS': 'leader_response_to_other_boss',
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

NODE_TYPE_DB = 'db'
NODE_TYPE_SCRAPPER = 'scrapper'
NODE_TYPE_ROUTER = 'router'
NODE_TYPE_CLIENT = 'client'

PORT_ROUTER = 8080
PORT_DB = 7070
PORT_SCRAPPER = 9090

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class ScrappingNode:
    def __init__(self, port=PORT_SCRAPPER):
        '''Metodo de inicializacion del scrapper'''

        self.node_type = NODE_TYPE_SCRAPPER
        self.name = socket.gethostname()
        self.ip = socket.gethostbyname(self.name)
        self.port = port
        self.sock_escucha = None
        self.escuchando = False
        self.lider = None  # IP del lider actual
        self.conexiones_activas = {}  # diccionario de conexiones activas {ip: socket}
        self.control_de_latidos:dict[str, datetime] = {}
        self.control_de_latidos[self.ip] = datetime.now()
        # cola de mensajes a enviar
        self.message_queue = queue.Queue()
        self.stop_event = threading.Event()
        # hilo de envio
        self.send_trend = None
        self.status_lock = threading.Lock()
        # jefes directamente
        self.jefe_router = None  # ((ip, addr), sock)
        self.jefe_db = None  # ((ip, addr), sock)


    #========================Escucha======================
    
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
                        self.procesar_mensaje(message, addr[0])
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
            MessageProtocol.MESSAGE_TYPES['LEADER_QUERY_TO_OTHER_BOSS']: self.enviar_lider_a_otro_jefe,
            MessageProtocol.MESSAGE_TYPES['LEADER_RESPONSE_TO_OTHER_BOSS']: self.handle_lider_response_from_other_boss,
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

    def conectar_a(self, destino, port=None):
        '''Metodo para conectarse a otro nodo'''
        if not port:
            port = self.port
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
            logging.error(f"Error conectando a {destino}:{self.port} - {e}")
            logging.info(f"destino: {destino}")
                          
        return None

    def buscar_otros_lideres(self, intervalo=20):
        '''Metodo para buscar otros lideres en la red'''
        
        while self.lider == self.ip:
            try:

                if self.jefe_router == None:
                    # Resolver directamente el node_type como network-alias (ej: 'router')
                    # Para db
                    result = socket.getaddrinfo(
                        NODE_TYPE_ROUTER, 
                        self.port, 
                        socket.AF_INET, 
                        socket.SOCK_STREAM
                    )
                    
                    # Extraer IPs únicas excluyendo la propia
                    router_ips = [addr_info[4][0] for addr_info in result]
                    logging.debug(f"Nodos '{NODE_TYPE_ROUTER}' encontrados para lideres: {router_ips}")

                    # Preguntar por lider a cada uno
                    msg_leader_query = MessageProtocol.create_message(
                        msg_type=MessageProtocol.MESSAGE_TYPES['LEADER_QUERY_TO_OTHER_BOSS'],
                        sender_id=self.ip,
                        node_type=self.node_type,
                        timestamp=datetime.now().isoformat()
                    )
                    for ip in router_ips:
                        try:
                            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            sock.settimeout(5)
                            sock.connect((ip, PORT_ROUTER))

                            msg_bytes = msg_leader_query.encode()
                            sock.send(len(msg_bytes).to_bytes(2, 'big'))
                            sock.send(msg_bytes)
                            logging.info(f"Mensaje de leader_query enviado a {ip}:{PORT_ROUTER} para lideres")
                            sock.close()

                        except socket.timeout as e:
                            logging.debug(f"No se pudo conectar a {ip}:{PORT_ROUTER} para lideres: {e}")
                        except Exception as e:
                            logging.error(f"Error conectando a {ip}:{PORT_ROUTER} para lideres: {e}")
                
                if self.jefe_db == None:
                    # Resolver directamente el node_type como network-alias (ej: 'router')
                    # Para scrapper
                    result = socket.getaddrinfo(
                        NODE_TYPE_DB, 
                        self.port, 
                        socket.AF_INET, 
                        socket.SOCK_STREAM
                    )
                    
                    # Extraer IPs únicas excluyendo la propia
                    scrapper_ips = [addr_info[4][0] for addr_info in result]
                    logging.debug(f"Nodos '{NODE_TYPE_DB}' encontrados para lideres: {scrapper_ips}")

                    # Preguntar por lider a cada uno
                    msg_leader_query = MessageProtocol.create_message(
                        msg_type=MessageProtocol.MESSAGE_TYPES['LEADER_QUERY_TO_OTHER_BOSS'],
                        sender_id=self.ip,
                        node_type=self.node_type,
                        timestamp=datetime.now().isoformat()
                    )
                    for ip in scrapper_ips:
                        try:
                            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            sock.settimeout(5)
                            sock.connect((ip, PORT_DB))

                            msg_bytes = msg_leader_query.encode()
                            sock.send(len(msg_bytes).to_bytes(2, 'big'))
                            sock.send(msg_bytes)
                            logging.info(f"Mensaje de leader_query enviado a {ip}:{PORT_DB} para lideres")
                            sock.close()

                        except socket.timeout as e:
                            logging.debug(f"No se pudo conectar a {ip}:{PORT_DB} para lideres: {e}")
                        except Exception as e:
                            logging.error(f"Error conectando a {ip}:{PORT_DB} para lideres: {e}")
                

                time.sleep(intervalo)
            
            except socket.gaierror as e:
                logging.warning(f"No se pudo resolver node_type {NODE_TYPE_DB} o {NODE_TYPE_ROUTER} en Docker DNS para lideres: {e}")
                logging.debug(f"Reintentando en {intervalo} segundos...")
                time.sleep(intervalo)
            except Exception as e:
                logging.error(f"Error en descubrimiento de nodos para lideres: {e}")
                time.sleep(intervalo)

    def enviar_lider_a_otro_jefe(self, message=None, sender_id=None, node_type=None, timestamp=None, data=None):
        '''Envía el líder actual al nodo que lo solicitó'''

        if self.lider and self.lider[0][0] in [addr[0] for addr in self.conexiones_activas.keys()]:
            lider_actual = (self.lider, self.port)
        
            respuesta_msg = MessageProtocol.create_message(
                msg_type=MessageProtocol.MESSAGE_TYPES['LEADER_RESPONSE_TO_OTHER_BOSS'],
                sender_id=self.ip,
                node_type=self.node_type,
                timestamp=datetime.now().isoformat(),
                data={'lider': lider_actual}
            )

            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                if node_type == NODE_TYPE_ROUTER:
                    sock.connect((sender_id, PORT_ROUTER))
                elif node_type == NODE_TYPE_DB:
                    sock.connect((sender_id, PORT_DB))
                msg_bytes = respuesta_msg.encode()
                sock.send(len(msg_bytes).to_bytes(2, 'big'))
                sock.send(msg_bytes)
                logging.info(f"Mensaje de líder actual enviado a {sender_id}")
                sock.close()
            
            except socket.timeout as e:
                logging.debug(f"No se pudo conectar a {sender_id} para enviar líder: {e}")
            except Exception as e:
                logging.error(f"Error conectando a {sender_id} para enviar líder: {e}")

        else:
            logging.info(f"No hay conexión activa con {sender_id} para enviar líder")


    def handle_lider_response_from_other_boss(self, message, sender_id, node_type, timestamp, data):
        '''Maneja la respuesta de líder de otro jefe'''
        lider = data.get('lider', None)
        logging.info(f"Líder recibido de {sender_id}: {lider} de tipo {node_type}")

        if lider:
            if node_type == NODE_TYPE_ROUTER:
                lider_ip = lider[0][0]
                self.conectar_a(lider_ip, PORT_ROUTER)
                self.jefe_router = (lider, self.conectar_a(lider_ip, PORT_ROUTER))
                logging.info(f"encontrado a {lider} jefe de DB")
            elif node_type == NODE_TYPE_DB:
                self.jefe_db = (lider, self.conexiones_activas.get(lider, None))
                logging.info(f"Jefe DB actualizado a {lider}")


    #==============para conectarse con otros nodos======================

    #======================Para el envio de mensajes====================

    def send_worker(self):
        '''Hilo que envia los mensajes de la cola'''
        while not self.stop_event.is_set():
            try:
                # esperar mensaje con timeout para poder verificar stop_event
                message_bytes, conn = self.message_queue.get(timeout=1.0)
                ip, port = conn.getsockname()
                peer_ip, peer_port = conn.getpeername()
                message = message_bytes.decode()
                #logging.info(f"mensaje a enviar: {message}")
                msg_type = json.loads(message).get('type')

                if message_bytes is None:
                    logging.info('message is none')
                    break

                try:
                    # enviar longitud
                    lenght = len(message_bytes)
                    conn.send(lenght.to_bytes(2, 'big'))
                    
                    # enviar mensaje completo
                    conn.send(message_bytes)
                    logging.info(f"mensaje enviado a {peer_ip} de tipo {msg_type}")


                except Exception as e:
                    logging.error(f"Error enviando mensaje: a {ip}, {port} {e}")
                    
                    if not self.stop_event.is_set():
                        self.message_queue.put((message_bytes, conn))
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
            message_bytes = message_dict.encode()
            self.message_queue.put((message_bytes, conn))
            return True
        
        except Exception as e:
            logging.error(f"Error encolando mensaje: {message_bytes}: {e}")
            return False                

    #======================para el envio de mensajes====================

    #=====================Funcionalidades del Nodo=======================
    
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

    #=====================funcionalidades del Nodo=======================

    #=====================Eleccion de Lider===================

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

    #=====================Eleccion de Lider===================

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


    #=======================Bucle Principal======================

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

    #=======================bucle Principal======================

if __name__ == "__main__":
    scrapingNode = ScrappingNode()
    try:
        scrapingNode.iniciar_nodo()
    except KeyboardInterrupt as e:
        scrapingNode.cerrar_nodo()