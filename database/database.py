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

class MessageProtocol:
    # tipos de mensaje
    MESSAGE_TYPES = {
        'DISCOVERY': 'discovery',
        'LEADER_QUERY': 'leader_query',
        'LEADER_RESPONSE': 'leader_response',
        'LEADER_ANNOUNCE': 'leader_announce',
        'ELECTION': 'election',
        'ANSWER': 'answer',
        'COORDINATOR': 'coordinator',
        'HEARTBEAT': 'heartbeat',
        'HEARTBEAT_RESPONSE': 'heartbeat_response',
        'NODE_LIST': 'node_list',
        'JOIN_NETWORK': 'join_network',
        'LEAVE_NETWORK': 'leave_network'
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
        self.conexiones_activas = {}
        self.control_de_latidos:dict[str, datetime] = {}  # una ip con una fecha
        # cola de mensajes a enviar, a diferencia del centralizado, aqui guardaremos la tupla, mensaje-socket, para enviar el mensaje al receptor correcto
        self.message_queue = queue.Queue()
        self.stop_event = threading.Event()
        # hilo de envio
        self.send_thread = None
        self.status_lock = threading.Lock()

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
                message = self.recibir_mensaje(conn)
                if not message:
                    break
                try:
                    message_dict = json.loads(message)
                    self.procesar_mensaje(message_dict, addr[0])
                except Exception as e:
                    logging.error(f"Error procesando mensaje de {addr[0]}: {e}\nmessage: {message}")
                    

                logging.info(f"Recibido de {addr}: {message}")
        
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
            #MessageProtocol.MESSAGE_TYPES['LEADER_QUERY']: self.handle_leader_query,
            #MessageProtocol.MESSAGE_TYPES['LEADER_RESPONSE']: self.handle_leader_response,
            #MessageProtocol.MESSAGE_TYPES['ELECTION']: self.handle_election,
            #MessageProtocol.MESSAGE_TYPES['ANSWER']: self.handle_answer,
            #MessageProtocol.MESSAGE_TYPES['COORDINATOR']: self.handle_coordinator,
            MessageProtocol.MESSAGE_TYPES['HEARTBEAT']: self.receive_heartbeat,
            #MessageProtocol.MESSAGE_TYPES['HEARTBEAT_RESPONSE']: self.handle_heartbeat_response,
            #MessageProtocol.MESSAGE_TYPES['JOIN_NETWORK']: self.handle_join_network,
            #MessageProtocol.MESSAGE_TYPES['LEAVE_NETWORK']: self.handle_leave_network,
            #MessageProtocol.MESSAGE_TYPES['NODE_LIST']: self.handle_node_list,
        }

        handler = handlers.get(msg_type)
        
        if handler:
            try:
                handler(message, sender_id, node_type, timestamp, data)
            except Exception as e:
                logging.error(f"error en handler: {e}")
        else:
            logging.error(f"Tipo de mensaje: {message.get('type')} desconocido")

        if msg_type == 'heartbeat':
            last_heartbeat = datetime.now().isoformat()
            logging.info(f"Recibiendo ping de: {ip} {last_heartbeat}")

        else:
            logging.error(f"El mensaje de {ip} mensaje: {message} no tiene las caracteristicas esperadas")

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
                ip_addreses = [item[4][0] for item in ips]
                logging.info(f"Contenedores {rm_nums(self.name)} encontrados: {ip_addreses}")
                
                # intentar conectar con cada ip
                for ip in ip_addreses:
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
            logging.info(f"Conectado2 a {destino}:{self.port} ")
            self.conexiones_activas[destino] = sock_cliente
            self.control_de_latidos[destino] = datetime.now()
            # iniciar hilo de timeout para la posible desconexion
            logging.info("antes de entrar al hilo de handle_heartbeat")
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

                if message is None:
                    logging.info('message is none')
                    break

                try:
                    # enviar longitud
                    lenght = len(message)
                    conn.send(lenght.to_bytes(2, 'big'))
                    
                    # enviar mensaje completo
                    conn.send(message)
                    logging.info('mensaje enviado')

                except Exception as e:
                    logging.error(f"Error enviando mensaje: {e}")
                    
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
                heartbeat_msg = {
                    'type': 'heartbeat',
                    'node_ip': self.ip,
                    'node_name': self.name,
                    'time_now': datetime.now().isoformat()
                }

                heartbeat_msg = MessageProtocol.create_message(
                    msg_type=MessageProtocol.MESSAGE_TYPES['HEARTBEAT'],
                    sender_id=self.ip,
                    node_type=self.node_type,
                    timestamp=datetime.now().isoformat()
                )

                logging.info(f"conexiones activas {self.conexiones_activas}")
                for ip, conn in self.conexiones_activas.items():
                    logging.info(f"ip: {ip}")
                    if self._enqueue_message(heartbeat_msg, ip, conn):
                        logging.info('mensaje de latido encolado')

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
            logging.info(f"Entrando al ciclo de handle_heartbeat y durmiendo proceso por {timeout} segundos ...")
            time.sleep(timeout)
            logging.info("Saliendo del suenio del proceso ...")
            logging.info(f"{self.control_de_latidos}")
            dif = abs((datetime.now() - self.control_de_latidos[node_ip]).total_seconds())
            logging.info(f"Diferencia de {dif} segundos")
            if dif > 35:
                logging.error(f"El nodo {node_ip} no responde, cerrando socket...")
                del(self.conexiones_activas[node_ip])
                break
            
            logging.info(f"Diferencia de {dif}\nEl nodo esta activo, continuando ciclo ...")


    #===================funcionalidades del nodo====================

    #=======================Bucle principal========================

    def iniciar_nodo(self):
        '''echa a andar todos los procesos del nodo'''
        self.iniciar_escucha()
        threading.Thread(target=self.send_worker, daemon=True).start()
        threading.Thread(target=self.buscar_semejantes, daemon=True).start() 
        threading.Thread(target=self.send_heartbeat, daemon=True).start()

        while(True):
            logging.info('entrando al ciclo principal')
            time.sleep(20)

    #======================bucle principal=========================



if __name__ == "__main__":#
    databaseNode = DatabaseNode()
    databaseNode.iniciar_nodo()