import time
import socket
import threading
import logging
import subprocess
import json
import os
import shutil
import queue
from datetime import datetime
from utils import MessageProtocol

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


NODE_TYPE_ROUTER = 'router'
PORT = 8080

# Configuración de logging con soporte para LOG_LEVEL
log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class ServiceDiscoverer:
    def __init__(self, port=PORT):
        self.node_type = NODE_TYPE_ROUTER  # node_type también es el network-alias de Docker
        # self.name = socket.gethostname()
        # logging.debug(f"Hostname: {self.name}")
        self.ip = socket.gethostbyname(socket.gethostname())
        logging.info(f"Nodo iniciado - Tipo: {self.node_type}, IP: {self.ip}")
        self.port = port
        self.sock_de_servidor = None
        self.escuchando = False
        self.conexiones_activas = {}
        self.control_de_latidos:dict[str, datetime] = {}  # key:ip, value:fecha u hora
        self.control_de_latidos[self.ip] = datetime.now()
        self.lider = None
        # cola de mensajes a enviar, a diferencia del centralizado, aqui guardaremos la tupla, mensaje-socket, para enviar el mensaje al receptor correcto
        self.message_queue = queue.Queue()
        self.stop_event = threading.Event()
        # hilo de envio
        self.sent_thread = None
        self.status_lock = threading.Lock()


    # ============================Escucha===========================

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
                logging.debug(f"Conexion entrante de {addr}")
                
                # No aceptar conexiones de si mismo
                if addr[0] == self.ip:
                    logging.debug(f"Rechazando conexión de sí mismo ({self.ip})")
                    conn.close()
                    continue
                
                if addr not in self.conexiones_activas.keys() and addr[0] != self.ip:
                    self.conexiones_activas[addr[0]] = conn
                    logging.info(f"Nueva conexión establecida con {addr[0]}")
                    threading.Thread(target=self.manejar_conexion, args=(conn, addr), daemon=True).start()
                    threading.Thread(target=self.handle_heartbeat, daemon=True, args=(addr[0],)).start()
                else:
                    logging.debug(f"Conexión duplicada ignorada de {addr[0]}")
            
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
                        logging.debug(f"Mensaje recibido de {addr[0]}: {message_dict.get('type', 'unknown')}")
                        self.procesar_mensaje(message_dict, addr[0])
                    except Exception as e:
                        logging.error(f"Error procesando mensaje de {addr[0]}: {e}")
                except Exception as e:
                    logging.debug(f"Timeout recibiendo mensaje de {addr[0]}, continuando")
                    continue
        except Exception as e:
            logging.error(f"Error manejando conexion con {addr}: {e}")

        finally:
            logging.info(f"Conexión cerrada con {addr[0]}")
            conn.close()
            if addr[0] in self.conexiones_activas.keys():
                del self.conexiones_activas[addr[0]]
                logging.debug(f"Nodo {addr[0]} eliminado de conexiones activas")

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
            #MessageProtocol.MESSAGE_TYPES['ELECTION']: self.handle_election,
            #MessageProtocol.MESSAGE_TYPES['ANSWER']: self.handle_answer,
            #MessageProtocol.MESSAGE_TYPES['COORDINATOR']: self.handle_coordinator,
            #MessageProtocol.MESSAGE_TYPES['HEARTBEAT_RESPONSE']: self.handle_heartbeat_response,
            #MessageProtocol.MESSAGE_TYPES['JOIN_NETWORK']: self.handle_join_network,
            #MessageProtocol.MESSAGE_TYPES['LEAVE_NETWORK']: self.handle_leave_network,
            #MessageProtocol.MESSAGE_TYPES['NODE_LIST']: self.handle_node_list,
        }

        logging.debug(f"Procesando mensaje de {sender_id} (tipo {node_type}): {msg_type}")
        handler = handlers.get(msg_type)
        
        if handler:
            try:
                handler(message, sender_id, node_type, timestamp, data)
            except Exception as e:
                logging.error(f"Error en handler {msg_type} desde {sender_id}: {e}")
        else:
            logging.warning(f"Tipo de mensaje desconocido: {msg_type} de {sender_id}")


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
                    message = json.loads(data.decode())
                    return message
                except json.JSONDecodeError as e:
                    logging.error(f"Error al cargar json: {e}") 
            

    # ============================escucha===========================


    # =================Para conectarse a otro nodo==================

    def buscar_semejantes(self, intervalo=10):
        """
        Descubre otros nodos del mismo tipo usando el network-alias de Docker.
        
        Args:
            intervalo (int): Segundos entre cada intento de descubrimiento
        """
        while True:
            try:
                # Resolver directamente el node_type como network-alias (ej: 'router')
                # Docker DNS devolverá todas las IPs de contenedores con ese alias
                result = socket.getaddrinfo(
                    self.node_type, 
                    self.port, 
                    socket.AF_INET, 
                    socket.SOCK_STREAM
                )
                
                # Extraer IPs únicas excluyendo la propia
                ip_addresses = [addr_info[4][0] for addr_info in result]
                ip_addresses = list(set(ip_addresses) - {self.ip})
                logging.debug(f"Nodos '{self.node_type}' encontrados (excluyendo self): {ip_addresses}")

                # Intentar conectar con cada IP
                for ip in ip_addresses:
                    try:
                        self.conectar_a(ip)
                    except (socket.timeout, ConnectionRefusedError) as e:
                        logging.debug(f"No se pudo conectar a {ip}:{self.port}: {e}")
                
                time.sleep(intervalo)
            
            except socket.gaierror as e:
                logging.warning(f"No se pudo resolver node_type '{self.node_type}' en Docker DNS: {e}")
                logging.debug(f"Reintentando en {intervalo} segundos...")
                time.sleep(intervalo)
            except Exception as e:
                logging.error(f"Error en descubrimiento de nodos: {e}")
                time.sleep(intervalo)

    def conectar_a(self, destino):
        # No conectar a si mismo
        if destino == self.ip:
            logging.debug(f"Evitando conexión a sí mismo ({self.ip})")
            return None
        
        if destino in self.conexiones_activas.keys():
            logging.debug(f"Ya existe conexión con {destino}")
            return self.conexiones_activas[destino]
        
        try:
            sock_cliente = socket.create_connection((destino, self.port), timeout=5)
            logging.info(f"Conectado exitosamente a {destino}:{self.port}")
            self.conexiones_activas[destino] = sock_cliente
            self.control_de_latidos[destino] = datetime.now()
            # iniciar hilos de manejo
            logging.debug(f"Iniciando hilos de manejo para {destino}")
            threading.Thread(target=self.manejar_conexion, daemon=True, args=(sock_cliente, sock_cliente.getsockname())).start()
            threading.Thread(target=self.handle_heartbeat, daemon=True, args=(destino,)).start()
            return sock_cliente
        except socket.timeout:
            logging.debug(f"Timeout al conectarse a {destino}")
        except socket.error as e:
            logging.debug(f"Error conectando a {destino}: {e}")
                          
        return None

    # =================para conectarse a otro nodo=================


    #====================Para el envio de mensajes=================

    def send_worker(self):
        '''Hilo que envia los mensajes de la cola'''
        while not self.stop_event.is_set():
            try:
                # esperar mensaje con timeout para poder verificar stop_event
                message, conn = self.message_queue.get(timeout=1.0)

                if message is None:
                    logging.debug('Cola de mensajes vacía, finalizando worker')
                    break

                try:
                    # enviar longitud
                    lenght = len(message)
                    conn.send(lenght.to_bytes(2, 'big'))
                    
                    # enviar mensaje completo
                    conn.send(message)
                    logging.debug('Mensaje enviado exitosamente')

                except Exception as e:
                    logging.error(f"Error enviando mensaje (reintentando): {e}")
                    
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

                logging.debug(f"Conexiones activas: {len(self.conexiones_activas)} nodos")
                connections_copy = self.conexiones_activas.copy()
                for ip, conn in connections_copy.items():
                    if self._enqueue_message(heartbeat_msg, ip, conn):
                        logging.debug(f"Heartbeat encolado para {ip}")

                time.sleep(10)
            
            except Exception as e:
                logging.error(f"Error al enviar un latido: {e}")
    
    def receive_heartbeat(self, message, sender_id, node_type, timestamp, data):
        '''Actualiza el ultimo latido para el control de latidos'''
        self.control_de_latidos[sender_id] = datetime.now()
        logging.debug(f"Heartbeat recibido de {sender_id}")

    def handle_heartbeat(self, node_ip, timeout=15):
        '''Hilo encargado de verificar que el nodo node_ip este dando latidos'''
        while(True):
            time.sleep(timeout)
            if node_ip not in self.control_de_latidos:
                logging.debug(f"Nodo {node_ip} ya no está en control de latidos, finalizando monitor")
                break
            
            dif = abs((datetime.now() - self.control_de_latidos[node_ip]).total_seconds())
            logging.debug(f"Heartbeat check para {node_ip}: {dif:.1f}s desde último latido")
            
            if dif > 35:
                logging.warning(f"Nodo {node_ip} no responde (timeout de {dif:.1f}s). Eliminando conexión...")
                if node_ip in self.conexiones_activas:
                    del self.conexiones_activas[node_ip]
                break
            else:
                logging.debug(f"Nodo {node_ip} activo ({dif:.1f}s)")


    #===================funcionalidades del nodo====================

    #=======================Eleccion de Lider=======================

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
                logging.info(f"🔶 Autoproclámandose como líder (sin otros nodos): {self.lider}")
            
            dif = abs((datetime.now() - self.control_de_latidos[self.ip]).total_seconds())
            if dif > 15:
                ips_actuales = list(self.conexiones_activas.keys())
                ips_actuales.append(self.ip)
                self.lider = comparar_lista_ips(ips_actuales)
                logging.info(f"🔶 Líder elegido por IP mayor: {self.lider} de {len(ips_actuales)} nodos")
                logging.debug("Notificando líder al resto de nodos...")
                self.enviar_lider_actual()
            
            else:
                logging.debug("Consultando por líder a nodos conectados...")
                lider_query_message = MessageProtocol.create_message(
                    msg_type=MessageProtocol.MESSAGE_TYPES['LEADER_QUERY'],
                    sender_id=self.ip,
                    node_type=self.node_type,
                    timestamp=datetime.now().isoformat()
                )

                conexiones_act_copia = self.conexiones_activas.copy()
                for ip, conn in conexiones_act_copia.items():
                    if self._enqueue_message(lider_query_message, ip, conn):
                        logging.debug(f"Leader query encolado para {ip}")

    def recibir_respuesta_de_lider_query(self, message, sender_id, node_type, timestamp, data):
        '''proceso encargado de asignar el lider, o actualizar el lider'''
        try:
            leader = data.get('leader')
            if self.lider == None:
                self.lider = leader
                logging.info(f"Líder asignado desde respuesta: {leader}")
            
            elif self.lider != leader:
                logging.warning(f"Conflicto de líder detectado: local={self.lider}, remoto={leader}")
                self.lider = comparar_lista_ips([self.lider, leader])
                logging.info(f"Líder resuelto: {self.lider}")
                logging.debug("Notificando líder resuelto a todos los nodos")
                self.enviar_lider_actual()
        except Exception as e:
            logging.error(f"Error procesando respuesta de leader_query: {e}")

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
                logging.debug(f"Respuesta de líder encolada para {sender_id}")
            
            except Exception as e:
                logging.error(f"Error encolando respuesta de líder a {sender_id}: {e}")

        else:
            logging.debug(f"Broadcasting líder {self.lider} a {len(self.conexiones_activas)} nodos")
            for ip, conn in self.conexiones_activas.items():
                self._enqueue_message(leader_message, ip, conn)


    #=======================eleccion de lider=======================


    #========================Bucle principal========================

    def iniciar_nodo(self):
        '''echa a andar todos los procesos del nodo'''
        self.iniciar_escucha()
        threading.Thread(target=self.send_worker, daemon=True).start()
        threading.Thread(target=self.buscar_semejantes, daemon=True).start() 
        threading.Thread(target=self.send_heartbeat, daemon=True).start()
        time.sleep(2)
        threading.Thread(target=self.verificar_conec_de_lider, daemon=True).start()

        while(True):
            logging.info(f"Estado: Líder={self.lider}, Conexiones={len(self.conexiones_activas)}")
            time.sleep(20)



    #========================bucle principal========================

  
if __name__ == '__main__':
    serviceDiscover = ServiceDiscoverer()
    serviceDiscover.iniciar_nodo()    


