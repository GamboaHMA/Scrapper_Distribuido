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

#utils

PEERS = ['db-test']
PORT = 8080

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class ServiceDiscoverer:
    def __init__(self, port=PORT):
        
        self.name = socket.gethostname()
        logging.info(f"name: {self.name}")
        self.ip = socket.gethostbyname(self.name)
        self.port = port
        self.sock_de_servidor = None
        self.escuchando = False
        self.conexiones_activas = {}
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

            #while True:
                # hilo para buscar semejantes
                #self.buscar_semejantes()
                #time.sleep(0.1)
        
        except KeyboardInterrupt :
            logging.info("Deteniendo nodo")
        # finally:
        #     self.cerrar_todo()
        #     logging.info("Nodo detenido")

    def aceptar_conexiones(self):
        '''Recibe una conexion entrante'''
        while self.escuchando:
            try:
                conn, addr = self.sock_de_servidor.accept()
                logging.info(f"Conexion entrante de {addr}")
                if addr not in self.conexiones_activas.keys():
                    self.conexiones_activas[addr[0]] = conn
                    threading.Thread(target=self.manejar_conexion, args=(conn, addr), daemon=True).start()
            
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
                    self.procesar_mensaje(message, addr[0])
                except Exception as e:
                    logging.error(f"Error procesando mensaje de {addr[0]}: {e}")

                logging.info(f"Recibido de {addr}: {message}")
        
        finally:
            logging.info(f"Conexion cerrada con {addr}")
            conn.close()
            if addr[0] in self.conexiones_activas.keys():
                del self.conexiones_activas[addr[0]]

    def procesar_mensaje(self, message, ip):
        '''Encargado de tomar decision segun el mensaje que entra'''
        msg_type = message.get('type')

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
                message = json.loads(data.decode())
                return message
            

    # ============================escucha===========================


    # =================Para conectarse a otro nodo==================

    def buscar_semejantes(self, intervalo=10):
        '''Busca el ip de los contenedores que tengan el mismo network-alias que el propio contenedor'''

        def rm_nums(cadena:str):
            return ''.join(car for car in cadena if not car.isdigit())

        # el --name de los contenedores es el mismo que el --network-alias, solo que --name tiene digitos
        while True:
            try:
                ips = socket.getaddrinfo(rm_nums(self.name), self.port, proto=socket.IPPROTO_TCP)
                logging.info(f"ips: {ips}")
                ip_addresses = [item[4][0] for item in ips]
                logging.info(f"Contenedores {rm_nums(self.name)} encontrados: {ip_addresses}")

                #intentar conectar con cada ip
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
                logging.info(f"Intentandolo de nuevo dentro de {intervalo} segundos")
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
            return sock_cliente
        except socket.timeout:
            logging.error(f"Timeout error al conectarse a {destino}")
        except socket.error as e:
            logging.error(f"Error conectando a {destino[0]}:{destino[1]} - {e}")
                          
        return None

    # =================para conectarse a otro nodo=================


    def cerrar_todo(self):
        '''Apagar el nodo'''
        self.escuchando = False
        if self.sock_de_servidor:
            self.sock_de_servidor.close()
        
        for ip, conn in self.conexiones_activas.items():
            conn.close


    #====================Para el envio de mensajes=================

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

                logging.info(f"conexiones activas {self.conexiones_activas}")
                for ip, conn in self.conexiones_activas.items():
                    logging.info(f"ip: {ip}")
                    if self._enqueue_message(heartbeat_msg, ip, conn):
                        logging.info('mensaje de latido encolado')

                time.sleep(10)
            
            except Exception as e:
                logging.error(f"Error al enviar un latido: {e}")
    
    

    #===================funcionalidades del nodo====================

    #========================Bucle principal========================

    def iniciar_nodo(self):
        '''echa a andar todos los procesos del nodo'''
        self.iniciar_escucha()
        threading.Thread(target=self.send_worker, daemon=True).start()
        threading.Thread(target=self.buscar_semejantes, daemon=True).start() 
        threading.Thread(target=self.send_heartbeat, daemon=True).start()

        while(True):
            logging.info('entrando al ciclo principal')
            time.sleep(20)



    #========================bucle principal========================

    def send_to_peer(self, peer_name, msg):
        try:
            peer_ip = socket.gethostbyname(peer_name)
            logging.info(f"peer_ip: {peer_ip}")
            own_ip = socket.gethostbyname(socket.gethostname())
            logging.info(f"own ip is: {own_ip}") 
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto(msg.encode(), (peer_ip, PORT))
            logging.info(f"Mensaje a {peer_name} ({peer_ip}): {msg}")
        except Exception as e:
            logging.error(f"Error comunicando con {peer_name}: {e}")

    def conexion_valida(self, destino):
        """Verifica si ya existe una conexión activa al destino."""
        return destino not in self.conexiones_activas

    def conectar(self, destino):
        """Intenta conectar al destino si no hay conexión activa."""
        if not self.conexion_valida(destino):
            print(f"Ya existe conexión activa a {destino}, evitando reconexión")
            return self.conexiones_activas[destino]

        try:
            sock = socket.create_connection(destino, timeout=5)
            self.conexiones_activas[destino] = sock
            print(f"Conectado a {destino}")
            return sock
        except socket.timeout:
            print(f"Timeout al conectar a {destino}")
            return None
        except socket.error as e:
            print(f"Error al conectar a {destino}: {e}")
            return None

    def cerrar_conexion(self, destino):
        """Cierra la conexión activa al destino si existe."""
        if destino in self.conexiones_activas:
            self.conexiones_activas[destino].close()
            del self.conexiones_activas[destino]
            print(f"Conexión a {destino} cerrada")
  
  #=================================================

if __name__ == '__main__':
    
    peer_to_peer_test = ServiceDiscoverer()
    logging.info(f"environment: {os.environ.get('HOSTNAME')}")

    peer_to_peer_test.iniciar_nodo()


