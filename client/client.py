import socket
import json
import time
import threading
import random
import logging
from datetime import datetime


# config del logging (igual que en el server)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class ClientNode():
    def __init__(self, server_host='0.0.0.0', server_port=8080) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.client_id = None
        self.connected = False
        self.socket = None
        self.current_task = {}
        self.send_lock = threading.Lock()

    def connect_to_server(self):
        '''conecta al servidor central'''
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.server_host, self.server_port))
            self.connected = True
            logging.info(f"Conectado al servidor {self.server_host}:{self.server_port}")

            # recibe mensaje de welcome de server
            welcome_data = self.socket.recv(1024).decode()
            welcome_msg = json.loads(welcome_data)
            self.client_id = welcome_msg.get('client_id')
            logging.info(f"ID asignado: {self.client_id}")

            return True

        except Exception as e:
            logging.error(f"Error conectando aal servidor: {e}")
            return False
    
    def send_heartbeat(self):
        '''envia senial periodica al server'''
        while(self.connected):
            try:
                heartbeat_msg = {
                    'type': 'heartbeat',
                    'time_now': datetime.now().isoformat()
                }
                message = json.dumps(heartbeat_msg).encode()

                with self.send_lock:
                    lenght = len(message)
                    self.socket.send(lenght.to_bytes(4, 'big'))
                    self.socket.send(message)

                time.sleep(5) # latido cada 5 segundos
            
            except Exception:
                self.connected = False

    def send_status(self, status):
        '''envia estado al servidor'''
        try:
            status_msg = {
                'type': 'status',
                'status': status,
                'time_now': datetime.now().isoformat()
            }
            message = json.dumps(status_msg).encode()
            with self.send_lock:
                lenght = len(message)
                self.socket.send(lenght.to_bytes(4, 'big'))
                self.socket.send(message)
        
        except Exception:
            self.connected = False
    
    def execute_task(self, task_id, task_data):
        '''ejecuta la tarea asignada'''
        logging.info(f"Ejecutando tarea {task_id}: {task_data}")

        # codigo para hacer scraping aqui

        result = None

        result_msg = {
            'type': 'task_result',
            'task_id': task_id,
            'result': result,
            'completed_at': datetime.now().isoformat()
        }

        try:
            self.socket.send(json.dumps(result_msg).encode())
            logging.info(f"Tarea {task_id}: completada: {result}")
        
        except Exception:
            self.connected = False
        
    def listen_for_tasks(self):
        '''escuchando tareas del servidor'''
        while self.connected:
            try:
                data = self.socket.recv(1024).decode()
                if not data:
                    self.connected = False
                    break
                
                message = json.loads(data)
                self.proccess_server_message(message)
            
            except json.JSONDecodeError:
                logging.error("Mensaje JSON invalido del servidor")
            except Exception as e:
                logging.error(f"Error recibiendo datos: {e}")
                self.connected = False
                break
        
    def proccess_server_message(self, message):
        '''procesa mensaje del servidor'''
        msg_type = message.get('type')

        if msg_type == 'task':
            # nueva tarea asignada
            task_id = message.get('task_id')
            task_data = message.get('task_data')

            logging.info(f"Nueva tarea recibida: {task_id}")

            # ejecuta la tarea en un hilo separado
            task_thread = threading.Thread(
                target=self.execute_task,
                args=(task_id, task_data)
            )
            task_thread.daemon = True
            task_thread.start()

            self.current_task[task_id] = {
                'data': task_data,
                'started_at': datetime.now().isoformat(),
                'status': 'executing'
            }

    def start(self):
        '''inicia al cliente'''
        if not self.connect_to_server():
            return
        
        # inicia hilos de seniales
        heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        heartbeat_thread.daemon = True
        heartbeat_thread.start()

        # envia el estado inicial
        self.send_status('Conectado y listo')

        self.listen_for_tasks()



if __name__ == "__main__":
    # el server se reconoce por nombre en docker
    client = ClientNode() #probando (server_host='mi-scrp-server')

    # intenta reconectar si se pierde la conexion
    while(True):
        client.start()
        if not client.connected:
            logging.info("Intentando reconectar en 10 seg ...")
            time.sleep(10)