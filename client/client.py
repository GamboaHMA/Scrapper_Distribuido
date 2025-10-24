import socket
import json
import time
import threading
import random
import logging
import os
from datetime import datetime
import struct
import queue

# Importar funciones de scrapping desde el módulo scrapper
from scrapper import get_html_from_url


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
        # cola para mensajes de salida y evento de parada   
        self.message_queue = queue.Queue()
        self.stop_event = threading.Event()
        # hilo de envio 
        self.send_thread = None
        # Estado del cliente (disponible/ocupado)
        self.is_busy = False
        self.status_lock = threading.Lock()

    def connect_to_server(self):
        '''conecta al servidor central'''
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.server_host, self.server_port))
            self.connected = True
            logging.info(f"Conectado al servidor {self.server_host}:{self.server_port}")

            # inicia hilo de envio
            self.stop_event.clear()
            self.send_thread = threading.Thread(target=self._send_worker)
            self.send_thread.daemon = True
            self.send_thread.start()

            # recibe mensaje de welcome de server
            welcome_data = self.socket.recv(1024).decode()
            welcome_msg = json.loads(welcome_data)
            self.client_id = welcome_msg.get('client_id')
            logging.info(f"ID asignado: {self.client_id}")

            return True

        except Exception as e:
            logging.error(f"Error conectando aal servidor: {e}")
            return False
        
    def _send_worker(self):
        '''hilo que envia mensajes desde la cola'''
        while self.connected and not self.stop_event.is_set():
            try:
                # esperar mensaje con timeout para poder verificar stop_event
                message = self.message_queue.get(timeout=1.0)

                if message is None:
                    break

                try:
                    # enviar longitud del mensaje(por ahora 2 bytes)
                    lenght = len(message)
                    self.socket.send(lenght.to_bytes(2, 'big'))

                    # enviar mensaje completo
                    self.socket.send(message)
                
                except Exception as e:
                    logging.error(f"Error enviando mensaje: {e}")
                    self.connected = False

                    if not self.stop_event.is_set():
                        self.message_queue.put(message)
                    break
                    
                self.message_queue.task_done()
            
            except queue.Empty:
                # timeout, verificar si debemos continuar
                continue
            
            except Exception as e:
                logging.error(f"Error en hilo de envio: {e}")
        
    def _enqueue_message(self, message_dict):
        '''encola un mensaje para eviar'''
        if not self.connected:
            logging.warning("Cliente no conectado, no se puede enviar mensaje")
            return False
        
        try:
            message_bytes = json.dumps(message_dict).encode()
            self.message_queue.put(message_bytes)
            return True
        
        except Exception as e:
            logging.error(f"Error encolando mensaje: {e}")
            return False
    
    
    def send_heartbeat(self):
        '''envia senial periodica al server'''
        while(self.connected):
            try:
                heartbeat_msg = {
                    'type': 'heartbeat',
                    'client_id': self.client_id,
                    'time_now': datetime.now().isoformat()
                }

                self._enqueue_message(heartbeat_msg)

                time.sleep(60) # latido cada 5 segundos
            
            except Exception:
                self.connected = False

    def update_busy_status(self, is_busy):
        '''Actualiza el estado de ocupación y notifica al servidor'''
        with self.status_lock:
            self.is_busy = is_busy
            status = 'busy' if is_busy else 'available'
            self.send_status(status)
            logging.info(f"Estado actualizado: {status}")
    
    def send_status(self, status):
        '''envia estado al servidor'''
        try:
            status_msg = {
                'type': 'status',
                'client_id': self.client_id,
                'status': status,
                'is_busy': self.is_busy,
                'time_now': datetime.now().isoformat()
            }

            return self._enqueue_message(status_msg)
        
        except Exception:
            self.connected = False
    
    def execute_task(self, task_id, task_data):
        '''ejecuta la tarea asignada'''
        logging.info(f"Ejecutando tarea {task_id}: {task_data}")
        
        # Marcar al cliente como ocupado
        self.update_busy_status(True)
        
        result = None
        try:
            # Verificar que task_data tiene formato adecuado
            if not isinstance(task_data, dict) or 'url' not in task_data: #la task tiene q tener la url a scrapear
                raise Exception("Formato de tarea inválido, se espera diccionario con campo 'url'")
                
            url = task_data['url']
            logging.info(f"Iniciando scraping para URL: {url}")
            
            # Realizar el scraping
            scrape_result = get_html_from_url(url)
            
            # Preparar resultado para enviar al servidor
            # NOTE: CAMBIAR SI SE DESEA OTRA INFO
            result = {
                'url': scrape_result['url'],
                'html_length': len(scrape_result['html']), #longitud del html
                'links_count': len(scrape_result['links']), #cant de links encontrados
                'links': scrape_result['links'][:10],  # solo primeros 10 enlaces
                'status': 'success'
            }
            
            logging.info(f"Scraping exitoso: {url}, encontrados {len(scrape_result['links'])} enlaces")
            
        except Exception as e:
            error_msg = str(e)
            logging.error(f"Error en el scraping: {error_msg}")
            
            # En caso de error, enviar información de error
            result = {
                'status': 'error',
                'error': error_msg
            }

        # Preparar mensaje de resultado para el servidor
        result_msg = {
            'type': 'task_result',
            'client_id': self.client_id,
            'task_id': task_id,
            'result': result,
            'completed_at': datetime.now().isoformat()
        }

        if self._enqueue_message(result_msg):
            logging.info(f"Tarea {task_id} completada y enviada al servidor")
        else:
            logging.error(f"Error al encolar resultados de tarea {task_id}")
            self.connected = False
            
        # Marcar al cliente como disponible nuevamente
        self.update_busy_status(False)
        
        
    def listen_for_tasks(self):
        '''escuchando tareas del servidor'''
        while self.connected and not self.stop_event.is_set():
            try:
                # como las tareas se sabe que son menores que 1024 pues nos podemos saltar el protocolo
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
            
            # Verificar si estoy ocupado
            if self.is_busy:
                logging.warning(f"Rechazando tarea {task_id} porque el cliente está ocupado")
                # Informar al servidor que no podemos aceptar esta tarea
                rejection_msg = {
                    'type': 'task_rejected',
                    'client_id': self.client_id,
                    'task_id': task_id,
                    'reason': 'client_busy'
                }
                self._enqueue_message(rejection_msg)
                return
                
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
            
            # Confirmacion de aceptacion de tarea
            acceptance_msg = {
                'type': 'task_accepted',
                'client_id': self.client_id,
                'task_id': task_id,
                'time_now': datetime.now().isoformat()
            }
            self._enqueue_message(acceptance_msg)

    def disconnect(self):
        '''desconecta'''
        self.connected = False
        self.stop_event.set()

        try:
            self.message_queue.join()
        
        except:
            pass

        if self.socket:
            try:
                self.socket.close()
            except:
                pass


    def start(self):
        '''inicia al cliente'''
        if not self.connect_to_server():
            return
        
        self.connected = True
        
        # Inicializar como disponible
        with self.status_lock:
            self.is_busy = False

        # inicia hilos de señales
        heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        heartbeat_thread.daemon = True
        heartbeat_thread.start()

        # envia el estado inicial
        self.send_status('available')

        self.listen_for_tasks()



if __name__ == "__main__":
    # Obtener la configuración del servidor desde variables de entorno
    server_host = os.environ.get('SERVER_HOST', '0.0.0.0')
    server_port = int(os.environ.get('SERVER_PORT', 8080))
    
    logging.info(f"Intentando conectar al servidor {server_host}:{server_port}")
    
    # Crear el cliente con la configuración del servidor
    client = ClientNode(server_host=server_host, server_port=server_port)

    # Intenta reconectar si se pierde la conexion
    while(True):
        try:
            client.start()
            if not client.connected:
                logging.info("Intentando reconectar en 10 seg ...")
                time.sleep(10)
            
        except KeyboardInterrupt:
            logging.info("Cliente terminado por el usuario")
            client.disconnect()
            break
        except Exception as e:
            logging.error(f"Error inesperado {e}")
            client.disconnect()
            logging.info("Intentando reconectar en 10 segundos ...")
            time.sleep(10)