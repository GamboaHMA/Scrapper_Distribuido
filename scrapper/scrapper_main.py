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

class ScrapperNode():
    def __init__(self, coordinator_host=None, coordinator_port=8080, broadcast_port=8081) -> None:
        self.coordinator_host = coordinator_host
        self.coordinator_port = coordinator_port
        self.broadcast_port = broadcast_port
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
        # Coordinatores detectados
        self.discovered_servers = set()
        # Flag para auto-descubrimiento
        self.auto_discovery = coordinator_host is None

    def listen_for_broadcasts(self):
        '''Escucha señales de broadcast de los coordinatores'''
        try:
            # Crear socket para recibir broadcast
            broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            # Permitir reutilización de direcciones
            broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # Enlazar a puerto de broadcast
            broadcast_socket.bind(('', self.broadcast_port))
            
            logging.info(f"Escuchando señales de broadcast en puerto {self.broadcast_port}")
            
            while not self.connected and not self.stop_event.is_set():
                try:
                    # Configurar timeout para comprobar periódicamente si debemos detener
                    broadcast_socket.settimeout(1.0)
                    data, addr = broadcast_socket.recvfrom(1024)
                    
                    if data:
                        try:
                            # Decodificar mensaje
                            message = json.loads(data.decode())
                            if message.get('type') == 'coordinator_discovery':
                                coordinator_host = message.get('coordinator_host')
                                coordinator_port = message.get('coordinator_port')
                                
                                # Si la dirección del broadcast es 0.0.0.0, usar la dirección del remitente
                                if coordinator_host == '0.0.0.0':
                                    coordinator_host = addr[0]
                                
                                server_info = (coordinator_host, coordinator_port)
                                self.discovered_servers.add(server_info)
                                logging.info(f"Coordinator descubierto: {coordinator_host}:{coordinator_port}")
                                
                                # Conectar al primer coordinator descubierto
                                if not self.connected:
                                    self.coordinator_host = coordinator_host
                                    self.coordinator_port = coordinator_port
                                    if self.connect_to_server():
                                        break
                        
                        except json.JSONDecodeError:
                            continue
                        except Exception as e:
                            logging.error(f"Error procesando broadcast: {e}")
                            
                except socket.timeout:
                    continue
                except Exception as e:
                    logging.error(f"Error en escucha de broadcast: {e}")
                    time.sleep(1)
            
        except Exception as e:
            logging.error(f"Error inicializando escucha de broadcast: {e}")
        finally:
            try:
                broadcast_socket.close()
            except:
                pass
    
    def connect_to_server(self):
        '''conecta al coordinator central'''
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.coordinator_host, self.coordinator_port))
            self.connected = True
            logging.info(f"Conectado al coordinator {self.coordinator_host}:{self.coordinator_port}")

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
            logging.error(f"Error conectando al coordinator: {e}")
            self.connected = False
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
        '''Actualiza el estado de ocupación y notifica al coordinator'''
        with self.status_lock:
            self.is_busy = is_busy
            status = 'busy' if is_busy else 'available'
            self.send_status(status)
            logging.info(f"Estado actualizado: {status}")
    
    def send_status(self, status):
        '''envia estado al coordinator'''
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
            
            # Preparar resultado para enviar al coordinator
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

        # Preparar mensaje de resultado para el coordinator
        result_msg = {
            'type': 'task_result',
            'client_id': self.client_id,
            'task_id': task_id,
            'result': result,
            'completed_at': datetime.now().isoformat()
        }

        if self._enqueue_message(result_msg):
            logging.info(f"Tarea {task_id} completada y enviada al coordinator")
        else:
            logging.error(f"Error al encolar resultados de tarea {task_id}")
            self.connected = False
            
        # Marcar al cliente como disponible nuevamente
        self.update_busy_status(False)
        
        
    def listen_for_tasks(self):
        '''escuchando tareas del coordinator'''
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
                logging.error("Mensaje JSON invalido del coordinator")
            except Exception as e:
                logging.error(f"Error recibiendo datos: {e}")
                self.connected = False
                break
        
    def proccess_server_message(self, message):
        '''procesa mensaje del coordinator'''
        msg_type = message.get('type')

        if msg_type == 'task':
            # nueva tarea asignada
            task_id = message.get('task_id')
            task_data = message.get('task_data')

            logging.info(f"Nueva tarea recibida: {task_id}")
            
            # Verificar si estoy ocupado
            if self.is_busy:
                logging.warning(f"Rechazando tarea {task_id} porque el cliente está ocupado")
                # Informar al coordinator que no podemos aceptar esta tarea
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
        # Si estamos en modo autodescubrimiento, intentar descubrir coordinatores
        if self.auto_discovery:
            logging.info("Modo autodescubrimiento activado. Buscando coordinatores...")
            # Iniciar hilo de escucha de broadcast en modo no bloqueante
            discovery_thread = threading.Thread(target=self.listen_for_broadcasts)
            discovery_thread.daemon = True
            discovery_thread.start()
            
            # Esperar a que se detecte un coordinator y se conecte
            max_wait = 30  # Esperar hasta 30 segundos
            wait_time = 0
            while not self.connected and wait_time < max_wait:
                time.sleep(1)
                wait_time += 1
                
            if not self.connected:
                logging.error("No se encontró ningún coordinator después de esperar")
                return
        else:
            # Modo directo, intentar conectar al coordinator configurado
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
    # Obtener la configuración del coordinator desde variables de entorno
    coordinator_host = os.environ.get('COORDINATOR_HOST', None)
    coordinator_port = int(os.environ.get('COORDINATOR_PORT', 8080))
    broadcast_port = int(os.environ.get('BROADCAST_PORT', 8081))
    auto_discovery = os.environ.get('AUTO_DISCOVERY', 'true').lower() == 'true'
    
    # Si auto_discovery está activado y no se especificó un COORDINATOR_HOST, usar None para activar autodescubrimiento
    if auto_discovery and not coordinator_host:
        logging.info("Modo autodescubrimiento activado, buscando coordinatores automáticamente")
        coordinator_host = None
    elif coordinator_host:
        logging.info(f"Modo directo, intentando conectar al coordinator {coordinator_host}:{coordinator_port}")
    
    # Crear el scrapper con la configuración del coordinator
    client = ScrapperNode(coordinator_host=coordinator_host, coordinator_port=coordinator_port, broadcast_port=broadcast_port)

    # Intenta reconectar si se pierde la conexion
    while True:
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