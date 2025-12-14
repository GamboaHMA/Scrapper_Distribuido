import socket
import json
import threading
import queue
import logging
from datetime import datetime


class NodeConnection:
    """
    Representa una conexión bidireccional con un nodo específico.
    Maneja tanto el envío como la recepción de mensajes de forma asíncrona.
    """
    
    def __init__(self, node_type, ip, port, on_message_callback=None):
        """
        Inicializa una conexión con un nodo.
        
        Args:
            node_type (str): Tipo de nodo ('scrapper', 'bd', 'router')
            ip (str): Dirección IP del nodo
            port (int): Puerto del nodo
            on_message_callback (callable, optional): Función que se llama al recibir un mensaje.
                                                      Debe aceptar (node_connection, message_dict)
        """
        self.node_type = node_type
        self.ip = ip
        self.port = port
        self.node_id = f"{node_type}-{ip}:{port}"
        
        # Socket y estado de conexión
        self.socket = None
        self.connected = False
        self.connection_lock = threading.Lock()
        
        # Envío de mensajes
        self.send_queue = queue.Queue()
        self.send_thread = None
        self.send_stop_event = threading.Event()
        
        # Recepción de mensajes
        self.receive_queue = queue.Queue()  # Cola para mensajes recibidos
        self.receive_thread = None
        self.receive_stop_event = threading.Event()
        self.on_message_callback = on_message_callback
        
        # Estado del nodo
        self.last_heartbeat = None
        self.is_busy = False
        self.estado = "desconectado"  # "desconectado", "conectado", "ocupado"
        self.metadata = {}  # Diccionario para datos adicionales
        
        logging.debug(f"NodeConnection creada para {self.node_id}")
    
    def connect(self, existing_socket=None):
        """
        Establece conexión con el nodo.
        
        Args:
            existing_socket (socket.socket, optional): Socket ya conectado para usar.
                                                       Si es None, crea uno nuevo.
        
        Returns:
            bool: True si la conexión fue exitosa, False en caso contrario.
        """
        with self.connection_lock:
            if self.connected:
                logging.warning(f"{self.node_id} ya está conectado")
                return True
            
            try:
                if existing_socket:
                    self.socket = existing_socket
                else:
                    self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.socket.settimeout(5)
                    self.socket.connect((self.ip, self.port))
                
                self.connected = True
                self.estado = "conectado"
                
                # Iniciar hilos de envío y recepción
                self._start_send_thread()
                self._start_receive_thread()
                
                logging.debug(f"Conectado exitosamente a {self.node_id}")
                return True
                
            except socket.timeout:
                logging.warning(f"Timeout al conectar con {self.node_id}")
                self.connected = False
                return False
            except Exception as e:
                logging.error(f"Error conectando a {self.node_id}: {e}")
                self.connected = False
                return False
    
    def _start_send_thread(self):
        """Inicia el hilo de envío de mensajes"""
        self.send_stop_event.clear()
        self.send_thread = threading.Thread(
            target=self._send_worker,
            name=f"Send-{self.node_id}",
            daemon=True
        )
        self.send_thread.start()
    
    def _start_receive_thread(self):
        """Inicia el hilo de recepción de mensajes"""
        self.receive_stop_event.clear()
        self.receive_thread = threading.Thread(
            target=self._receive_worker,
            name=f"Receive-{self.node_id}",
            daemon=True
        )
        self.receive_thread.start()
    
    def _send_worker(self):
        """Hilo que envía mensajes desde la cola de envío"""
        while self.connected and not self.send_stop_event.is_set():
            try:
                # Esperar mensaje con timeout para poder verificar stop_event
                message_bytes = self.send_queue.get(timeout=1.0)
                
                if message_bytes is None:  # Señal de parada
                    break
                
                try:
                    # Enviar longitud del mensaje (2 bytes)
                    length = len(message_bytes)
                    self.socket.send(length.to_bytes(2, 'big'))
                    
                    # Enviar mensaje completo
                    self.socket.send(message_bytes)
                    
                    self.send_queue.task_done()
                    
                except socket.error as e:
                    logging.error(f"Error de socket enviando a {self.node_id}: {e}")
                    
                    # Reencolar mensaje si no se envió
                    if not self.send_stop_event.is_set():
                        self.send_queue.put(message_bytes)
                    
                    self.connected = False
                    break
                    
            except queue.Empty:
                # Timeout, verificar si debemos continuar
                continue
                
            except Exception as e:
                logging.error(f"Error en hilo de envío para {self.node_id}: {e}")
                break
        
        logging.debug(f"Hilo de envío terminado para {self.node_id}")
    
    def _receive_worker(self):
        """Hilo que recibe mensajes del socket"""
        while self.connected and not self.receive_stop_event.is_set():
            try:
                # Configurar timeout para permitir chequeo periódico de stop_event
                self.socket.settimeout(1.0)
                
                # Recibir longitud del mensaje (2 bytes)
                length_bytes = self.socket.recv(2)
                
                if not length_bytes:
                    logging.debug(f"Conexión cerrada por {self.node_id}")
                    self.connected = False
                    break
                
                message_length = int.from_bytes(length_bytes, 'big')
                
                # Recibir mensaje completo
                message_bytes = b''
                while len(message_bytes) < message_length:
                    chunk = self.socket.recv(message_length - len(message_bytes))
                    if not chunk:
                        logging.debug(f"Conexión cerrada por {self.node_id} durante recepción")
                        self.connected = False
                        break
                    message_bytes += chunk
                
                if not self.connected:
                    break
                
                # Decodificar mensaje
                message_dict = json.loads(message_bytes.decode())
                
                # Actualizar último heartbeat si es un heartbeat
                msg_type = message_dict.get('type')
                if msg_type == 'heartbeat':
                    self.last_heartbeat = datetime.now()
                    logging.debug(f"Heartbeat recibido de {self.node_id}")
                else:
                    logging.info(f"Mensaje recibido de {self.node_id}: {msg_type}")
                
                # Llamar callback si está definido
                if self.on_message_callback:
                    try:
                        self.on_message_callback(self, message_dict)
                    except Exception as e:
                        logging.error(f"Error en callback de mensaje para {self.node_id}: {e}")
                
                # Agregar a cola de recepción para procesamiento posterior
                self.receive_queue.put(message_dict)
                
            except socket.timeout:
                # Timeout normal, continuar esperando
                continue
                
            except json.JSONDecodeError as e:
                logging.error(f"Error decodificando JSON de {self.node_id}: {e}")
                
            except Exception as e:
                logging.error(f"Error en hilo de recepción para {self.node_id}: {e}")
                self.connected = False
                break
        
        logging.debug(f"Hilo de recepción terminado para {self.node_id}")
    
    def send_message(self, message_dict):
        """
        Encola un mensaje para ser enviado al nodo.
        
        Args:
            message_dict (dict): Diccionario con el mensaje a enviar
            
        Returns:
            bool: True si el mensaje fue encolado, False si no hay conexión
        """
        if not self.connected:
            logging.error(f"No se puede enviar mensaje a {self.node_id}: no conectado")
            return False
        
        try:
            message_bytes = json.dumps(message_dict).encode()
            self.send_queue.put(message_bytes)
            return True
            
        except Exception as e:
            logging.error(f"Error al encolar mensaje para {self.node_id}: {e}")
            return False
    
    def get_received_message(self, timeout=None):
        """
        Obtiene el siguiente mensaje recibido de la cola.
        
        Args:
            timeout (float, optional): Tiempo máximo de espera en segundos.
                                      None = no bloquear, esperar indefinidamente.
        
        Returns:
            dict: Mensaje recibido, o None si no hay mensajes (con timeout)
        """
        try:
            if timeout is None:
                return self.receive_queue.get_nowait()
            else:
                return self.receive_queue.get(timeout=timeout)
        except queue.Empty:
            return None
    
    def send_heartbeat(self, additional_data=None):
        """
        Envía un mensaje de heartbeat al nodo.
        
        Args:
            additional_data (dict, optional): Datos adicionales para incluir en el heartbeat
        """
        heartbeat_msg = {
            'type': 'heartbeat',
            'timestamp': datetime.now().isoformat()
        }
        
        if additional_data:
            heartbeat_msg.update(additional_data)
        
        return self.send_message(heartbeat_msg)
    
    def disconnect(self):
        """Cierra la conexión con el nodo"""
        with self.connection_lock:
            if not self.connected:
                return
            
            logging.info(f"Desconectando de {self.node_id}")
            
            # Detener hilos
            self.send_stop_event.set()
            self.receive_stop_event.set()
            
            # Señal de parada para hilo de envío
            try:
                self.send_queue.put(None)
            except:
                pass
            
            # Cerrar socket
            if self.socket:
                try:
                    self.socket.close()
                except:
                    pass
            
            self.connected = False
            self.estado = "desconectado"
            
            logging.info(f"Desconectado de {self.node_id}")
    
    def is_connected(self):
        """Retorna si la conexión está activa"""
        return self.connected
    
    def update_heartbeat(self):
        """Actualiza el timestamp del último heartbeat"""
        self.last_heartbeat = datetime.now()
    
    def get_time_since_last_heartbeat(self):
        """
        Retorna el tiempo en segundos desde el último heartbeat.
        Retorna None si nunca se ha recibido un heartbeat.
        """
        if self.last_heartbeat is None:
            return None
        return (datetime.now() - self.last_heartbeat).total_seconds()
    
    def __repr__(self):
        status = "conectado" if self.connected else "desconectado"
        return f"<NodeConnection {self.node_id} [{status}]>"
    
    def __str__(self):
        return self.node_id
