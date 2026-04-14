#!/usr/bin/env python3
"""
Cliente Web para el sistema distribuido de scrapping

Servidor HTTP simple que sirve una interfaz web y maneja peticiones de scrapping.
Mantiene conexión persistente con el Router y se reconecta si se cae.

NO usa frameworks externos (Flask, Streamlit, etc), solo http.server de Python.
"""

import socket
import json
import sys
import os
import threading
import uuid
import time
import ssl
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse
import logging

# Agregar directorio padre al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from base_node.utils import MessageProtocol
from base_node.utils.node_connection import NodeConnection

# Configuración
ROUTER_HOST = os.environ.get('ROUTER_HOST', 'router')
ROUTER_PORT = int(os.environ.get('ROUTER_PORT', 7070))
WEB_PORT = int(os.environ.get('WEB_PORT', 8080))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class WebScrapperClient:
    """Cliente web con conexión persistente al Router"""
    
    def __init__(self, router_host=ROUTER_HOST, router_port=ROUTER_PORT):
        self.router_host = router_host
        self.router_port = router_port
        self.router_ip = None
        self.running = True
        
        self.router_ips = []
        
        # Conexión persistente con el router
        self.router_connection = None
        self.connection_lock = threading.Lock()
        
        # Cliente ID único
        self.client_id = f"web-client-{uuid.uuid4().hex[:8]}"
        
        # Tracking de peticiones
        self.pending_requests = {}  # {task_id: {'url': url, 'timestamp': datetime}}
        self.completed_requests = {}  # {task_id: {'result': result, 'timestamp': datetime}}
        self.pending_db_requests = {}  # {request_id: {'type': 'tables'|'data', 'timestamp': datetime}}
        self.completed_db_responses = {}  # {request_id: response_data}
        self.requests_lock = threading.Lock()
        
        # SSL context for encrypted connections
        self.ssl_context = None
        self._initialize_ssl_context()
        
        # Thread de reconexión
        self.reconnect_thread = None
        
    def _initialize_ssl_context(self):
        """Initialize SSL context for client connections"""
        try:
            self.ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            
            # Load CA certificate for server verification
            cafile = os.environ.get('SSL_CAFILE')
            if cafile and os.path.exists(cafile):
                self.ssl_context.load_verify_locations(cafile=cafile)
                self.ssl_context.check_hostname = False  # Disable for self-signed certs
                self.ssl_context.verify_mode = ssl.CERT_NONE  # Accept self-signed
                logging.info(f"SSL context initialized with CA file: {cafile}")
            else:
                # Fallback: no verification
                self.ssl_context.check_hostname = False
                self.ssl_context.verify_mode = ssl.CERT_NONE
                logging.warning("SSL context initialized without CA verification")
                
        except Exception as e:
            logging.error(f"Failed to initialize SSL context: {e}")
            self.ssl_context = None
    
    def resolve_router(self):
        """Resuelve IPs de routers y encuentra el jefe"""
        try:
            logging.info(f"Consultando DNS por routers...")
            
            addr_info = socket.getaddrinfo(
                self.router_host, 
                self.router_port, 
                socket.AF_INET, 
                socket.SOCK_STREAM
            )
            
            router_ips = list(set([info[4][0] for info in addr_info]))
            logging.info(f"{len(router_ips)} router(s) encontrado(s): {router_ips}")
            self.router_ips = router_ips
            
            # Buscar el jefe
            boss_ip = self._find_boss_router(router_ips)
            
            if boss_ip:
                self.router_ip = boss_ip
                logging.info(f"Router jefe encontrado: {boss_ip}:{self.router_port}")
                return True
            else:
                logging.error("No se encontró router jefe")
                return False
            
        except Exception as e:
            logging.error(f"Error resolviendo router: {e}")
            return False
    
    def _find_boss_router(self, router_ips):
        """Encuentra el router jefe"""
        logging.info(f"Buscando router jefe entre {len(router_ips)} candidatos...")
        
        for ip in router_ips:
            try:
                temp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                temp_socket.settimeout(3)
                temp_socket.connect((ip, self.router_port))
                
                # Wrap socket with SSL if context available
                if self.ssl_context:
                    temp_socket = self.ssl_context.wrap_socket(temp_socket, server_hostname=ip)
                    logging.debug(f"SSL wrapped temporary socket to {ip}")
                
                identification_msg = {
                    'type': MessageProtocol.MESSAGE_TYPES['IDENTIFICATION'],
                    'sender_id': 'client-discovery',
                    'timestamp': datetime.now().isoformat(),
                    'data': {
                        'is_temporary': True
                    }
                }
                
                msg_bytes = json.dumps(identification_msg).encode()
                temp_socket.send(len(msg_bytes).to_bytes(2, 'big'))
                temp_socket.send(msg_bytes)
                
                length_bytes = temp_socket.recv(2)
                if length_bytes:
                    length = int.from_bytes(length_bytes, 'big')
                    response_bytes = temp_socket.recv(length)
                    response = json.loads(response_bytes.decode())
                    
                    is_boss = response.get('data', {}).get('is_boss', False)
                    temp_socket.close()
                    
                    if is_boss:
                        logging.info(f"Router jefe confirmado en {ip}")
                        return ip
                
                temp_socket.close()
                    
            except Exception as e:
                logging.debug(f"Error consultando {ip}: {e}")
                try:
                    temp_socket.close()
                except:
                    pass
        
        return None
    
    def connect(self):
        """Establece conexión con el Router jefe"""
        with self.connection_lock:
            if self.router_connection and self.router_connection.is_connected():
                return True
            
            if not self.router_ip:
                if not self.resolve_router():
                    return False
            
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((self.router_ip, self.router_port))
                
                # Wrap socket with SSL if context available
                if self.ssl_context:
                    sock = self.ssl_context.wrap_socket(sock, server_hostname=self.router_ip)
                    logging.debug(f"SSL wrapped connection socket to {self.router_ip}")
                
                self.router_connection = NodeConnection(
                    node_type='router',
                    ip=self.router_ip,
                    port=self.router_port,
                    on_message_callback=self._handle_message,
                    sender_node_type='client',
                    sender_id=self.client_id,
                    ssl_context=self.ssl_context  # Pass SSL context
                )
                
                self.router_connection.connect(existing_socket=sock)
                
                # Enviar identificación
                self.router_connection.send_message({
                    'type': MessageProtocol.MESSAGE_TYPES['IDENTIFICATION'],
                    'sender_id': self.client_id,
                    'node_type': 'client',  # Importante: debe ser 'client' en la raíz
                    'timestamp': datetime.now().isoformat(),
                    'data': {
                        'client_type': 'web',
                        'is_temporary': False,
                        'is_boss': False,
                        'port': 0  # Cliente no acepta conexiones entrantes
                    }
                })
                
                logging.info(f"Conectado al Router jefe: {self.router_ip}")
                
                # Iniciar thread de monitoreo de conexión
                if not self.reconnect_thread or not self.reconnect_thread.is_alive():
                    self.reconnect_thread = threading.Thread(
                        target=self._monitor_connection,
                        daemon=True
                    )
                    self.reconnect_thread.start()
                
                return True
                
            except Exception as e:
                logging.error(f"Error conectando al router: {e}")
                self.router_connection = None
                return False
    
    def _monitor_connection(self):
        """Monitorea la conexión y reconecta si es necesario"""
        logging.info("👀 Thread de monitoreo de conexión iniciado")
        
        while self.running:
            time.sleep(5)  # Chequear cada 5 segundos (más frecuente)
            
            # Verificar estado de conexión SIN bloquear (lectura rápida)
            is_connected = False
            if self.router_connection:
                try:
                    is_connected = self.router_connection.is_connected()
                except:
                    is_connected = False
            
            if not is_connected:
                logging.warning("⚠️ Conexión con router perdida, intentando reconectar...")
                # Intentar reconectar (esto adquiere el lock internamente)
                success = self._attempt_reconnection()
                if success:
                    logging.info("✅ Reconexión exitosa, continuando monitoreo...")
                else:
                    logging.error("❌ Reconexión fallida, reintentando en 5s...")
        
        logging.info("👀 Thread de monitoreo de conexión finalizado")
    
    def _attempt_reconnection(self):
        """
        Intenta reconectar al router (potencialmente un nuevo jefe).
        Adquiere el lock internamente.
        """
        max_retries = 3  # Menos intentos por ciclo, pero el monitoreo seguirá intentando
        retry_interval = 3  # segundos
        
        with self.connection_lock:
            # Cerrar conexión anterior si existe
            if self.router_connection:
                try:
                    self.router_connection.disconnect()
                except:
                    pass
                self.router_connection = None
            
            # Resetear IP para forzar nuevo descubrimiento DNS
            self.router_ip = None
        
        for attempt in range(1, max_retries + 1):
            if not self.running:
                break
            
            logging.info(f"🔄 Intento de reconexión {attempt}/{max_retries}...")
            
            # Intentar resolver y conectar al nuevo jefe (sin lock para evitar deadlock)
            if not self.resolve_router():
                logging.error(f"No se pudo resolver router en intento {attempt}")
                if attempt < max_retries and self.running:
                    logging.info(f"⏳ Esperando {retry_interval}s antes del próximo intento...")
                    time.sleep(retry_interval)
                continue
            
            try:
                with self.connection_lock:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(5)  # Timeout para evitar bloqueos largos
                    sock.connect((self.router_ip, self.router_port))
                    
                    self.router_connection = NodeConnection(
                        node_type='router',
                        ip=self.router_ip,
                        port=self.router_port,
                        on_message_callback=self._handle_message,
                        sender_node_type='client',
                        sender_id=self.client_id
                    )
                    
                    self.router_connection.connect(existing_socket=sock)
                    
                    # Enviar identificación
                    self.router_connection.send_message({
                        'type': MessageProtocol.MESSAGE_TYPES['IDENTIFICATION'],
                        'sender_id': self.client_id,
                        'node_type': 'client',
                        'timestamp': datetime.now().isoformat(),
                        'data': {
                            'client_type': 'web',
                            'is_temporary': False,
                            'is_boss': False,
                            'port': 0
                        }
                    })
                    
                    logging.info(f"✅ Reconexión exitosa al Router jefe: {self.router_ip}")
                    return True
                    
            except Exception as e:
                logging.error(f"❌ Error en intento {attempt}: {e}")
                with self.connection_lock:
                    if self.router_connection:
                        try:
                            self.router_connection.disconnect()
                        except:
                            pass
                        self.router_connection = None
            
            # Esperar antes del próximo intento (excepto en el último)
            if attempt < max_retries and self.running:
                logging.info(f"⏳ Esperando {retry_interval}s antes del próximo intento...")
                time.sleep(retry_interval)
        
        logging.warning("⚠️ No se pudo reconectar en este ciclo, el monitoreo continuará intentando...")
        return False
    
    def _handle_message(self, connection, message):
        """Callback para mensajes del router"""
        msg_type = message.get('type')
        
        if msg_type == MessageProtocol.MESSAGE_TYPES['TASK_RESULT']:
            data = message.get('data', {})
            task_id = data.get('task_id')
            result_data = data.get('result', {})
            
            with self.requests_lock:
                if task_id in self.pending_requests:
                    self.completed_requests[task_id] = {
                        'result': result_data,
                        'timestamp': datetime.now().isoformat(),
                        'url': self.pending_requests[task_id]['url']
                    }
                    del self.pending_requests[task_id]
                    logging.info(f"✓ Resultado recibido para {task_id}")
                else:
                    logging.warning(f"Resultado recibido para tarea desconocida: {task_id}")
        
        elif msg_type == MessageProtocol.MESSAGE_TYPES['LIST_TABLES_RESPONSE']:
            data = message.get('data', {})
            success = data.get('success', False)
            tables = data.get('tables', [])
            
            logging.info(f"LIST_TABLES_RESPONSE recibido: success={success}, tables_count={len(tables)}")
            logging.info(f"DEBUG - Contenido de tables: {tables}")
            logging.info(f"DEBUG - Tipo de primer elemento: {type(tables[0]) if tables else 'lista vacía'}")
            
            with self.requests_lock:
                if 'tables' in self.pending_db_requests:
                    self.completed_db_responses['tables'] = data
                    del self.pending_db_requests['tables']
                    logging.info(f"✓ Lista de {len(tables)} tablas guardada")
                else:
                    logging.warning("Respuesta de tablas recibida sin petición pendiente")
        
        elif msg_type == MessageProtocol.MESSAGE_TYPES['GET_TABLE_DATA_RESPONSE']:
            data = message.get('data', {})
            request_id = data.get('request_id', '')
            table_name = data.get('table_name', 'unknown')
            
            with self.requests_lock:
                # Buscar por request_id si está disponible
                if request_id and request_id in self.pending_db_requests:
                    self.completed_db_responses[request_id] = data
                    del self.pending_db_requests[request_id]
                    logging.info(f"✓ Datos de tabla '{table_name}' recibidos (request_id={request_id})")
                else:
                    # Fallback: buscar por table_name (compatibilidad)
                    found = False
                    for req_id, req_info in list(self.pending_db_requests.items()):
                        if req_info.get('table_name') == table_name and req_info.get('type') == 'data':
                            self.completed_db_responses[req_id] = data
                            del self.pending_db_requests[req_id]
                            logging.info(f"✓ Datos de tabla '{table_name}' recibidos (fallback match: {req_id})")
                            found = True
                            break
                    
                    if not found:
                        logging.warning(f"Respuesta de tabla '{table_name}' recibida sin petición pendiente")
        
        elif msg_type == MessageProtocol.MESSAGE_TYPES['EXPORT_ALL_DATA_RESPONSE']:
            data = message.get('data', {})
            request_id = data.get('request_id', '')
            success = data.get('success', False)
            
            logging.info(f"EXPORT_ALL_DATA_RESPONSE recibido: success={success}, request_id={request_id}")
            
            with self.requests_lock:
                if request_id and request_id in self.pending_db_requests:
                    self.completed_db_responses[request_id] = data
                    del self.pending_db_requests[request_id]
                    urls_count = len(data.get('data', {}).get('urls', []))
                    logging.info(f"✓ Exportación completa recibida ({urls_count} URLs, request_id={request_id})")
                else:
                    logging.warning(f"Respuesta de exportación recibida sin petición pendiente (request_id={request_id})")

    
    def request_scraping(self, url):
        """Envía petición de scrapping"""
        if not self.router_connection or not self.router_connection.is_connected():
            if not self.connect():
                return None, "No hay conexión con el router"
        
        task_id = f"task-{uuid.uuid4().hex[:8]}"
        
        message = {
            'type': MessageProtocol.MESSAGE_TYPES['CLIENT_REQUEST'],
            'sender_id': self.client_id,
            'timestamp': datetime.now().isoformat(),
            'data': {
                'task_id': task_id,
                'url': url
            }
        }
        
        with self.requests_lock:
            self.pending_requests[task_id] = {
                'url': url,
                'timestamp': datetime.now()
            }
        
        success = self.router_connection.send_message(message)
        
        if success:
            logging.info(f"Petición enviada: {task_id} para {url}")
            return task_id, None
        else:
            with self.requests_lock:
                del self.pending_requests[task_id]
            return None, "Error enviando petición"
    
    def get_result(self, task_id):
        """Obtiene resultado de una tarea"""
        with self.requests_lock:
            return self.completed_requests.get(task_id)
    
    def get_status(self):
        """Obtiene estado del cliente"""
        with self.connection_lock:
            connected = self.router_connection and self.router_connection.is_connected()
        
        with self.requests_lock:
            pending_count = len(self.pending_requests)
            completed_count = len(self.completed_requests)
        
        return {
            'connected': connected,
            'router_ip': self.router_ip,
            'pending_requests': pending_count,
            'completed_requests': completed_count
        }
    
    def request_list_tables(self):
        """Solicita lista de tablas disponibles en BD"""
        if not self.router_connection or not self.router_connection.is_connected():
            if not self.connect():
                return None, "No hay conexión con el router"
        
        message = {
            'type': MessageProtocol.MESSAGE_TYPES['LIST_TABLES'],
            'sender_id': self.client_id,
            'timestamp': datetime.now().isoformat(),
            'data': {}
        }
        
        with self.requests_lock:
            # Marcar que hay una petición de tablas pendiente
            self.pending_db_requests['tables'] = {
                'type': 'tables',
                'timestamp': datetime.now()
            }
        
        success = self.router_connection.send_message(message)
        
        if success:
            logging.info("Petición de lista de tablas enviada")
            return 'tables', None
        else:
            with self.requests_lock:
                if 'tables' in self.pending_db_requests:
                    del self.pending_db_requests['tables']
            return None, "Error enviando petición"
    
    def request_table_data(self, table_name, page=1, page_size=50):
        """Solicita datos paginados de una tabla"""
        if not self.router_connection or not self.router_connection.is_connected():
            if not self.connect():
                return None, "No hay conexión con el router"
        
        # Usar ID único para cada petición para evitar usar caché
        import uuid
        request_id = f"table_{table_name}_{uuid.uuid4().hex[:8]}"
        
        message = {
            'type': MessageProtocol.MESSAGE_TYPES['GET_TABLE_DATA'],
            'sender_id': self.client_id,
            'timestamp': datetime.now().isoformat(),
            'data': {
                'request_id': request_id,  # Incluir request_id en el mensaje
                'table_name': table_name,
                'page': page,
                'page_size': page_size
            }
        }
        
        with self.requests_lock:
            self.pending_db_requests[request_id] = {
                'type': 'data',
                'table_name': table_name,
                'page': page,
                'timestamp': datetime.now()
            }
        
        success = self.router_connection.send_message(message)
        
        if success:
            logging.info(f"Petición de datos de tabla '{table_name}' enviada (página {page}, request_id={request_id})")
            return request_id, None
        else:
            with self.requests_lock:
                if request_id in self.pending_db_requests:
                    del self.pending_db_requests[request_id]
            return None, "Error enviando petición"
    
    def request_export_all_data(self):
        """Solicita exportación de todos los datos almacenados en BD"""
        if not self.router_connection or not self.router_connection.is_connected():
            if not self.connect():
                return None, "No hay conexión con el router"
        
        # Usar ID único para esta petición
        import uuid
        request_id = f"export_{uuid.uuid4().hex[:8]}"
        
        message = {
            'type': MessageProtocol.MESSAGE_TYPES['EXPORT_ALL_DATA'],
            'sender_id': self.client_id,
            'timestamp': datetime.now().isoformat(),
            'data': {
                'request_id': request_id
            }
        }
        
        with self.requests_lock:
            self.pending_db_requests[request_id] = {
                'type': 'export',
                'timestamp': datetime.now()
            }
        
        success = self.router_connection.send_message(message)
        
        if success:
            logging.info(f"Petición de exportación de datos enviada (request_id={request_id})")
            return request_id, None
        else:
            with self.requests_lock:
                if request_id in self.pending_db_requests:
                    del self.pending_db_requests[request_id]
            return None, "Error enviando petición"
    
    def get_db_response(self, request_id):
        """Obtiene respuesta de una petición de BD y la elimina del caché"""
        with self.requests_lock:
            response = self.completed_db_responses.get(request_id)
            if response:
                # Eliminar la respuesta del caché para evitar reutilizarla
                del self.completed_db_responses[request_id]
            return response
    
    def stop(self):
        """Detiene el cliente"""
        self.running = False
        if self.router_connection:
            self.router_connection.disconnect()


# Instancia global del cliente
client = WebScrapperClient()


class WebHandler(BaseHTTPRequestHandler):
    """Handler para peticiones HTTP"""
    
    def log_message(self, format, *args):
        """Sobrescribir para usar logging"""
        logging.info("%s - %s" % (self.address_string(), format % args))
    
    def do_GET(self):
        """Maneja peticiones GET"""
        parsed_path = urlparse(self.path)
        
        if parsed_path.path == '/':
            # Servir página principal
            self.serve_file('index.html', 'text/html')
        elif parsed_path.path == '/style.css':
            self.serve_file('style.css', 'text/css')
        elif parsed_path.path == '/script.js':
            self.serve_file('script.js', 'application/javascript')
        elif parsed_path.path == '/api/status':
            # API: Estado del cliente
            self.send_json_response(client.get_status())
        elif parsed_path.path.startswith('/api/result/'):
            # API: Obtener resultado
            task_id = parsed_path.path.split('/')[-1]
            result = client.get_result(task_id)
            if result:
                self.send_json_response({'success': True, 'result': result})
            else:
                self.send_json_response({'success': False, 'message': 'Resultado no encontrado'})
        elif parsed_path.path == '/api/tables':
            # API: Solicitar lista de tablas
            request_id, error = client.request_list_tables()
            if request_id:
                # Esperar respuesta (con timeout)
                import time
                timeout = 5  # 5 segundos
                start = time.time()
                while time.time() - start < timeout:
                    response = client.get_db_response(request_id)
                    if response:
                        logging.info(f"Respuesta BD recibida para list_tables: success={response.get('success')}, tables_count={len(response.get('tables', []))}")
                        self.send_json_response(response)
                        return
                    time.sleep(0.1)
                logging.warning("Timeout esperando respuesta de lista de tablas")
                self.send_json_response({'success': False, 'message': 'Timeout esperando respuesta'})
            else:
                self.send_json_response({'success': False, 'message': error}, 500)
        elif parsed_path.path == '/api/export':
            # API: Exportar todos los datos
            request_id, error = client.request_export_all_data()
            if request_id:
                # Esperar respuesta (con timeout extendido ya que puede ser mucha data)
                import time
                timeout = 30  # 30 segundos para exportación completa
                start = time.time()
                while time.time() - start < timeout:
                    response = client.get_db_response(request_id)
                    if response:
                        logging.info(f"Respuesta BD recibida para export: success={response.get('success')}, urls_count={len(response.get('data', {}).get('urls', []))}")
                        self.send_json_response(response)
                        return
                    time.sleep(0.1)
                logging.warning("Timeout esperando respuesta de exportación")
                self.send_json_response({'success': False, 'message': 'Timeout esperando respuesta'})
            else:
                self.send_json_response({'success': False, 'message': error}, 500)
        elif parsed_path.path.startswith('/api/table/'):
            # API: Obtener datos de tabla específica
            # Formato: /api/table/<nombre>?page=1&page_size=50
            path_parts = parsed_path.path.split('/')
            if len(path_parts) >= 4:
                table_name = path_parts[3]
                query_params = parse_qs(parsed_path.query)
                page = int(query_params.get('page', ['1'])[0])
                page_size = int(query_params.get('page_size', ['50'])[0])
                
                request_id, error = client.request_table_data(table_name, page, page_size)
                if request_id:
                    # Esperar respuesta (con timeout aumentado)
                    import time
                    timeout = 10  # Aumentado de 5 a 10 segundos
                    start = time.time()
                    while time.time() - start < timeout:
                        response = client.get_db_response(request_id)
                        if response:
                            logging.info(f"✓ Respuesta de tabla '{table_name}' recibida en {time.time() - start:.2f}s")
                            self.send_json_response(response)
                            return
                        time.sleep(0.1)
                    logging.warning(f"Timeout esperando datos de tabla '{table_name}' (>{timeout}s)")
                    self.send_json_response({'success': False, 'message': f'Timeout esperando respuesta del servidor (>{timeout}s). Intenta de nuevo.', 'error': 'timeout'})
                else:
                    self.send_json_response({'success': False, 'message': error}, 500)
            else:
                self.send_json_response({'success': False, 'message': 'Tabla no especificada'}, 400)
        else:
            self.send_error(404)
    
    def do_POST(self):
        """Maneja peticiones POST"""
        parsed_path = urlparse(self.path)
        
        if parsed_path.path == '/api/scrape':
            # API: Nueva petición de scrapping
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode())
            
            url = data.get('url')
            if not url:
                self.send_json_response({'success': False, 'message': 'URL requerida'}, 400)
                return
            
            task_id, error = client.request_scraping(url)
            if task_id:
                self.send_json_response({'success': True, 'task_id': task_id})
            else:
                self.send_json_response({'success': False, 'message': error}, 500)
        else:
            self.send_error(404)
    
    def serve_file(self, filename, content_type):
        """Sirve un archivo estático"""
        try:
            filepath = os.path.join(os.path.dirname(__file__), filename)
            with open(filepath, 'rb') as f:
                content = f.read()
            
            self.send_response(200)
            self.send_header('Content-type', content_type)
            self.end_headers()
            self.wfile.write(content)
        except FileNotFoundError:
            self.send_error(404)
    
    def send_json_response(self, data, status=200):
        """Envía respuesta JSON"""
        self.send_response(status)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        
        # Encoder personalizado para manejar datetime
        class DateTimeEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                return super().default(obj)
        
        self.wfile.write(json.dumps(data, cls=DateTimeEncoder).encode())


def main():
    """Función principal"""
    logging.info("=== Cliente Web de Scrapping ===")
    
    # Conectar al router
    logging.info("Conectando al router...")
    if not client.connect():
        logging.error("No se pudo conectar al router, pero el servidor web iniciará de todos modos")
    
    # Iniciar servidor HTTP
    server = HTTPServer(('0.0.0.0', WEB_PORT), WebHandler)
    logging.info(f"Servidor web escuchando en http://0.0.0.0:{WEB_PORT}")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logging.info("Deteniendo servidor...")
        client.stop()
        server.shutdown()


if __name__ == '__main__':
    main()
