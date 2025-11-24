#!/usr/bin/env python3
"""
API REST simple para el servidor de scrapping distribuido
Puerto 8082 para evitar conflictos con servidor principal (8080) y broadcast (8081)
"""

from flask import Flask, request, jsonify, send_from_directory
import requests
import socket
import json
import threading
import time
import os
from datetime import datetime
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

app = Flask(__name__)

# Configurar CORS para permitir requests desde el navegador
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

class CoordinatorConnector:
    """Conector para comunicarse con el coordinator principal via TCP"""
    
    def __init__(self, coordinator_host='localhost', coordinator_port=8080):
        self.coordinator_host = coordinator_host
        self.coordinator_port = coordinator_port
        self.socket = None
        self.connected = False
        
    def connect(self):
        """Conecta al servidor principal"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Agregar timeout para evitar que se cuelgue
            self.socket.settimeout(5.0)
            self.socket.connect((self.coordinator_host, self.coordinator_port))
            self.connected = True
            
            # Recibir mensaje de bienvenida
            welcome_data = self.socket.recv(1024).decode()
            welcome_msg = json.loads(welcome_data)
            logging.info(f"Conectado al coordinator: {welcome_msg}")
            # Quitar timeout una vez conectado
            self.socket.settimeout(None)
            return True
            
        except Exception as e:
            logging.warning(f"No se pudo conectar al coordinator principal: {e}")
            self.connected = False
            if self.socket:
                try:
                    self.socket.close()
                except:
                    pass
                self.socket = None
            return False
    
    def send_message(self, message_dict):
        """Envía un mensaje al coordinator usando el protocolo existente"""
        if not self.connected:
            if not self.connect():
                return False
                
        try:
            message_bytes = json.dumps(message_dict).encode()
            # Enviar longitud primero (2 bytes)
            length = len(message_bytes)
            self.socket.send(length.to_bytes(2, 'big'))
            # Enviar mensaje completo
            self.socket.send(message_bytes)
            return True
            
        except Exception as e:
            logging.error(f"Error enviando mensaje: {e}")
            self.connected = False
            return False
    
    def send_scrape_command(self, url):
        """Envía comando de scraping al coordinator"""
        command_msg = {
            'type': 'command',
            'data': ['assign', 'auto', url],
            'client_id': 'api_client',
            'timestamp': datetime.now().isoformat()
        }
        
        return self.send_message(command_msg)
    
    def send_status_command(self):
        """Solicita el estado del coordinator"""
        command_msg = {
            'type': 'command',
            'data': ['status'],
            'client_id': 'api_client',
            'timestamp': datetime.now().isoformat()
        }
        
        return self.send_message(command_msg)
    
    def send_clients_command(self):
        """Solicita la lista de scrappers del coordinator"""
        command_msg = {
            'type': 'command',
            'data': ['clients'],
            'client_id': 'api_client',
            'timestamp': datetime.now().isoformat()
        }
        
        return self.send_message(command_msg)
    
    def get_tasks_from_server(self):
        """Obtiene las tareas y sus resultados del coordinator"""
        if not self.connected:
            if not self.connect():
                return None
        
        try:
            # Enviar comando para obtener tareas
            tasks_msg = {
                'type': 'command',
                'data': ['tasks'],
                'client_id': 'api_client',
                'timestamp': datetime.now().isoformat()
            }
            
            message_bytes = json.dumps(tasks_msg).encode()
            length = len(message_bytes)
            self.socket.send(length.to_bytes(2, 'big'))
            self.socket.send(message_bytes)
            
            # Recibir respuesta
            response_length_bytes = self.socket.recv(2)
            if not response_length_bytes:
                return None
            
            response_length = int.from_bytes(response_length_bytes, 'big')
            response_data = b""
            while len(response_data) < response_length:
                chunk = self.socket.recv(min(1024, response_length - len(response_data)))
                if not chunk:
                    break
                response_data += chunk
            
            if len(response_data) == response_length:
                response = json.loads(response_data.decode())
                if response.get('type') == 'tasks_response':
                    return response.get('tasks', {})
            
            return None
            
        except Exception as e:
            logging.error(f"Error obteniendo tareas del coordinator: {e}")
            self.connected = False
            return None
    
    def get_coordinator_logs(self):
        """Obtiene los logs del coordinator principal"""
        if not self.connected:
            if not self.connect():
                return None
        
        try:
            # Enviar comando para obtener logs
            logs_msg = {
                'type': 'get_logs',
                'client_id': 'api_client',
                'timestamp': datetime.now().isoformat()
            }
            
            message_bytes = json.dumps(logs_msg).encode()
            length = len(message_bytes)
            self.socket.send(length.to_bytes(2, 'big'))
            self.socket.send(message_bytes)
            
            # Recibir respuesta (usando el mismo protocolo del servidor)
            response_length_bytes = self.socket.recv(2)
            if not response_length_bytes:
                return None
                
            response_length = int.from_bytes(response_length_bytes, 'big')
            response_data = b""
            while len(response_data) < response_length:
                chunk = self.socket.recv(min(1024, response_length - len(response_data)))
                if not chunk:
                    break
                response_data += chunk
            
            if len(response_data) == response_length:
                response = json.loads(response_data.decode())
                return response.get('logs', [])
            
            return None
            
        except Exception as e:
            logging.error(f"Error obteniendo logs del coordinator: {e}")
            self.connected = False
            return None
    
    def get_system_status(self):
        """Obtiene el estado del sistema del coordinator principal"""
        if not self.connected:
            if not self.connect():
                return None
        
        try:
            status_msg = {
                'type': 'get_status',
                'client_id': 'api_client',
                'timestamp': datetime.now().isoformat()
            }
            
            message_bytes = json.dumps(status_msg).encode()
            length = len(message_bytes)
            self.socket.send(length.to_bytes(2, 'big'))
            self.socket.send(message_bytes)
            
            # Recibir respuesta
            response_length_bytes = self.socket.recv(2)
            if not response_length_bytes:
                return None
                
            response_length = int.from_bytes(response_length_bytes, 'big')
            response_data = b""
            while len(response_data) < response_length:
                chunk = self.socket.recv(min(1024, response_length - len(response_data)))
                if not chunk:
                    break
                response_data += chunk
            
            if len(response_data) == response_length:
                response = json.loads(response_data.decode())
                return response.get('status', {})
            
            return None
            
        except Exception as e:
            logging.error(f"Error obteniendo estado del coordinator: {e}")
            self.connected = False
            return None
    
    def disconnect(self):
        """Desconecta del coordinator"""
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
        self.connected = False

# Instancia global del conector
coordinator_host = os.environ.get('COORDINATOR_HOST', 'localhost')
coordinator_port = int(os.environ.get('COORDINATOR_PORT', 8080))
server_connector = CoordinatorConnector(coordinator_host, coordinator_port)

@app.route('/api/health', methods=['GET'])
def health_check():
    """Endpoint de health check"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'server_connected': server_connector.connected,
        'api_version': '1.0.0'
    })

@app.route('/api/scrape', methods=['POST'])
def create_scrape_task():
    """
    Endpoint para crear una tarea de scraping
    
    Body JSON esperado:
    {
        "url": "https://example.com"
    }
    """
    try:
        data = request.get_json()
        
        if not data or 'url' not in data:
            return jsonify({
                'error': 'URL es requerida',
                'example': {'url': 'https://example.com'}
            }), 400
        
        url = data['url']
        
        # Validación básica de URL
        if not url.startswith(('http://', 'https://')):
            return jsonify({
                'error': 'URL debe empezar con http:// o https://'
            }), 400
        
        # Enviar comando al coordinator principal
        success = server_connector.send_scrape_command(url)
        
        if success:
            return jsonify({
                'status': 'success',
                'message': 'Tarea de scraping enviada al coordinator',
                'url': url,
                'timestamp': datetime.now().isoformat()
            }), 201
        else:
            # Proporcionar más información sobre el error
            if not server_connector.connected:
                error_msg = 'Coordinator principal no disponible. Verificar que esté corriendo en puerto 8080.'
            else:
                error_msg = 'Error enviando la tarea al servidor'
                
            return jsonify({
                'status': 'error',
                'message': error_msg,
                'url': url,
                'server_connected': server_connector.connected,
                'suggestion': 'Asegúrate de que el servidor principal esté corriendo: python3 server/coordinator.py'
            }), 503
            
    except Exception as e:
        logging.error(f"Error en create_scrape_task: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Error interno: {str(e)}'
        }), 500

@app.route('/api/scrape/batch', methods=['POST'])
def create_batch_scrape_tasks():
    """
    Endpoint para crear múltiples tareas de scraping
    
    Body JSON esperado:
    {
        "urls": ["https://example1.com", "https://example2.com"]
    }
    """
    try:
        data = request.get_json()
        
        if not data or 'urls' not in data:
            return jsonify({
                'error': 'Lista de URLs es requerida',
                'example': {'urls': ['https://example1.com', 'https://example2.com']}
            }), 400
        
        urls = data['urls']
        
        if not isinstance(urls, list):
            return jsonify({
                'error': 'URLs debe ser una lista'
            }), 400
        
        results = []
        for url in urls:
            if not url.startswith(('http://', 'https://')):
                results.append({
                    'url': url,
                    'status': 'error',
                    'message': 'URL inválida'
                })
                continue
            
            success = server_connector.send_scrape_command(url)
            results.append({
                'url': url,
                'status': 'success' if success else 'error',
                'message': 'Enviado' if success else 'Error al enviar'
            })
        
        return jsonify({
            'status': 'completed',
            'results': results,
            'timestamp': datetime.now().isoformat()
        }), 201
        
    except Exception as e:
        logging.error(f"Error en create_batch_scrape_tasks: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Error interno: {str(e)}'
        }), 500

@app.route('/api/status', methods=['GET'])
def get_api_status():
    """Endpoint para obtener el estado de la API y del sistema"""
    api_status = {
        'api_status': 'running',
        'server_connection': {
            'connected': server_connector.connected,
            'host': server_connector.coordinator_host,
            'port': server_connector.coordinator_port
        },
        'timestamp': datetime.now().isoformat()
    }
    
    # Intentar obtener estado del servidor principal
    coordinator_status = server_connector.get_system_status()
    if coordinator_status:
        api_status['coordinator_status'] = coordinator_status
    
    return jsonify(api_status)

@app.route('/api/tasks', methods=['GET'])
def get_tasks():
    """Endpoint para obtener todas las tareas y sus resultados"""
    try:
        tasks = server_connector.get_tasks_from_server()
        
        if tasks is None:
            return jsonify({
                'status': 'error',
                'message': 'No se pudo obtener las tareas del servidor',
                'server_connected': server_connector.connected,
                'tasks': {},
                'suggestion': 'Verificar que el servidor principal esté corriendo'
            }), 503
        
        # Procesar y formatear las tareas para el frontend
        formatted_tasks = []
        for task_id, task in tasks.items():
            formatted_task = {
                'id': task.get('id', task_id),
                'url': task.get('data', {}).get('url', 'URL no disponible'),
                'client_id': task.get('client_id', 'N/A'),
                'status': task.get('status', 'unknown'),
                'assigned_at': task.get('assigned_at'),
                'completed_at': task.get('completed_at'),
                'result': task.get('result')
            }
            formatted_tasks.append(formatted_task)
        
        # Ordenar por fecha de asignación (más recientes primero)
        formatted_tasks.sort(key=lambda x: x.get('assigned_at', ''), reverse=True)
        
        return jsonify({
            'status': 'success',
            'tasks': formatted_tasks,
            'total_tasks': len(formatted_tasks),
            'completed_tasks': len([t for t in formatted_tasks if t['status'] == 'completed']),
            'pending_tasks': len([t for t in formatted_tasks if t['status'] == 'assigned']),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logging.error(f"Error en endpoint /api/tasks: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Error interno: {str(e)}',
            'tasks': []
        }), 500

@app.route('/api/logs', methods=['GET'])
def get_logs():
    """Endpoint para obtener logs del servidor principal"""
    try:
        # Parámetros opcionales
        limit = request.args.get('limit', 50, type=int)
        log_type = request.args.get('type', 'all')  # all, server, clients
        
        logs = []
        
        # Obtener logs del servidor principal
        if log_type in ['all', 'server']:
            coordinator_logs = server_connector.get_coordinator_logs()
            if coordinator_logs:
                logs.extend([{
                    'source': 'server',
                    'timestamp': log.get('time_now', ''),
                    'type': log.get('type', 'INFO'),
                    'message': log.get('message', ''),
                    'client_id': log.get('client_id', None)
                } for log in coordinator_logs[-limit:]])
        
        # Agregar logs de la API
        if log_type in ['all', 'api']:
            # Aquí podríamos agregar logs específicos de la API
            api_log = {
                'source': 'api',
                'timestamp': datetime.now().isoformat(),
                'type': 'INFO',
                'message': f'API funcionando en puerto {os.environ.get("API_PORT", 8082)}',
                'client_id': None
            }
            logs.append(api_log)
        
        # Ordenar por timestamp
        logs.sort(key=lambda x: x['timestamp'], reverse=True)
        
        return jsonify({
            'status': 'success',
            'logs': logs[:limit],
            'total_logs': len(logs),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Error obteniendo logs: {str(e)}',
            'logs': []
        }), 500

@app.route('/api/logs/stream', methods=['GET'])
def get_logs_stream():
    """Endpoint para obtener logs en tiempo real (Server-Sent Events)"""
    def generate_logs():
        while True:
            try:
                # Obtener logs recientes del coordinator
                coordinator_logs = server_connector.get_coordinator_logs()
                if coordinator_logs:
                    # Enviar solo el último log
                    latest_log = coordinator_logs[-1]
                    log_data = {
                        'source': 'server',
                        'timestamp': latest_log.get('time_now', ''),
                        'type': latest_log.get('type', 'INFO'),
                        'message': latest_log.get('message', ''),
                        'client_id': latest_log.get('client_id', None)
                    }
                    yield f"data: {json.dumps(log_data)}\n\n"
                
                time.sleep(2)  # Actualizar cada 2 segundos
                
            except Exception as e:
                error_data = {
                    'source': 'api',
                    'timestamp': datetime.now().isoformat(),
                    'type': 'ERROR',
                    'message': f'Error obteniendo logs: {str(e)}',
                    'client_id': None
                }
                yield f"data: {json.dumps(error_data)}\n\n"
                time.sleep(5)
    
    return app.response_class(
        generate_logs(),
        mimetype='text/plain',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive'
        }
    )

@app.route('/', methods=['GET'])
def root():
    """Endpoint raíz - redirige al dashboard"""
    import os
    dashboard_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'web_interface')
    return send_from_directory(dashboard_path, 'dashboard.html')

@app.route('/dashboard')
def dashboard():
    """Sirve el dashboard HTML"""
    import os
    dashboard_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'web_interface')
    return send_from_directory(dashboard_path, 'dashboard.html')

@app.route('/api/info', methods=['GET'])
def api_info():
    """Endpoint con información básica de la API"""
    return jsonify({
        'name': 'Scrapper Distribuido API',
        'version': '1.0.0',
        'endpoints': {
            'GET /': 'Dashboard web',
            'GET /dashboard': 'Dashboard web',
            'POST /api/scrape': 'Crear tarea de scraping simple',
            'POST /api/scrape/batch': 'Crear múltiples tareas de scraping',
            'GET /api/status': 'Estado de la API',
            'GET /api/health': 'Health check',
            'GET /api/info': 'Información de la API'
        },
        'timestamp': datetime.now().isoformat()
    })

def init_server_connection():
    """Inicializa la conexión con el coordinator principal de forma no bloqueante"""
    def try_connect():
        if server_connector.connect():
            logging.info("✅ Conexión inicial con coordinator principal establecida")
        else:
            logging.info("⚠️  Coordinator principal no disponible (se intentará conectar cuando sea necesario)")
    
    # Ejecutar la conexión en un hilo separado para no bloquear
    connection_thread = threading.Thread(target=try_connect)
    connection_thread.daemon = True
    connection_thread.start()

if __name__ == '__main__':
    # Configurar variables de entorno
    port = int(os.environ.get('API_PORT', 8082))
    host = os.environ.get('API_HOST', '0.0.0.0')
    
    logging.info(f"Iniciando API REST en {host}:{port}")
    logging.info(f"Conectando a coordinator principal en {server_connector.coordinator_host}:{server_connector.coordinator_port}")
    
    # Intentar conectar al coordinator principal de forma no bloqueante
    init_server_connection()
    
    # Iniciar API (esto ya no se bloquea)
    logging.info("🌐 API REST lista para recibir requests")
    app.run(host=host, port=port, debug=False)