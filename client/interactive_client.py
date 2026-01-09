#!/usr/bin/env python3
"""
Cliente Interactivo para el sistema distribuido de scrapping

Mantiene una conexión persistente con el Router y permite:
- Enviar peticiones de scrapping
- Recibir resultados asíncronamente
- Ver estado del sistema
- Comandos interactivos
"""

import socket
import json
import sys
import os
import threading
import uuid
import time
from datetime import datetime
from queue import Queue, Empty

# Agregar directorio padre al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from base_node.utils import MessageProtocol

# Configuración
DNS_SERVER = "dns_server"
DNS_PORT = 5300
ROUTER_PORT = 7070


class InteractiveClient:
    """Cliente interactivo con conexión persistente al Router"""
    
    def __init__(self):
        self.router_ip = None
        self.socket = None
        self.running = True
        self.connected = False
        
        # Cola de mensajes recibidos
        self.message_queue = Queue()
        
        # Tracking de peticiones pendientes
        self.pending_requests = {}  # {task_id: {'url': url, 'timestamp': datetime}}
        self.completed_requests = {}  # {task_id: {'result': result, 'timestamp': datetime}}
        
        # Thread para recibir mensajes
        self.receive_thread = None
        
    def log(self, message, level="INFO"):
        """Log con color"""
        colors = {
            "INFO": "\033[94m",      # Azul
            "SUCCESS": "\033[92m",   # Verde
            "WARNING": "\033[93m",   # Amarillo
            "ERROR": "\033[91m",     # Rojo
            "RESULT": "\033[95m"     # Magenta
        }
        reset = "\033[0m"
        timestamp = datetime.now().strftime('%H:%M:%S')
        color = colors.get(level, "")
        print(f"{color}[{timestamp}] {level}: {message}{reset}")
    
    def discover_router(self):
        """Descubre el Router jefe usando DNS"""
        try:
            self.log("🔍 Buscando Router en DNS...")
            
            query = {
                'query_type': 'get_nodes',
                'node_type': 'router'
            }
            
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(5)
                sock.connect((DNS_SERVER, DNS_PORT))
                
                query_bytes = json.dumps(query).encode()
                sock.send(len(query_bytes).to_bytes(2, 'big'))
                sock.send(query_bytes)
                
                length_bytes = sock.recv(2)
                if not length_bytes:
                    self.log("DNS no respondió", "ERROR")
                    return False
                
                length = int.from_bytes(length_bytes, 'big')
                response_bytes = sock.recv(length)
                response = json.loads(response_bytes.decode())
                
                router_ips = response.get('nodes', [])
                
                if not router_ips:
                    self.log("No hay Routers disponibles", "ERROR")
                    return False
                
                # Buscar el jefe o usar el primero
                for ip in router_ips:
                    if self._is_boss(ip):
                        self.router_ip = ip
                        self.log(f"✓ Router jefe encontrado: {ip}", "SUCCESS")
                        return True
                
                self.router_ip = router_ips[0]
                self.log(f"✓ Usando Router: {self.router_ip}", "SUCCESS")
                return True
                
        except Exception as e:
            self.log(f"Error conectando con DNS: {e}", "ERROR")
            return False
    
    def _is_boss(self, router_ip):
        """Verifica si un Router es jefe"""
        try:
            msg = {
                'type': MessageProtocol.MESSAGE_TYPES['IDENTIFICATION'],
                'sender_id': 'interactive-client',
                'data': {'is_temporary': True}
            }
            
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(3)
                sock.connect((router_ip, ROUTER_PORT))
                
                msg_bytes = json.dumps(msg).encode()
                sock.send(len(msg_bytes).to_bytes(2, 'big'))
                sock.send(msg_bytes)
                
                length_bytes = sock.recv(2)
                if not length_bytes:
                    return False
                
                length = int.from_bytes(length_bytes, 'big')
                response_bytes = sock.recv(length)
                response = json.loads(response_bytes.decode())
                
                return response.get('data', {}).get('is_boss', False)
        except:
            return False
    
    def connect_to_router(self):
        """Establece conexión persistente con el Router"""
        try:
            self.log(f"🔌 Conectando con Router {self.router_ip}...")
            
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.router_ip, ROUTER_PORT))
            self.connected = True
            
            # Iniciar thread para recibir mensajes
            self.receive_thread = threading.Thread(target=self._receive_loop, daemon=True)
            self.receive_thread.start()
            
            self.log("✓ Conectado al Router", "SUCCESS")
            return True
            
        except Exception as e:
            self.log(f"Error conectando: {e}", "ERROR")
            self.connected = False
            return False
    
    def _receive_loop(self):
        """Loop para recibir mensajes del Router"""
        try:
            while self.running and self.connected:
                # Leer longitud del mensaje
                length_bytes = self.socket.recv(2)
                if not length_bytes:
                    self.log("Conexión cerrada por el Router", "WARNING")
                    self.connected = False
                    break
                
                # Leer mensaje completo
                length = int.from_bytes(length_bytes, 'big')
                message_bytes = b''
                while len(message_bytes) < length:
                    chunk = self.socket.recv(length - len(message_bytes))
                    if not chunk:
                        break
                    message_bytes += chunk
                
                if len(message_bytes) < length:
                    self.log("Mensaje incompleto recibido", "WARNING")
                    continue
                
                # Procesar mensaje
                message = json.loads(message_bytes.decode())
                self._handle_message(message)
                
        except Exception as e:
            if self.running:
                self.log(f"Error en recepción: {e}", "ERROR")
            self.connected = False
    
    def _handle_message(self, message):
        """Procesa mensajes recibidos del Router"""
        msg_type = message.get('type')
        data = message.get('data', {})
        
        # Resultado de scrapping
        if msg_type == MessageProtocol.MESSAGE_TYPES.get('TASK_RESULT'):
            task_id = data.get('task_id')
            result = data.get('result', {})
            success = data.get('success', False)
            
            if task_id in self.pending_requests:
                request_info = self.pending_requests.pop(task_id)
                url = request_info['url']
                
                self.completed_requests[task_id] = {
                    'result': result,
                    'timestamp': datetime.now()
                }
                
                if success:
                    self.log(f"✓ Resultado recibido para: {url}", "SUCCESS")
                    self._display_result(url, result)
                else:
                    self.log(f"✗ Fallo en scrapping de: {url}", "ERROR")
                    if result:
                        self.log(f"  Error: {result}", "ERROR")
        
        # Respuesta a status
        elif msg_type == MessageProtocol.MESSAGE_TYPES.get('STATUS_RESPONSE'):
            self._display_status(data)
        
        # Otros mensajes
        else:
            self.log(f"Mensaje recibido: {msg_type}", "INFO")
    
    def _display_result(self, url, result):
        """Muestra resultado de scrapping formateado"""
        print("\n" + "="*70)
        self.log(f"RESULTADO: {url}", "RESULT")
        print("="*70)
        
        if isinstance(result, dict):
            for key, value in result.items():
                if isinstance(value, str) and len(value) > 100:
                    value = value[:100] + "..."
                print(f"  {key}: {value}")
        else:
            print(f"  {result}")
        
        print("="*70 + "\n")
    
    def _display_status(self, status):
        """Muestra estado del sistema"""
        print("\n" + "="*70)
        self.log("ESTADO DEL SISTEMA", "RESULT")
        print("="*70)
        
        for key, value in status.items():
            print(f"  {key}: {value}")
        
        print("="*70 + "\n")
    
    def send_message(self, message):
        """Envía mensaje al Router"""
        try:
            if not self.connected:
                self.log("No hay conexión con el Router", "ERROR")
                return False
            
            msg_bytes = json.dumps(message).encode()
            self.socket.send(len(msg_bytes).to_bytes(2, 'big'))
            self.socket.send(msg_bytes)
            return True
            
        except Exception as e:
            self.log(f"Error enviando mensaje: {e}", "ERROR")
            self.connected = False
            return False
    
    def scrape_url(self, url):
        """Solicita scrapping de una URL"""
        task_id = str(uuid.uuid4())
        
        message = {
            'type': MessageProtocol.MESSAGE_TYPES['CLIENT_REQUEST'],
            'sender_id': 'interactive-client',
            'data': {
                'task_id': task_id,
                'url': url
            }
        }
        
        if self.send_message(message):
            self.pending_requests[task_id] = {
                'url': url,
                'timestamp': datetime.now()
            }
            self.log(f"→ Petición enviada: {url}", "INFO")
            self.log(f"  Task ID: {task_id}", "INFO")
            return True
        
        return False
    
    def request_status(self):
        """Solicita estado del sistema"""
        message = {
            'type': MessageProtocol.MESSAGE_TYPES.get('STATUS_REQUEST', 'status_request'),
            'sender_id': 'interactive-client',
            'data': {}
        }
        
        if self.send_message(message):
            self.log("→ Solicitando estado del sistema...", "INFO")
            return True
        
        return False
    
    def show_pending(self):
        """Muestra peticiones pendientes"""
        if not self.pending_requests:
            print("\nNo hay peticiones pendientes\n")
            return
        
        print("\n" + "="*70)
        print("PETICIONES PENDIENTES:")
        print("="*70)
        
        for task_id, info in self.pending_requests.items():
            url = info['url']
            elapsed = (datetime.now() - info['timestamp']).total_seconds()
            print(f"  • {url}")
            print(f"    Task ID: {task_id}")
            print(f"    Tiempo esperando: {elapsed:.1f}s")
        
        print("="*70 + "\n")
    
    def show_completed(self):
        """Muestra peticiones completadas"""
        if not self.completed_requests:
            print("\nNo hay peticiones completadas\n")
            return
        
        print("\n" + "="*70)
        print("PETICIONES COMPLETADAS:")
        print("="*70)
        
        for task_id, info in self.completed_requests.items():
            timestamp = info['timestamp'].strftime('%H:%M:%S')
            print(f"  • Task ID: {task_id}")
            print(f"    Completado: {timestamp}")
        
        print("="*70 + "\n")
    
    def show_help(self):
        """Muestra ayuda de comandos"""
        help_text = """
╔══════════════════════════════════════════════════════════════════╗
║                      COMANDOS DISPONIBLES                        ║
╠══════════════════════════════════════════════════════════════════╣
║  scrape <url>         - Solicitar scrapping de una URL          ║
║  status               - Ver estado del sistema                   ║
║  pending              - Mostrar peticiones pendientes            ║
║  completed            - Mostrar peticiones completadas           ║
║  clear                - Limpiar historial de completadas         ║
║  help                 - Mostrar esta ayuda                       ║
║  exit / quit          - Salir del cliente                        ║
╚══════════════════════════════════════════════════════════════════╝
        """
        print(help_text)
    
    def run(self):
        """Loop principal interactivo"""
        print("\n" + "="*70)
        print("  CLIENTE INTERACTIVO - Sistema de Scrapping Distribuido")
        print("="*70 + "\n")
        
        # Descubrir y conectar
        if not self.discover_router():
            return
        
        if not self.connect_to_router():
            return
        
        self.show_help()
        
        # Loop de comandos
        try:
            while self.running and self.connected:
                try:
                    command = input("\n> ").strip()
                    
                    if not command:
                        continue
                    
                    parts = command.split(maxsplit=1)
                    cmd = parts[0].lower()
                    
                    if cmd in ['exit', 'quit']:
                        self.log("Cerrando cliente...", "INFO")
                        break
                    
                    elif cmd == 'scrape':
                        if len(parts) < 2:
                            self.log("Uso: scrape <url>", "WARNING")
                        else:
                            url = parts[1]
                            self.scrape_url(url)
                    
                    elif cmd == 'status':
                        self.request_status()
                    
                    elif cmd == 'pending':
                        self.show_pending()
                    
                    elif cmd == 'completed':
                        self.show_completed()
                    
                    elif cmd == 'clear':
                        self.completed_requests.clear()
                        self.log("Historial limpiado", "SUCCESS")
                    
                    elif cmd == 'help':
                        self.show_help()
                    
                    else:
                        self.log(f"Comando desconocido: {cmd}", "WARNING")
                        self.log("Escribe 'help' para ver comandos disponibles", "INFO")
                
                except KeyboardInterrupt:
                    print()  # Nueva línea después de Ctrl+C
                    self.log("Usa 'exit' para salir", "INFO")
        
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Limpieza al cerrar"""
        self.running = False
        self.connected = False
        
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
        
        self.log("Conexión cerrada", "INFO")


def main():
    """Punto de entrada"""
    client = InteractiveClient()
    client.run()


if __name__ == "__main__":
    main()
