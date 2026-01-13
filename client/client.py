#!/usr/bin/env python3
"""
Cliente Interactivo para el sistema distribuido de scrapping

Mantiene una conexión persistente con el Router y permite:
- Enviar peticiones de scrapping
- Recibir resultados asíncronamente
- Ver estado del sistema
- Comandos interactivos

Usa el DNS interno de Docker para descubrir servicios.
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
from base_node.utils.node_connection import NodeConnection

# Configuración - Usa hostnames de Docker (network-alias)
ROUTER_HOST = os.environ.get('ROUTER_HOST', 'router')
ROUTER_PORT = int(os.environ.get('ROUTER_PORT', 7070))


class InteractiveClient:
    """Cliente interactivo con conexión persistente al Router"""
    
    def __init__(self, router_host=ROUTER_HOST, router_port=ROUTER_PORT):
        self.router_host = router_host
        self.router_port = router_port
        self.router_ip = None
        self.running = True
        
        # Conexión persistente con el router usando NodeConnection
        self.router_connection = None
        
        # Cliente ID único
        self.client_id = f"client-{uuid.uuid4().hex[:8]}"
        
        # Thread de heartbeat
        self.heartbeat_thread = None
        
        # Tracking de peticiones pendientes
        self.pending_requests = {}  # {task_id: {'url': url, 'timestamp': datetime}}
        self.completed_requests = {}  # {task_id: {'result': result, 'timestamp': datetime}}
        
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
    
    def resolve_router(self):
        """Resuelve TODAS las IPs de routers usando DNS de Docker"""
        try:
            self.log(f"🔍 Consultando DNS de Docker por routers...")
            
            # Docker DNS devuelve TODAS las IPs asociadas al network-alias 'router'
            # Usamos getaddrinfo para obtener todas las IPs, no solo una
            addr_info = socket.getaddrinfo(
                self.router_host, 
                self.router_port, 
                socket.AF_INET, 
                socket.SOCK_STREAM
            )
            
            # Extraer IPs únicas
            router_ips = list(set([info[4][0] for info in addr_info]))
            
            self.log(f"✓ {len(router_ips)} router(s) encontrado(s): {router_ips}", "SUCCESS")
            
            # Buscar el jefe entre todos los routers
            boss_ip = self._find_boss_router(router_ips)
            
            if boss_ip:
                self.router_ip = boss_ip
                self.log(f"✓ Router jefe encontrado en {boss_ip}:{self.router_port}", "SUCCESS")
                return True
            else:
                self.log("No se encontró un router jefe", "ERROR")
                return False
            
        except socket.gaierror as e:
            self.log(f"Error resolviendo '{self.router_host}': {e}", "ERROR")
            self.log("Asegúrate de estar en la red 'scrapper-network'", "WARNING")
            return False
        except Exception as e:
            self.log(f"Error: {e}", "ERROR")
            return False
    
    def _find_boss_router(self, router_ips):
        """
        Encuentra el router jefe enviando mensajes de identificación temporal.
        Similar a cómo los nodos encuentran a sus jefes.
        
        Args:
            router_ips: Lista de IPs de routers
            
        Returns:
            IP del router jefe o None
        """
        self.log(f"🔍 Buscando router jefe entre {len(router_ips)} candidatos...")
        
        for ip in router_ips:
            try:
                # Crear socket temporal
                temp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                temp_socket.settimeout(3)
                
                # Conectar
                temp_socket.connect((ip, self.router_port))
                
                # Enviar mensaje de identificación temporal
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
                
                # Esperar respuesta
                length_bytes = temp_socket.recv(2)
                if length_bytes:
                    length = int.from_bytes(length_bytes, 'big')
                    response_bytes = temp_socket.recv(length)
                    response = json.loads(response_bytes.decode())
                    
                    # Verificar si es el jefe
                    is_boss = response.get('data', {}).get('is_boss', False)
                    
                    temp_socket.close()
                    
                    if is_boss:
                        self.log(f"  ✓ Router jefe confirmado en {ip}", "SUCCESS")
                        return ip
                    else:
                        self.log(f"  ✗ Router en {ip} no es jefe", "INFO")
                else:
                    temp_socket.close()
                    
            except socket.timeout:
                self.log(f"  ✗ Timeout en {ip}", "WARNING")
                try:
                    temp_socket.close()
                except:
                    pass
            except Exception as e:
                self.log(f"  ✗ Error consultando {ip}: {e}", "WARNING")
                try:
                    temp_socket.close()
                except:
                    pass
        
        return None
    
    def connect(self):
        """Descubre el Router Jefe y establece conexión persistente"""
        if not self.router_ip:
            if not self.resolve_router():
                return False
        
        self.log(f"✓ Router Jefe identificado: {self.router_ip}:{self.router_port}", "SUCCESS")
        
        try:
            # Crear socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.router_ip, self.router_port))
            self.log(f"✓ Conexión establecida con Router", "SUCCESS")
            
            # Crear NodeConnection con callback para mensajes
            self.router_connection = NodeConnection(
                node_type='router',
                ip=self.router_ip,
                port=self.router_port,
                on_message_callback=self._handle_message,
                sender_node_type='client',
                sender_id=self.client_id
            )
            
            # Conectar usando el socket existente
            if not self.router_connection.connect(existing_socket=sock):
                raise Exception("No se pudo establecer NodeConnection")
            
            self.log(f"✓ NodeConnection establecida con Router", "SUCCESS")
            
            # Enviar mensaje de identificación
            identification_msg = {
                'type': MessageProtocol.MESSAGE_TYPES['IDENTIFICATION'],
                'sender_id': self.client_id,
                'node_type': 'client',  # A nivel raíz para que Node lo detecte
                'timestamp': datetime.now().isoformat(),
                'data': {
                    'is_temporary': False,
                    'is_boss': False,
                    'port': 0  # Cliente no acepta conexiones entrantes
                }
            }
            
            self.router_connection.send_message(identification_msg)
            self.log(f"✓ Identificación enviada como '{self.client_id}'", "SUCCESS")
            
            # Iniciar thread de heartbeat
            self.heartbeat_thread = threading.Thread(target=self._send_heartbeats, daemon=True)
            self.heartbeat_thread.start()
            
            return True
            
        except Exception as e:
            self.log(f"✗ Error estableciendo conexión persistente: {e}", "ERROR")
            if self.router_connection:
                try:
                    self.router_connection.close()
                except:
                    pass
                self.router_connection = None
            return False
    

    
    def _send_heartbeats(self):
        """Thread que envía heartbeats periódicos al Router"""
        self.log("💓 Iniciando envío de heartbeats...")
        
        while self.running and self.router_connection and self.router_connection.connected:
            try:
                time.sleep(5)  # Heartbeat cada 5 segundos
                
                if not self.router_connection or not self.router_connection.connected:
                    self.log("⚠️ Desconexión detectada del Router", "WARNING")
                    break
                
                # Crear mensaje de heartbeat
                heartbeat_msg = {
                    'type': MessageProtocol.MESSAGE_TYPES['HEARTBEAT'],
                    'sender_id': self.client_id,
                    'timestamp': datetime.now().isoformat(),
                    'data': {}
                }
                
                # Enviar heartbeat usando NodeConnection
                self.router_connection.send_message(heartbeat_msg)
                
            except Exception as e:
                if self.running:
                    self.log(f"⚠️ Error enviando heartbeat (Router caído): {e}", "WARNING")
                break
        
        self.log("💓 Envío de heartbeats finalizado", "INFO")
        
        # Si aún estamos corriendo, intentar reconectar
        if self.running:
            self.log("🔄 Intentando reconectar al Router...", "WARNING")
            self._reconnect_to_router()
    
    def _reconnect_to_router(self):
        """Intenta reconectar al Router usando DNS de Docker"""
        retry_interval = 5  # Segundos entre intentos
        max_retries = 10
        
        for attempt in range(1, max_retries + 1):
            if not self.running:
                break
            
            self.log(f"🔄 Intento de reconexión {attempt}/{max_retries}...", "WARNING")
            
            # Cerrar conexión anterior si existe
            if self.router_connection:
                try:
                    self.router_connection.close()
                except:
                    pass
                self.router_connection = None
            
            # Resetear IP para forzar nueva búsqueda DNS
            self.router_ip = None
            
            # Intentar conectar
            if self.connect():
                self.log("✅ Reconexión exitosa al Router", "SUCCESS")
                return True
            
            # Esperar antes del próximo intento
            if attempt < max_retries:
                self.log(f"⏳ Esperando {retry_interval}s antes del próximo intento...", "INFO")
                time.sleep(retry_interval)
        
        self.log("❌ No se pudo reconectar al Router después de múltiples intentos", "ERROR")
        return False
        
        # Si aún estamos corriendo, intentar reconectar
        if self.running:
            self.log("🔄 Intentando reconectar al Router...", "WARNING")
            self._reconnect_to_router()
    
    def _handle_message(self, node_connection, message):
        """Maneja mensajes recibidos del Router"""
        msg_type = message.get('type')
        data = message.get('data', {})
        
        if msg_type == MessageProtocol.MESSAGE_TYPES['TASK_RESULT']:
            self._handle_task_result(data)
        elif msg_type == MessageProtocol.MESSAGE_TYPES['STATUS_RESPONSE']:
            self._handle_status_response(data)
        else:
            self.log(f"Mensaje recibido: {msg_type}", "INFO")
    
    def _handle_task_result(self, data):
        """Maneja resultado de scrapping"""
        task_id = data.get('task_id')
        result = data.get('result', {})
        success = data.get('success', False)
        
        if task_id in self.pending_requests:
            req_info = self.pending_requests.pop(task_id)
            url = req_info['url']
            start_time = req_info['timestamp']
            elapsed = (datetime.now() - start_time).total_seconds()
            
            self.completed_requests[task_id] = {
                'result': result,
                'timestamp': datetime.now(),
                'elapsed': elapsed
            }
            
            if success:
                self.log(f"✓ Resultado recibido para '{url}' ({elapsed:.2f}s)", "SUCCESS")
                self._print_result(result)
            else:
                self.log(f"✗ Error procesando '{url}': {result.get('error', 'Unknown')}", "ERROR")
        else:
            self.log(f"Resultado recibido para task desconocida: {task_id}", "WARNING")
    
    def _handle_status_response(self, data):
        """Maneja respuesta de status"""
        self.log("📊 Estado del Sistema:", "RESULT")
        print(json.dumps(data, indent=2))
    
    def _print_result(self, result):
        """Muestra resultado de scrapping formateado"""
        print("\n" + "="*60)
        print("📄 RESULTADO DEL SCRAPPING:")
        print("="*60)
        
        if isinstance(result, dict):
            for key, value in result.items():
                if isinstance(value, str) and len(value) > 100:
                    value = value[:100] + "..."
                print(f"  {key}: {value}")
        else:
            print(f"  {result}")
        
        print("="*60 + "\n")
    
    def send_scrape_request(self, url):
        """Envía petición de scrapping"""
        if not self.router_connection or not self.router_connection.connected:
            self.log("No conectado al Router (intenta 'reconnect')", "ERROR")
            return None
        
        task_id = str(uuid.uuid4())
        
        message = {
            'type': MessageProtocol.MESSAGE_TYPES['CLIENT_REQUEST'],
            'sender_id': self.client_id,
            'timestamp': datetime.now().isoformat(),
            'data': {
                'task_id': task_id,
                'url': url
            }
        }
        
        try:
            self.router_connection.send_message(message)
            
            self.pending_requests[task_id] = {
                'url': url,
                'timestamp': datetime.now()
            }
            
            self.log(f"📤 Petición enviada: {url}", "INFO")
            self.log(f"   Task ID: {task_id}", "INFO")
            return task_id
            
        except Exception as e:
            self.log(f"Error enviando petición: {e}", "ERROR")
            return None
    
    def send_status_request(self):
        """Solicita estado del sistema"""
        if not self.router_connection or not self.router_connection.connected:
            self.log("No conectado al Router (intenta 'reconnect')", "ERROR")
            return False
        
        message = {
            'type': MessageProtocol.MESSAGE_TYPES['STATUS_REQUEST'],
            'sender_id': self.client_id,
            'timestamp': datetime.now().isoformat(),
            'data': {}
        }
        
        try:
            self.router_connection.send_message(message)
            self.log("📤 Solicitando estado del sistema...", "INFO")
            return True
            
        except Exception as e:
            self.log(f"Error enviando status request: {e}", "ERROR")
            return False
    
    def show_help(self):
        """Muestra ayuda de comandos"""
        help_text = """
╔══════════════════════════════════════════════════════════════╗
║                    COMANDOS DISPONIBLES                      ║
╠══════════════════════════════════════════════════════════════╣
║  scrape <url>     - Solicita scrapping de una URL           ║
║  status           - Muestra estado del sistema               ║
║  pending          - Muestra peticiones pendientes            ║
║  history          - Muestra historial de peticiones          ║
║  reconnect        - Reconectar al Router (si se cayó)        ║
║  clear            - Limpia la pantalla                       ║
║  help             - Muestra esta ayuda                       ║
║  exit/quit        - Cierra el cliente                        ║
╚══════════════════════════════════════════════════════════════╝
        """
        print(help_text)
    
    def show_pending(self):
        """Muestra peticiones pendientes"""
        if not self.pending_requests:
            self.log("No hay peticiones pendientes", "INFO")
            return
        
        print("\n📋 Peticiones Pendientes:")
        print("-" * 80)
        for task_id, info in self.pending_requests.items():
            elapsed = (datetime.now() - info['timestamp']).total_seconds()
            print(f"  • {info['url'][:60]}")
            print(f"    Task ID: {task_id}")
            print(f"    Tiempo: {elapsed:.1f}s")
            print()
    
    def show_history(self):
        """Muestra historial de peticiones completadas"""
        if not self.completed_requests:
            self.log("No hay historial", "INFO")
            return
        
        print("\n📚 Historial (últimas 10):")
        print("-" * 80)
        recent = list(self.completed_requests.items())[-10:]
        for task_id, info in recent:
            timestamp = info['timestamp'].strftime('%H:%M:%S')
            elapsed = info['elapsed']
            print(f"  [{timestamp}] Completada en {elapsed:.2f}s")
            print(f"    Task ID: {task_id}")
            print()
    
    def run(self):
        """Ejecuta el cliente interactivo"""
        print("\n" + "="*60)
        print("  🌐 CLIENTE INTERACTIVO - Sistema de Scrapping Distribuido")
        print("="*60)
        
        # Conectar al Router
        if not self.connect():
            self.log("No se pudo conectar al Router", "ERROR")
            return
        
        self.show_help()
        
        # Loop principal
        try:
            while self.running:
                try:
                    command = input("\n> ").strip()
                    
                    if not command:
                        continue
                    
                    parts = command.split(maxsplit=1)
                    cmd = parts[0].lower()
                    
                    if cmd in ['exit', 'quit']:
                        self.log("Cerrando cliente...", "INFO")
                        self.running = False
                        break
                    
                    elif cmd == 'scrape':
                        if len(parts) < 2:
                            self.log("Uso: scrape <url>", "WARNING")
                        else:
                            url = parts[1]
                            self.send_scrape_request(url)
                    
                    elif cmd == 'status':
                        self.send_status_request()
                    
                    elif cmd == 'pending':
                        self.show_pending()
                    
                    elif cmd == 'history':
                        self.show_history()
                    
                    elif cmd == 'reconnect':
                        self.log("🔄 Reconectando al Router...", "INFO")
                        if self._reconnect_to_router():
                            self.log("✅ Reconexión exitosa", "SUCCESS")
                        else:
                            self.log("❌ Reconexión falló", "ERROR")
                    
                    elif cmd == 'clear':
                        os.system('clear' if os.name != 'nt' else 'cls')
                    
                    elif cmd == 'help':
                        self.show_help()
                    
                    else:
                        self.log(f"Comando desconocido: '{cmd}'. Usa 'help' para ver comandos.", "WARNING")
                
                except KeyboardInterrupt:
                    print()
                    self.log("Usa 'exit' para salir", "INFO")
                    continue
                
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Limpia recursos"""
        self.running = False
        
        # Cerrar conexión
        if self.router_connection:
            try:
                self.router_connection.close()
                self.router_connection = None
            except:
                pass
        
        # Esperar thread de heartbeat
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join(timeout=2)
        
        self.log("Cliente cerrado", "INFO")


def main():
    """Punto de entrada"""
    client = InteractiveClient()
    client.run()


if __name__ == '__main__':
    main()
