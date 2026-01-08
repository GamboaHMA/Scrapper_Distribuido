#!/usr/bin/env python3
"""
Router Client - Cliente interactivo para enviar peticiones al RouterNode
Se ejecuta desde terminal y permite seleccionar URLs predefinidas para scrapping
"""

import sys
import os

# Agregar directorio padre al path para imports absolutos (mismo que usan los nodos)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import socket
import json
import threading
import time
import uuid
from datetime import datetime
from base_node.node import Node
from base_node.utils import MessageProtocol, NodeConnection, BossProfile


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from base_node.utils import MessageProtocol

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RouterClient:
    """
    Cliente interactivo para enviar peticiones de scrapping al RouterNode.
    Se conecta al puerto 7070 (router) usando el mismo protocolo de mensajes.
    """
    
    # URLs de prueba locales para diferentes casos
    TEST_URLS = [
        "http://httpbin.org/html",           # 1: HTML simple
        "http://httpbin.org/json",           # 2: JSON response  
        "http://httpbin.org/get",            # 3: GET params
        "https://python.org",               # 4: HTTPS simple
        "http://httpbin.org/delay/2",        # 5: Con delay (2s)
        "http://httpbin.org/cache/100",      # 6: Cacheable
        "http://httpbin.org/cache/200",     # 7: Status 200
        "http://httpbin.org/cache/404",     # 8: Status 404 (error)
    ]

    STATES = {
        'DISCONNECTED': 'DESCONECTADO',
        'CONNECTED': 'CONECTADO',
        'WAITING_RESPONSE': 'ESPERANDO_RESPUESTA'
    }
    
    def __init__(self, puerto=5050):
        self.ip = None
        self.port = puerto
        self.node_type = 'cliente'
        self.router_host = None
        self.router_port = 7070
        self.socket = None
        self.running = True
        self.state = 'DISCONNECTED'
        self.current_task_id = None
        # cache por si se cae el nodo router
        self.cache = {}
        
        print("Router Client iniciado")
        self._show_status()

    def _show_status(self):
        """Muestra el estado actual claramente"""
        print("\n" + "="*70)
        print(f"ESTADO ACTUAL: {self.STATES[self.state]}")
        print("="*70)
        
        if self.state == 'DISCONNECTED':
            print("💡 ESTADO: No hay conexión con Router")
            print("📋 ACCIONES DISPONIBLES:")
            print("   connect <IP> [puerto]  → Conectarse (ej: connect 172.20.0.3 7070)")
            print("   status                 → Ver estado")
            print("   help                   → Este menú")
            print("   quit                   → Salir")
            
        elif self.state == 'CONNECTED':
            print(f"📡 Router: {self.router_host}:{self.router_port}")
            print("📋 ACCIONES DISPONIBLES:")
            print("   1-8                    → Enviar URL de prueba")
            print("   url <URL>              → URL personalizada")
            print("   disconnect             → Desconectar")
            print("   reconnect              → Reconectar")
            print("   status/help            → Estado/menú")
            print("   quit                   → Salir")
            
        elif self.state == 'WAITING_RESPONSE':
            print("⏳ ESPERANDO RESPUESTA del Router...")
            print("📋 Puedes: status, disconnect, quit")
        
    def connect(self, host, port=None):
        """Conecta al Router usando socket TCP"""
        if port:
            self.router_port = int(port)
        
        self.router_host = host

        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.router_host, self.router_port))
            print("Conexión establecida con Router")
            self.ip, self.port = self.socket.getsockname()

            message_dict = MessageProtocol.create_message(
                msg_type=MessageProtocol.MESSAGE_TYPES['IDENTIFICATION'],
                sender_id=self.ip,
                node_type=self.node_type,
                timestamp=datetime.now().isoformat(),
                data={'ip': self.ip, 'port': self.port, 'is_boss': False, 'is_temporally': True}
            )

            message_bytes = message_dict.encode()
            try:
                # enviar longitud
                lenght_bytes = len(message_bytes).to_bytes(2, 'big')
                self.socket.sendall(lenght_bytes)

                # enviar mensaje completo
                self.socket.sendall(message_bytes)

            except Exception as e:
                print(f"Error al enviar mensaje de tipo IDENTIFICACION a {host}: {e}")

            print(f"ip: {self.socket.getsockname()}")
            print("RouterClient listo. Escribe 'help' para opciones.\n")
            self.state = 'CONNECTED'
            self._show_status()
            return True
        except Exception as e:
            print(f"Error conectando al Router: {e}")
            self.state = 'DISCONNECTED'
            return False
    
    def disconnect(self):
        """Desconecta limpiamente"""
        if self.socket:
            self.socket.close()
            self.socket = None
        
        self.state = 'DISCONNECTED'
        print("\n👋 Conexión cerrada")
        self._show_status()
    
    def generate_task_id(self):
        """Genera un ID único para la tarea"""
        return str(uuid.uuid4())[:8]
    
    def send_request(self, url):
        """Envía petición CLIENT_REQUEST al Router"""
        task_id = self.generate_task_id()
        
        # Crear mensaje usando MessageProtocol
        message = MessageProtocol.create_message(
            msg_type=MessageProtocol.MESSAGE_TYPES['CLIENT_REQUEST'],
            sender_id=f"client-{self.router_host}",
            node_type="client",
            data={
                'task_id': task_id,
                'url': url,
                'timestamp': datetime.now().isoformat()
            }
        )
        
        try:
            # Serializar y enviar (protocolo: 2 bytes longitud + JSON)
            message_bytes = json.dumps(message).encode('utf-8')
            length_bytes = len(message_bytes).to_bytes(2, 'big')
            
            self.socket.sendall(length_bytes)
            self.socket.sendall(message_bytes)
            
            print(f"Petición enviada: task_id={task_id}, url={url}")
            self.state = 'WAITING_RESPONSE'
            print(f"Esperando respuesta... (mantén la conexión abierta)")

            
            return task_id
            
        except Exception as e:
            print(f"Error enviando petición: {e}")
            return None
    

    def receive_response(self, timeout=30):
        """Recibe y procesa respuesta del Router"""
        self.socket.settimeout(timeout)
        try:
            # Leer longitud (2 bytes)
            length_bytes = self.socket.recv(2)
            if not length_bytes:
                print("Conexión cerrada por Router")
                return None
                
            length = int.from_bytes(length_bytes, 'big')
            
            # Leer mensaje completo
            response_bytes = b''
            while len(response_bytes) < length:
                chunk = self.socket.recv(min(1024, length - len(response_bytes)))
                if not chunk:
                    break
                response_bytes += chunk
            
            response = json.loads(response_bytes.decode('utf-8'))
            self._handle_response(response)
            self.state = 'CONNECTED'
            return response
            
        except socket.timeout:
            print("Timeout esperando respuesta")
            self.state = 'CONNECTED'
            return None
        except Exception as e:
            print(f"Error recibiendo respuesta: {e}")
            self.state = 'CONNECTED'
        finally:
            self._show_status()

    
    def _handle_response(self, response):
        """Procesa diferentes tipos de respuesta"""
        msg_type = response.get('type')
        data = response.get('data', {})
        task_id = data.get('task_id')
        
        print(f"\nRespuesta recibida:")
        print(f"   Tipo: {msg_type}")
        print(f"   Task ID: {task_id}")
        
        if msg_type == MessageProtocol.MESSAGE_TYPES['TASK_RESULT']:
            success = data.get('success', False)
            result = data.get('result', {})
            
            if success:
                print("Scrapping exitoso!")
                if isinstance(result, dict):
                    print(f"Datos: {json.dumps(result, indent=2)[:500]}...")
                else:
                    print(f"Resultado: {result[:500]}...")
            else:
                print(f"Error: {result.get('error', 'Desconocido')}")
    
    def show_menu(self):
        """Muestra menú interactivo"""
        print("\n" + "="*60)
        print("CLIENTE ROUTER - URLs de prueba disponibles:")
        print("="*60)
        for i, url in enumerate(self.TEST_URLS, 1):
            print(f"{i:2d}. {url}")
        print("\nOpciones adicionales:")
        print("  'h' o 'help'   - Mostrar este menú")
        print("  'q' o 'quit'   - Salir")
        print("  'urls'         - Listar URLs")
        print("  URL personal   - Enviar URL personalizada")
        print("="*60)
    
    def run(self):
        """Loop principal interactivo"""
        
        
        while self.running:
            self.show_menu()
            try:
                cmd = input("\nSelecciona opción (1-8, h=help, q=quit): ").strip()
                
                if cmd.lower() in ['q', 'quit', 'exit']:
                    break
                elif cmd.lower() in ['h', 'help']:
                    self.show_menu()
                elif cmd.lower() == 'urls':
                    self.show_urls()
                elif cmd.startswith('connect '):
                    parts = cmd.split()
                    if len(parts) >= 2:
                        host = parts[1]
                        port = parts[2] if len(parts) > 2 else None
                        self.connect(host, port)
                    else:
                        print("Uso: connect <IP> [puerto]")
                elif cmd.lower() == 'disconnect':
                    self.disconnect()
                elif cmd.lower() == 'reconnect':
                    if self.router_host:
                        self.connect(self.router_host, self.router_port)
                    else:
                        print("Especifica IP primero: connect <IP>")
                elif self.state == 'CONNECTED':
                    if cmd.isdigit() and 1 <= int(cmd) <= len(self.TEST_URLS):
                        url = self.TEST_URLS[int(cmd) - 1]
                        self.send_request(url)
                        if self.current_task_id:
                            self.receive_response()
                    elif cmd.startswith('url'):
                        url = cmd[4:].strip()
                        if url.startswith('http'):
                            self.send_request(url)
                            self.receive_response()
                        else:
                            print("URL debe empezar con http")
                    else:
                        print("URL inválida o opción no reconocida")

                elif self.state == 'WAITING_RESPONSE':
                    if cmd.lower() in ['status', 'disconnect', 'quit']:
                        pass
                    else:
                        print("⏳ Esperando respuesta... (usa status/disconnect/quit)")
                else:
                    print("❌ Debes conectarte primero: connect <IP>")                
            except KeyboardInterrupt:
                print("\n Saliendo...")
                break
            except Exception as e:
                print(f" Error: {e}")
        
        self.disconnect()

    def show_urls(self):
        """Lista URLs disponibles"""
        print("\n🌐 URLs de prueba:")
        for i, url in enumerate(self.TEST_URLS, 1):
            print(f"   {i:2d}. {url}")

def main():
    """Función principal"""
    print("Iniciando Router Client")
    print("Úsalo con: python router_client.py [host] [port]")
    
    client = RouterClient()
    try:
        client.run()
    except KeyboardInterrupt:
        print("\n Cliente terminado")

if __name__ == "__main__":
    main()
