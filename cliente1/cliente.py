#!/usr/bin/env python3
"""
Router Client - Cliente interactivo para enviar peticiones al RouterNode
Se ejecuta desde terminal y permite seleccionar URLs predefinidas para scrapping
"""

import sys
import os

# Agregar directorio padre al path para imports absolutos (mismo que usan los nodos)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import socket
import json
import threading
import time
from base_node.utils import MessageProtocol
from datetime import datetime

URLS = [
    "https://httpbin.org/json",          # JSON testing API
    "https://quotes.toscrape.com",       # Quotes para BeautifulSoup
    "http://quotes.toscrape.com/scroll", # Paginación infinita
    "https://httpbin.org/html",          # HTML de prueba
    "https://jsonplaceholder.typicode.com/posts/1",  # Fake REST API
    "https://example.com",               # HTML básico
    "https://httpbin.org/get"           # Headers y params
]

class Client:
    def __init__(self, router_host, router_port, host='0.0.0.0.', port=5050):
        self.router_host = router_host
        self.router_port = router_port
        self.sock = None
        self.listen_sock = None
        self.task_id_counter = 0
        self.cache = []
        self.ip = None
        self.port = port
        self.node_type = 'client'

        
    def initial_connect(self):
        """Conecta al router"""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #self.sock.settimeout(3)
        try:
            self.sock.connect((self.router_host, self.router_port))
            self.ip, port = self.sock.getsockname()
            peer_ip, peer_port = self.sock.getpeername()
            self.cache.append((peer_ip, peer_port))
            print(f"✓ Conectado a {self.router_host}:{self.router_port}")
            print(f"peer_ip: {peer_ip} peer_port: {peer_port}")
        except Exception as e:
            print(f"Error conectando al router en {self.router_host}:{self.router_port}: {e}")
            return

        ident_message = MessageProtocol.create_message(
            MessageProtocol.MESSAGE_TYPES['IDENTIFICATION'],
            sender_id=self.ip,
            node_type=self.node_type,
            data={'is_temporary': True},
            timestamp=datetime.now().isoformat()
        )

        try:    
            msg_bytes = ident_message.encode('utf-8')
            length = len(msg_bytes).to_bytes(2, 'big')
            self.sock.send(length)
            self.sock.send(msg_bytes)
            
            #self.test_mess()
        except Exception as e:
            print(f"Error enviando mensaje de identificación a {peer_ip}: {e}")

        try:
            lenght_bytes = self.sock.recv(2)
            if len(lenght_bytes) < 2:
                print("no response")
                return
            msg_lenght = int.from_bytes(lenght_bytes, 'big')

            msg_bytes = b''
            while lenght_bytes


    def test_mess(self):
        msg = MessageProtocol.create_message(
            MessageProtocol.MESSAGE_TYPES['IDENTIFICATION'],
            sender_id=self.ip,
            node_type=self.node_type,
            data={},
            timestamp=datetime.now().isoformat()
        )
        try:
            msg_bytes = msg.encode()
            lenght = len(msg_bytes).to_bytes(2, 'big')
            self.sock.send(lenght)
            self.sock.send(msg_bytes)
        except Exception as e:
            print(f"Error enviando mensaje de prueba de {self.sock.getsockname()} a {self.sock.getpeername()}: {e}")
            
        
    def send_subordinates_request(self, cache=False):
        """Envía petición GET_SUBORDINATES al router"""

        # inicialmente vemos a ver si el router que se tenia registrado como lider se mantiene
        if not cache:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.timeout = 3
            try:
                print("esperando conectar al router...")
                self.socket.connect((self.router_host, self.router_port))

            except socket.timeout as e:
                self.send_subordinates_request(True)

            message = MessageProtocol.create_message(
                MessageProtocol.MESSAGE_TYPES['GET_SUBORDINATES'],
                sender_id=self.ip,
                node_type=self.node_type,
                data={},
                timestamp=datetime.now().isoformat()
            )

            try:
                message_bytes = message.encode('utf-8')
                length = len(message_bytes).to_bytes(2, 'big')
                self.socket.sendall(length)
                self.socket.sendall(message_bytes)
                print(" Enviado petición de subordinados")
                print("esperando respuesta...")

                try:
                    lenght_bytes = self.socket.recv(2)
                    if len(lenght_bytes) < 2:
                        print("no response")
                        return None
                    
                    msg_length = int.from_bytes(lenght_bytes, 'big')

                    #leer msg completo
                    msg_bytes = b''
                    while len(msg_bytes) < msg_length:
                        chunk = self.socket.recv(msg_length - len(msg_bytes))
                        if not chunk:
                            break
                        msg_bytes += chunk

                    response = json.loads(msg_bytes.decode('utf-8'))
                    print(f"Subordinados recibidos: {response.get('data')}")
                    subs = response.get('data').get('subordinates', [])

                    for sub in subs:
                        if sub not in self.cache:
                            self.cache.append(sub)

                except socket.timeout as e:
                    print("Tiempo de espera agotado esperando respuesta del router")
                    return None

                except Exception as e:
                    print(f"Error recibiendo respuesta del router: {e}")
                    return None

            
            except TimeoutError as e:
                print("Tiempo de espera agotado esperando respuesta del router")
                self.send_subordinates_request(True)
            except Exception as e:
                print(f"Error enviando petición de subordinados al router: {e}")
                return
            
        # en caso de que el lider haya cambiado
        else:
            if not self.cache:
                print("No hay routers en cache")
                return

            else:
                while(True):
                    msg_leader_search = MessageProtocol.create_message(
                        MessageProtocol.MESSAGE_TYPES['GET_ROUTER_LEADER'],
                        sender_id=self.ip,
                        node_type=self.node_type,
                        data={'expect_response': True, 'port': self.port},
                        timestamp=datetime.now().isoformat()
                    )

                    self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.socket.timeout = 3
                    #for router_ip in self.cache:

        
        # Enviar con longitud primero (formato del router)
        message_bytes = message.encode()
        length = len(message_bytes).to_bytes(2, 'big')
        self.sock.sendall(length)
        self.s
        print(" Enviado petición de subordinados")

    def send_request(self, url):
        """Envía petición CLIENT_REQUEST al router"""
        self.task_id_counter += 1
        task_id = f"task_{self.task_id_counter}_{int(time.time())}"
        
        message = MessageProtocol.create_message(
            MessageProtocol.MESSAGE_TYPES['CLIENT_REQUEST'],
            "client_local",
            "client",
            {
                'task_id': task_id,
                'url': url
            }
        )
        
        # Enviar con longitud primero (formato del router)
        length = len(message).to_bytes(2, 'big')
        self.sock.send(length + message.encode('utf-8'))
        print(f" Enviado task_id={task_id}, url={url}")
        
    def interactive_loop(self):
        """Loop interactivo en terminal"""
        self.initial_connect()
        
        print("\n Cliente conectado. Comandos:")
        print("  url <dirección>  → Enviar URL")
        print("  exit             → Salir\n")
        
        while True:
            try:
                cmd = input("> ").strip()
                if cmd.lower() == 'exit':
                    break
                elif cmd.lower() == 'reconnect':
                    self.initial_connect()
                elif cmd.startswith('url '):
                    url = int(cmd[4:].strip())
                    if url:
                        self.send_request(URLS[url])
                    else:
                        print("URL requerida")
                elif cmd.lower().startswith('subs'):
                    pass
                else:
                    print("Comando inválido")
                    
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Error: {e}")
        
        self.sock.close()
        print("Desconectado")

if __name__ == "__main__":
    
    parts = input("> teclee el ip y el puerto del primer router: ").split()
    client = Client(parts[0], int(parts[1]))  # o la IP del contenedor
    client.interactive_loop()