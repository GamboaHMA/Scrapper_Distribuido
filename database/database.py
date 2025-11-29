import socket
import time
import json
import threading
import random
import logging
import os
from datetime import datetime
import struct
import queue

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class DatabaseService:
    def __init__(self, host='0.0.0.0', port=8080):
        self.host = host
        self.port = port
        self.conexiones_activas = {}  # key:(ip, puerto), value: socket
        
    def get_host_info(self):
        """Obtiene información del host para debugging"""
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
        logging.info(f"ip: {ip}")
        return hostname, ip
    
    def conexion_valida(self, destino):
        return destino not in self.conexiones_activas
    
    def conectar(self, destino):
        '''Intenta conectar al destino si no hay conexion activa'''
        if not self.conexion_valida(destino):
            logging.info(f"Desde {self.get_host_info()[0]} ya existe conexion activa a {destino}, evitando reconexion...")
            return self.conexiones_activas[destino]
        
        try:
            sock = socket.create_connection(destino, timeout=5)
            self.conexiones_activas[destino] = sock
            logging.info(f"Conectado a {destino}")
            return sock
        except socket.timeout:
            logging.error(f"Timeout al conectar con {destino}")
            return None
        except Exception as e:
            logging.error(f"Error al conectar con {destino}: {e}")

    def cerrar_conexion(self, destino):
        '''Cierra la conexion activa al destino en caso de que exista.'''
        if destino in self.conexiones_activas:
            self.conexiones_activas[destino].close()
            del self.conexiones_activas[destino]
            logging.info(f"Conexion a {destino} cerrada")
    


    
    def handle_client(self, client_socket, address):
        """Maneja las conexiones de clientes"""
        try:
            logging.info(f"Conexión establecida desde {address}")
            
            # Simular procesamiento de base de datos
            while True:
                # Recibir datos del cliente
                data = client_socket.recv(1024)
                if not data:
                    break
                    
                # Procesar la solicitud (ejemplo simple)
                request = data.decode('utf-8').strip()
                logging.info(f"Recibido: {request}")
                
                # Simular respuesta de base de datos
                response = {
                    "status": "success",
                    "timestamp": datetime.now().isoformat(),
                    "data": f"Procesado: {request}",
                    "server_info": {
                        "hostname": socket.gethostname(),
                        "service": "database"
                    }
                }
                
                # Enviar respuesta
                client_socket.send(json.dumps(response).encode('utf-8'))
                
        except Exception as e:
            logging.error(f"Error manejando cliente {address}: {e}")
        finally:
            client_socket.close()
            logging.info(f"Conexión cerrada con {address}")
    
    def start_server(self):
        """Inicia el servidor de base de datos"""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            server_socket.bind((self.host, self.port))
            server_socket.listen(5)
            self.running = True
            
            logging.info(f"Servicio de database iniciado en {self.host}:{self.port}")
            hostname, ip = self.get_host_info()
            logging.info(f"Hostname: {hostname}, IP: {ip}")
            
            while self.running:
                logging.info("Base de datos esperando conexiones...")
                
                client_socket, address = server_socket.accept()
                
                # Manejar cada cliente en un hilo separado
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, address)
                )
                client_thread.daemon = True
                client_thread.start()
                
        except Exception as e:
            logging.error(f"Error del servidor: {e}")
        finally:
            server_socket.close()

def main():
    # Usar variables de entorno para configuración
    host = os.getenv('DB_HOST', '0.0.0.0')
    port = int(os.getenv('DB_PORT', '8080'))
    
    db_service = DatabaseService(host, port)
    db_service.start_server()

if __name__ == "__main__":
    main()