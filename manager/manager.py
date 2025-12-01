import socket
import threading
import json
import time
import os
from datetime import datetime
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class ManagerServer:
    def __init__(self, host='0.0.0.0', port=8080) -> None:
        self.host = host
        self.port = port
        self.ip_cache = {}
        self.clients = {}
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        logging.info(f"ManagerServer listening on {self.host}:{self.port}")
        
    def start(self):