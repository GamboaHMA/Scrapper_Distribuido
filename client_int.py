import socket
import json
import time
import threading
import random
import os
from datetime import datetime
import struct

def interact():
    server_host = '0.0.0.0'
    server_port = 8080
    socket_ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        socket_.connect((server_host, server_port))

        welcome_data = socket_.recv(1024).decode()
        welcome_msg = json.loads(welcome_data)
        client_id = welcome_msg.get('client_id')

        while(True):
            try:
                print("\n>>> ", end='', flush=True)
                user_input = input().lower()
                parts = user_input.split()
                if len(parts) > 1:
                    interact_msg = {
                        'type': 'command',
                        'client_id': parts[1],
                        'data': parts,
                        'time_now': datetime.now().isoformat()
                    }
                else:
                    interact_msg = {
                        'type': 'command',
                        'data': parts,
                        'time_now': datetime.now().isoformat()

                    }
                message = json.dumps(interact_msg).encode()
                lentgh_ = len(message)
                
                socket_.send(lentgh_.to_bytes(2, 'big'))
                socket_.send(message)
            
            except Exception as e:
                print(f"Error enviando mensaje: {e}")


    except Exception as e:
        print(f"no se pudo comunicar con el servidor: {e}")

if __name__=="__main__":
    while(True):
        n = 2
        interact()
        print(f"intentando conectar, de nuevo en {n} segundos")
        time.sleep(n)
        
