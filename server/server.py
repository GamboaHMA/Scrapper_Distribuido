import socket
import threading
import json
import time
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('server.log'),
        logging.StreamHandler()
    ]
)

class CentralServer():
    def __init__(self, host='0.0.0.0', port=8080) -> None:
        self.host = host
        self.port = port
        self.clients = {}
        self.tasks = {}
        self.task_id_counter = 1
        self.log_entries = []
    
    def receive_messages(self, client_socket):
        # recibe longitud primero
        lenght_bytes = client_socket.recv(4)
        if not lenght_bytes:
            return False

        lenght =  int.from_bytes(lenght_bytes, 'big')
        # recibe mensaje completo
        data = b""
        while(len(data) < lenght):
            chunk = client_socket.recv(min(1024, lenght - len(data)))
            if not chunk:
                break
            data += chunk
                
        if len(data) == lenght:
            message = json.loads(data.decode())
            return message
            

    def log_event(self, event_type, message, client_id=None):
        '''Registrar eventos'''
        time_now = datetime.now().isoformat()
        log_entry = {
            'time_now': time_now,
            'type': event_type,
            'message': message,
            'client_id': client_id
        }
        self.log_entries.append(log_entry)
        logging.info(f"[{event_type}] Client {client_id}: {message}" if client_id else f"[{event_type}] {message}")

    def handle_client(self, client_socket, client_address):
        '''Comunicacion con un cliente conectado'''
        client_id = f"{client_address[0]}:{client_address[1]}"

        try:
            # registra nueva conexion
            self.clients[client_id] = {
                'socket': client_socket,
                'address': client_address,
                'connected_at': datetime.now().isoformat(),
                'status': 'connected'
            }
            self.log_event('CONNECTION', "Cliente conectado", client_id)

            # envia ID al cliente 
            welcome_msg = {
                'type': 'welcome',
                'client_id': client_id,
                'message': 'Conectado al servidor central'
            } 
            client_socket.send(json.dumps(welcome_msg).encode())

            while(True):
                # recibe datos eviados por cliente
                message = self.receive_messages(client_socket)
                if not message:
                    break

                try:
                    self.process_client_message(client_id, message)
                
                except json.JSONDecodeError:
                    self.log_event('ERROR', 'Mensaje JSON invalido', client_id)

        except Exception as e:
            self.log_event('ERROR', f'Error en comunicacion :{str(e)}', client_id)
        finally:
            # limpia cliente desconectado
            if client_id in self.clients:
                del self.clients[client_id]
            client_socket.close()
            self.log_event('DISCONNECTION', 'Cliente desconectado', client_id)

    def process_client_message(self, client_id, message):
        '''procesa los mensajes recibidos de los clientes'''
        msg_type = message.get('type')

        if msg_type == 'heartbeat':
            self.clients[client_id]['last_heartbeat'] = datetime.now().isoformat()
            self.clients[client_id]['status'] = 'active'
        
        elif msg_type == 'take_result':
            task_id = message.get('task_id')
            result = message.get('result')
            self.log_event('TAKE_RESULT', f"Tarea {task_id} completada: {result}", client_id)

            if task_id in self.tasks:
                self.tasks[task_id]['status'] = 'completed'
                self.tasks[task_id]['result'] = result
                self.tasks[task_id]['completed_at'] = datetime.now().isoformat()

        elif msg_type == 'status':
            self.log_event('STATUS', f"Estado: {message.get('status')}", client_id)

    def assign_tasks(self, client_id, task_data):
        '''asigna una tarea a un cliente especifico'''
        if client_id not in self.clients:
            return False
        
        task_id = self.task_id_counter
        self.task_id_counter += 1

        task = {
            'id': task_id,
            'client_id': client_id,
            'data': task_data,
            'assigned_at': datetime.now().isoformat(),
            'status': 'assigned'
        }

        self.tasks[task_id] = task

        # envia la info de la tarea al cliente
        task_msg = {
            'type': 'task',
            'task_id': task_id,
            'task_data': task_data
        }

        try:
            self.clients[client_id]['socket'].send(json.dumps(task_msg).encode())
            self.log_event('TASK_ASSIGNED', f"Tarea {task_id} asignada: {task_data}", client_id)
            return True
        except ConnectionError:
            self.log_event('ERROR', 'No se pudo enviar la tarea al cliente', client_id)
            return False
        
    def get_system_status(self):
        '''obtiene el estado del sistema actualmente'''
        return {
            'total_clients': len(self.clients),
            'connected_clients': list(self.clients.keys()),
            'total_tasks': len(self.tasks),
            'completed_tasks': len([t for t in self.tasks.values() if t['status'] == 'completed']),
            'active_tasks': len([t for t in self.tasks.values() if t['status'] == 'assigned'])
        }
    
    def start_socket_server(self):
        '''inicia el servidor de sockets'''
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.host, self.port))
        server_socket.listen(10)

        self.log_event('SERVER_START', f"Servidor iniciado en {self.host}:{self.port}")

        try:
            while(True):
                client_socket, client_address = server_socket.accept()
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, client_address),
                )
                client_thread.daemon = True
                client_thread.start()
        except KeyboardInterrupt:
            self.log_event('SERVER_STOP', 'Servidor detenido')
        finally:
            server_socket.close()

if __name__ == "__main__":
    server = CentralServer()

    # iniciar servidor 
    server.start_socket_server() 
    print('asd')