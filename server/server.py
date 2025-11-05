import socket
import threading
import json
import time
import os
from datetime import datetime
import logging
import struct

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('server.log'),
        logging.StreamHandler()
    ]
)

class CentralServer():
    def __init__(self, host='0.0.0.0', port=8080, broadcast_port=8081) -> None:
        self.host = host
        self.port = port
        self.broadcast_port = broadcast_port
        self.clients = {}
        self.tasks = {}
        self.task_id_counter = 1
        self.log_entries = []
        self.running = False
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(10)
        
        # Cola de tareas pendientes cuando no hay clientes disponibles
        self.pending_tasks = []
        self.clients_lock = threading.Lock()
        # Socket para broadcast
        self.broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Permitir broadcast en este socket
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    
    def receive_messages(self, client_socket):
        # recibe longitud primero
        lenght_bytes = client_socket.recv(2)
        if not lenght_bytes:
            return False

        lenght =  int.from_bytes(lenght_bytes, 'big') 
        # recibe mensaje completo
        data = b""
        while(len(data) < lenght):
            len_data = len(data)
            chunk = client_socket.recv(min(1024, lenght - len_data))
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
            with self.clients_lock:
                self.clients[client_id] = {
                    'socket': client_socket,
                    'address': client_address,
                    'connected_at': datetime.now().isoformat(),
                    'status': 'connected',
                    'client_status': 'unknown',
                    'is_busy': False
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
                # recibe datos enviados por cliente
                message = self.receive_messages(client_socket)
                if not message:
                    break

                try:
                    self.process_client_message(client_id, message)
                
                except json.JSONDecodeError:
                    self.log_event('ERROR', 'Mensaje JSON invalido', client_id)

        except Exception as e:
            self.log_event('ERROR', f"Error en comunicacion :{str(e)}", client_id)
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
            self.log_event('PING', "Recibiendo ping", client_id)

        elif msg_type == 'command':
            if message.get('data')[0] == 'assign':
                client_id = message.get('client_id')
                url = message.get('data')[2]
                self.assign_task_to_available_client({'url': url})
            elif message.get('data')[0] == 'status':
                print(self.get_system_status())
            elif message.get('data')[0] == 'clients':
                print('Clientes conectados:\n')
                for client in self.clients:
                    print(client + '\n')
            elif message.get('data')[0] == 'tasks':
                # Nuevo comando para obtener las tareas y sus resultados
                tasks_info = {}
                for task_id, task in self.tasks.items():
                    tasks_info[task_id] = {
                        'id': task['id'],
                        'client_id': task.get('client_id'),
                        'data': task.get('data'),
                        'status': task.get('status'),
                        'assigned_at': task.get('assigned_at'),
                        'completed_at': task.get('completed_at'),
                        'result': task.get('result')
                    }
                
                # Enviar respuesta de vuelta al cliente API
                response_msg = {
                    'type': 'tasks_response',
                    'tasks': tasks_info,
                    'timestamp': datetime.now().isoformat()
                }
                
                try:
                    response_json = json.dumps(response_msg).encode()
                    length = len(response_json)
                    self.clients[client_id]['socket'].send(length.to_bytes(2, 'big'))
                    self.clients[client_id]['socket'].send(response_json)
                except Exception as e:
                    self.log_event('ERROR', f'Error enviando respuesta de tareas: {e}', client_id)

        elif msg_type == 'task_result':
            task_id = message.get('task_id')
            result = message.get('result')
            self.log_event('TASK_RESULT', f"Tarea {task_id} completada: {result}", client_id)

            if task_id in self.tasks:
                self.tasks[task_id]['status'] = 'completed'
                self.tasks[task_id]['result'] = result
                self.tasks[task_id]['completed_at'] = datetime.now().isoformat()
            
            # Marcar cliente como disponible inmediatamente
            with self.clients_lock:
                if client_id in self.clients:
                    self.clients[client_id]['client_status'] = 'available'
                    self.clients[client_id]['is_busy'] = False
                    if 'assigned_task' in self.clients[client_id]:
                        del self.clients[client_id]['assigned_task']
                    self.log_event('STATUS_AUTO', f"Cliente marcado como disponible tras completar tarea {task_id}", client_id)

        elif msg_type == 'task_accepted':
            task_id = message.get('task_id')
            self.log_event('TASK_ACCEPTED', f"Tarea {task_id} aceptada", client_id)
            
            # Si el cliente no estaba marcado como ocupado, marcarlo ahora
            with self.clients_lock:
                if client_id in self.clients:
                    self.clients[client_id]['is_busy'] = True
                    self.clients[client_id]['client_status'] = 'busy'
                    self.clients[client_id]['assigned_task'] = task_id
            
        elif msg_type == 'task_rejected':
            task_id = message.get('task_id')
            reason = message.get('reason')
            self.log_event('TASK_REJECTED', f"Tarea {task_id} rechazada. Razón: {reason}", client_id)
            
            # Si la tarea fue rechazada, establecer como pendiente
            if task_id in self.tasks:
                self.tasks[task_id]['status'] = 'pending'

        elif msg_type == 'status':
            status = message.get('status')
            is_busy = message.get('is_busy', False)
            self.log_event('STATUS', f"Estado: {status}, Ocupado: {is_busy}", client_id)
            
            with self.clients_lock:
                if client_id in self.clients:
                    self.clients[client_id]['client_status'] = status
                    self.clients[client_id]['is_busy'] = is_busy

    def get_available_clients(self):
        '''Obtener la lista de clientes disponibles'''
        available_clients = []
        with self.clients_lock:
            for client_id, client_info in self.clients.items():
                if not client_info.get('is_busy', True) and client_info.get('client_status') == 'available':
                    available_clients.append(client_id)
        return available_clients

    def process_pending_tasks(self):
        '''Procesa tareas pendientes de forma simple y directa'''
        # Buscar tareas con status 'pending' en el diccionario principal
        pending_task_ids = [tid for tid, task in self.tasks.items() if task['status'] == 'pending']
        
        if not pending_task_ids:
            return
        
        # Buscar clientes disponibles
        available_clients = self.get_available_clients()
        
        if not available_clients:
            return
        
        # Asignar una tarea por vez a clientes disponibles
        for task_id in pending_task_ids:
            for client_id in available_clients:
                task = self.tasks[task_id]
                
                # Intentar asignar la tarea
                success = self.assign_tasks(client_id, task['data'], task_id)
                
                if success:
                    # Actualizar status de la tarea
                    self.tasks[task_id]['status'] = 'assigned'
                    self.tasks[task_id]['client_id'] = client_id
                    self.tasks[task_id]['assigned_at'] = datetime.now().isoformat()
                    # self.log_event('TASK_ASSIGNED', f"Tarea {task_id} asignada a cliente {client_id}", "SYSTEM")
                    
                    # Remover el cliente de la lista de disponibles
                    available_clients.remove(client_id)
                    break  # Salir del bucle de clientes para pasar a la siguiente tarea
            # Si no hay más clientes disponibles, parar
            if not available_clients:
                break

    def assign_task_to_available_client(self, task_data, task_id=None):
        '''Asigna una tarea a un cliente disponible o la almacena como pendiente'''
        
        if not task_id:
            # Si no se proporciona un ID de tarea, crear uno nuevo
            task_id = self.task_id_counter
            self.task_id_counter += 1
        
        # Crear la tarea y almacenarla en el diccionario principal
        task = {
            'id': task_id,
            'data': task_data,
            'status': 'pending',  # Siempre comienza como pendiente
            'created_at': datetime.now().isoformat(),
            'client_id': None,
            'assigned_at': None
        }
        
        self.tasks[task_id] = task
        self.log_event('TASK_CREATED', f"Tarea {task_id} creada y almacenada como pendiente", "SYSTEM")
        
        return task_id  # Devolver el ID de la tarea creada
    
    def assign_tasks(self, client_id, task_data, task_id=None):
        '''asigna una tarea a un cliente especifico'''
        if client_id not in self.clients:
            return False
        
        with self.clients_lock:
            # Verificar si el cliente sigue disponible
            if self.clients[client_id].get('is_busy', True):
                self.log_event('ERROR', f"Cliente {client_id} está ocupado, no se puede asignar tarea", client_id)
                return False
        
        # Si no hay task_id, crear uno nuevo
        if task_id is None:
            task_id = self.task_id_counter
            self.task_id_counter += 1
            # Crear nueva tarea
            task = {
                'id': task_id,
                'client_id': client_id,
                'data': task_data,
                'assigned_at': datetime.now().isoformat(),
                'status': 'assigned',
                'created_at': datetime.now().isoformat()
            }
            self.tasks[task_id] = task
        else:
            # Actualizar tarea existente
            if task_id in self.tasks:
                self.tasks[task_id]['client_id'] = client_id
                self.tasks[task_id]['status'] = 'assigned'
                self.tasks[task_id]['assigned_at'] = datetime.now().isoformat()
            else:
                # La tarea no existe, crear una nueva
                task = {
                    'id': task_id,
                    'client_id': client_id,
                    'data': task_data,
                    'assigned_at': datetime.now().isoformat(),
                    'status': 'assigned',
                    'created_at': datetime.now().isoformat()
                }
                self.tasks[task_id] = task

        # envia la info de la tarea al cliente
        task_msg = {
            'type': 'task',
            'task_id': task_id,
            'task_data': task_data
        }

        try:
            self.clients[client_id]['socket'].send(json.dumps(task_msg).encode()) ##
            
            # Marcar cliente como ocupado inmediatamente después de enviar la tarea
            with self.clients_lock:
                self.clients[client_id]['is_busy'] = True
                self.clients[client_id]['client_status'] = 'busy'
                self.clients[client_id]['assigned_task'] = task_id
            
            self.log_event('TASK_ASSIGNED', f"Tarea {task_id} asignada: {task_data}", client_id)
                
            return True
        except ConnectionError:
            self.log_event('ERROR', 'No se pudo enviar la tarea al cliente', client_id)
            return False
        
        
        
    def get_system_status(self):
        '''obtiene el estado del sistema actualmente'''
        available_clients = self.get_available_clients()
        busy_clients = [c for c in self.clients.keys() if c not in available_clients]
        
        return {
            'total_clients': len(self.clients),
            'available_clients': available_clients,
            'busy_clients': busy_clients,
            'total_tasks': len(self.tasks),
            'pending_tasks': len([t for t in self.tasks.values() if t['status'] == 'pending']),
            'completed_tasks': len([t for t in self.tasks.values() if t['status'] == 'completed']),
            'assigned_tasks': len([t for t in self.tasks.values() if t['status'] == 'assigned'])
        }

    def inspect_client(self, client):
        return self.clients[client]
    
    def send_broadcast_signal(self):
        '''Envia una señal de broadcast para que los clientes detecten al servidor'''
        while self.running:
            try:
                broadcast_info = {
                    'type': 'server_discovery',
                    'server_host': self.host,
                    'server_port': self.port,
                    'time': datetime.now().isoformat()
                }
                broadcast_message = json.dumps(broadcast_info).encode()
                
                # Enviar a la dirección de broadcast en la red Docker (normalmente 255.255.255.255)
                # Pero en Docker Swarm, envía a la dirección de broadcast de la red overlay
                self.broadcast_socket.sendto(broadcast_message, ('255.255.255.255', self.broadcast_port))
                self.log_event('BROADCAST', 'Señal de descubrimiento enviada')
                
                # Enviar señal cada 5 segundos
                time.sleep(5)
            except Exception as e:
                self.log_event('ERROR', f"Error en envío de broadcast: {e}")
                time.sleep(5)
    
    def start_socket_server(self):
        '''inicia el servidor de sockets'''
        self.running = True

        self.log_event('SERVER_START', f"Servidor iniciado en {self.host}:{self.port}")

        try:
            # Iniciar hilo para aceptar conexiones de clientes
            accept_thread = threading.Thread(target=self.accepts_conections)
            accept_thread.daemon = True
            accept_thread.start()
            
            # Iniciar hilo para enviar señales de broadcast
            broadcast_thread = threading.Thread(target=self.send_broadcast_signal)
            broadcast_thread.daemon = True
            broadcast_thread.start()

            # BUCLE PRINCIPAL DEL SERVIDOR
            while self.running:
                # 1. Procesar comandos de interfaz (opcional)
                # self.command_interface()
                
                # 2. Procesar tareas pendientes automáticamente
                self.process_pending_tasks()
                
                # 3. Pequeña pausa para no saturar el CPU
                time.sleep(0.1)

        except KeyboardInterrupt:
            self.log_event('SERVER_STOP', 'Servidor detenido')
        finally:
            self.server_socket.close()
            self.broadcast_socket.close()

    def accepts_conections(self):
        '''hilo para conexiones de clientes'''
        while self.running:
            try:

                # timeout para verificar si esta corriendo periodicamente
                self.server_socket.settimeout(1.0)
                client_socket, client_address = self.server_socket.accept()
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, client_address)
                )
                client_thread.daemon = True
                client_thread.start()

                

            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    self.log_event('ERORR', f"Error en comunicacion: {e}")

    # def command_interface(self):
        # '''hilo para la interaccion con el usuario'''
        # help_text = '''Comandos disponibles: 
        # - "status": estado actual del sistema
        # - "clients": lista de clientes conectados
        # - "client IP": info sobre el cliente
        # - "available": lista de clientes disponibles
        # - "pending": muestra tareas pendientes en cola
        # - "scrape URL": asigna URL al primer cliente disponible
        # - "assign CLIENT URL": asigna URL a un cliente específico
        # - "exit": cerrar servidor
        # - "help": mostrar esta ayuda\n
        # '''

        # while self.running:
        #     try:
        #         print("\n>>> ", end='', flush=True)
        #         user_input = input().strip().lower()

        #         if user_input == 'status':
        #             print(self.get_system_status())
        #         elif user_input == 'clients':
        #             print('Clientes conectados:\n')
        #             for client_id, client_info in self.clients.items():
        #                 status = "OCUPADO" if client_info.get('is_busy', False) else "DISPONIBLE"
        #                 print(f"{client_id} - Estado: {status} - {client_info.get('client_status', 'unknown')}\n")
        #         elif user_input == 'available':
        #             available = self.get_available_clients()
        #             print(f'Clientes disponibles ({len(available)}):\n')
        #             for client in available:
        #                 print(client + '\n')
        #         elif user_input == 'pending':
        #             print(f'Tareas pendientes ({len(self.pending_tasks)}):\n')
        #             for i, task in enumerate(self.pending_tasks):
        #                 print(f"{i+1}. Tarea {task.get('task_id')} - {task.get('task_data')}\n")
        #         elif user_input == 'help':
        #             print(help_text)
        #         elif user_input == 'exit':
        #             self.running = False
        #             try:
        #                 self.server_socket.close()
        #                 self.log_event('SERVER_STOP', 'Servidor detenido')
        #             except Exception as e:
        #                 print(f"Error al cerrar el servidor: {e}")
        #         elif user_input.startswith('scrape '):
        #             url = user_input[7:].strip()
        #             print(f"Asignando tarea para URL: {url}")
        #             if self.assign_task_to_available_client({'url': url}):
        #                 print("Tarea asignada exitosamente")
        #             else:
        #                 print("Tarea puesta en cola")
        #         elif len(user_input.split()) >= 2:
        #             parts = user_input.split()
        #             if parts[0] == 'client':
        #                 print(self.inspect_client(parts[1]))
        #             elif parts[0] == 'assign' and len(parts) >= 3:
        #                 client_id = parts[1]
        #                 url = parts[2]
        #                 if self.assign_tasks(client_id, {'url': url}):
        #                     print(f"Tarea asignada a cliente {client_id}")
        #                 else:
        #                     print(f"No se pudo asignar tarea al cliente {client_id}")
        #         elif user_input:
        #             print(f"Comando no reconocido: {user_input}")
            
        #     except KeyboardInterrupt:
        #         pass
        #     except Exception as e:
        #         print(f"Error en interfaz de comandos: {e}")
        # pass

                

if __name__ == "__main__":
    # Obtener la configuración desde variables de entorno
    host = '0.0.0.0'
    port = int(os.environ.get('SERVER_PORT', 8080))
    broadcast_port = int(os.environ.get('BROADCAST_PORT', 8081))
    
    server = CentralServer(host=host, port=port, broadcast_port=broadcast_port)

    # iniciar servidor 
    server.start_socket_server()