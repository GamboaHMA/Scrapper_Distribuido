import socket
import json
import time
import threading
import random
import logging
import os
from datetime import datetime
import urllib.parse
from urllib.robotparser import RobotFileParser
import re
import requests
from bs4 import BeautifulSoup


# config del logging (igual que en el server)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuración global para scraping
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
}
DELAY = 1  # Segundos de espera entre solicitudes al mismo dominio
MAX_RETRIES = 3
TIMEOUT = 10

# Diccionario para almacenar información de dominio (robots.txt y último acceso)
domain_info = {}

# Funciones para scraping
def get_robots_parser(domain):
    """Obtiene y parsea el archivo robots.txt de un dominio."""
    if domain not in domain_info:
        domain_info[domain] = {
            'last_access': 0,
            'robots_parser': RobotFileParser()
        }
        
        # Configurar el parser de robots.txt
        robots_url = f"https://{domain}/robots.txt"
        try:
            domain_info[domain]['robots_parser'].set_url(robots_url)
            domain_info[domain]['robots_parser'].read()
            logging.info(f"Robots.txt leído para {domain}")
        except Exception as e:
            logging.warning(f"Error al leer robots.txt para {domain}: {e}")
            # Si hay un error, configuramos un parser vacío que no restringe nada
            domain_info[domain]['robots_parser'] = RobotFileParser()
            
    return domain_info[domain]['robots_parser']

def respect_robots_txt(url):
    """Verifica si una URL está permitida según el robots.txt del sitio."""
    try:
        parsed_url = urllib.parse.urlparse(url)
        domain = parsed_url.netloc
        
        robots_parser = get_robots_parser(domain)
        can_fetch = robots_parser.can_fetch(HEADERS['User-Agent'], url)
        
        if not can_fetch:
            logging.info(f"URL {url} no permitida por robots.txt")
            
        return can_fetch
    except Exception as e:
        logging.error(f"Error verificando robots.txt para {url}: {e}")
        return True  # En caso de duda, permitir

def respect_rate_limits(domain):
    """Respeta los límites de velocidad para un dominio específico."""
    if domain not in domain_info:
        domain_info[domain] = {'last_access': 0}
        
    # Calcular tiempo a esperar
    elapsed = time.time() - domain_info[domain]['last_access']
    if elapsed < DELAY:
        wait_time = DELAY - elapsed
        logging.debug(f"Esperando {wait_time:.2f}s para respetar rate limit de {domain}")
        time.sleep(wait_time)
        
    # Actualizar tiempo de último acceso
    domain_info[domain]['last_access'] = time.time()

def is_valid_url(url):
    """Verifica si una URL es válida y cumple con ciertos criterios."""
    # Rechazar URLs no HTTP/HTTPS
    if not url.startswith(('http://', 'https://')):
        return False
        
    # Rechazar ciertos formatos de archivo
    extensions = ['.pdf', '.jpg', '.jpeg', '.png', '.gif', '.css', '.js', 
                  '.zip', '.tar', '.gz', '.mp3', '.mp4', '.avi', '.mov']
    if any(url.lower().endswith(ext) for ext in extensions):
        return False
        
    # Rechazar URLs que contienen parámetros de sesión o tracking
    if re.search(r'(sessionid|jsessionid|sid|session_id)', url.lower()):
        return False
        
    return True

def extract_links(html, base_url):
    """Extrae enlaces de una página HTML."""
    links = []
    try:
        soup = BeautifulSoup(html, 'html.parser')
        for a_tag in soup.find_all('a', href=True):
            href = str(a_tag['href'])
            # Resolver URLs relativas
            full_url = urllib.parse.urljoin(base_url, href)
            if is_valid_url(full_url):
                links.append(full_url)
    except Exception as e:
        logging.error(f"Error al extraer enlaces de {base_url}: {e}")
    
    return links

def get_html_from_url(url):
    """
    Recibe una URL como string y devuelve el HTML completo de esa página respetando robots.txt
    y límites de velocidad.
    
    Args:
        url (str): La URL de la página web a descargar
        
    Returns:
        dict: Diccionario con el contenido HTML de la página, la URL y los enlaces encontrados
        
    Raises:
        Exception: Si ocurre un error durante la descarga
    """
    if not is_valid_url(url):
        raise Exception(f"URL no válida: {url}")
        
    # Verificar robots.txt
    if not respect_robots_txt(url):
        raise Exception(f"URL no permitida por robots.txt: {url}")
    
    # Respetar límites de velocidad
    parsed_url = urllib.parse.urlparse(url)
    domain = parsed_url.netloc
    respect_rate_limits(domain)
    
    # Intentar descarga con reintentos
    for attempt in range(MAX_RETRIES):
        try:
            # Establecer headers
            logging.info(f"Descargando URL: {url}")
            
            # Realizar la petición GET a la URL
            response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            
            # Verificar si la petición fue exitosa
            response.raise_for_status()
            
            # Verificar si el contenido es HTML
            content_type = response.headers.get('Content-Type', '').lower()
            if 'text/html' not in content_type:
                raise Exception(f"El contenido no es HTML: {content_type}")
            
            # Extraer enlaces
            html_content = response.text
            links = extract_links(html_content, url)
            
            # Devolver el contenido y los enlaces
            return {
                'url': url,
                'html': html_content,
                'links': links
            }
        
        except requests.exceptions.HTTPError as e:
            if response.status_code in [403, 404, 410]:
                # No reintentar para estos códigos
                raise Exception(f"URL no accesible (Código {response.status_code}): {url}")
            else:
                logging.warning(f"Error HTTP {response.status_code} al descargar {url}, intento {attempt+1}/{MAX_RETRIES}")
                
        except requests.exceptions.RequestException as e:
            logging.warning(f"Error al descargar URL: {url}, intento {attempt+1}/{MAX_RETRIES}. Error: {str(e)}")
        
        except Exception as e:
            logging.warning(f"Error inesperado al procesar URL: {url}, intento {attempt+1}/{MAX_RETRIES}. Error: {str(e)}")
        
        # Esperar antes de reintentar (con backoff exponencial)
        if attempt < MAX_RETRIES - 1:
            wait_time = 2 ** attempt
            logging.info(f"Esperando {wait_time}s antes de reintentar...")
            time.sleep(wait_time)
    
    raise Exception(f"No se pudo descargar la URL después de {MAX_RETRIES} intentos: {url}")

class ClientNode():
    def __init__(self, server_host='0.0.0.0', server_port=8080) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.client_id = None
        self.connected = False
        self.socket = None
        self.current_task = {}
        self.send_lock = threading.Lock()

    def connect_to_server(self):
        '''conecta al servidor central'''
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.server_host, self.server_port))
            self.connected = True
            logging.info(f"Conectado al servidor {self.server_host}:{self.server_port}")

            # recibe mensaje de welcome de server
            welcome_data = self.socket.recv(1024).decode()
            welcome_msg = json.loads(welcome_data)
            self.client_id = welcome_msg.get('client_id')
            logging.info(f"ID asignado: {self.client_id}")

            return True

        except Exception as e:
            logging.error(f"Error conectando aal servidor: {e}")
            return False
    
    def send_heartbeat(self):
        '''envia senial periodica al server'''
        while(self.connected):
            try:
                heartbeat_msg = {
                    'type': 'heartbeat',
                    'time_now': datetime.now().isoformat()
                }
                message = json.dumps(heartbeat_msg).encode()

                with self.send_lock:
                    lenght = len(message)
                    self.socket.send(lenght.to_bytes(4, 'big'))
                    self.socket.send(message)

                time.sleep(5) # latido cada 5 segundos
            
            except Exception:
                self.connected = False

    def send_status(self, status):
        '''envia estado al servidor'''
        try:
            status_msg = {
                'type': 'status',
                'status': status,
                'time_now': datetime.now().isoformat()
            }
            message = json.dumps(status_msg).encode()
            with self.send_lock:
                lenght = len(message)
                self.socket.send(lenght.to_bytes(4, 'big'))
                self.socket.send(message)
        
        except Exception:
            self.connected = False
    
    def execute_task(self, task_id, task_data):
        '''ejecuta la tarea asignada'''
        logging.info(f"Ejecutando tarea {task_id}: {task_data}")
        
        result = None
        try:
            # Verificar que task_data tiene formato adecuado
            if not isinstance(task_data, dict) or 'url' not in task_data:
                raise Exception("Formato de tarea inválido, se espera diccionario con campo 'url'")
                
            url = task_data['url']
            logging.info(f"Iniciando scraping para URL: {url}")
            
            # Realizar el scraping
            scrape_result = get_html_from_url(url)
            
            # Preparar resultado para enviar al servidor
            result = {
                'url': scrape_result['url'],
                'html_length': len(scrape_result['html']),
                'links_count': len(scrape_result['links']),
                'links': scrape_result['links'][:10],  # Enviar solo los primeros 10 enlaces
                'status': 'success'
            }
            
            logging.info(f"Scraping exitoso: {url}, encontrados {len(scrape_result['links'])} enlaces")
            
        except Exception as e:
            error_msg = str(e)
            logging.error(f"Error en el scraping: {error_msg}")
            
            # En caso de error, enviar información de error
            result = {
                'status': 'error',
                'error': error_msg
            }

        # Preparar mensaje de resultado para el servidor
        result_msg = {
            'type': 'task_result',
            'task_id': task_id,
            'result': result,
            'completed_at': datetime.now().isoformat()
        }

        try:
            self.socket.send(json.dumps(result_msg).encode())
            logging.info(f"Tarea {task_id} completada y enviada al servidor")
        
        except Exception as e:
            logging.error(f"Error al enviar resultados al servidor: {e}")
            self.connected = False
        
    def listen_for_tasks(self):
        '''escuchando tareas del servidor'''
        while self.connected:
            try:
                data = self.socket.recv(1024).decode()
                if not data:
                    self.connected = False
                    break
                
                message = json.loads(data)
                self.proccess_server_message(message)
            
            except json.JSONDecodeError:
                logging.error("Mensaje JSON invalido del servidor")
            except Exception as e:
                logging.error(f"Error recibiendo datos: {e}")
                self.connected = False
                break
        
    def proccess_server_message(self, message):
        '''procesa mensaje del servidor'''
        msg_type = message.get('type')

        if msg_type == 'task':
            # nueva tarea asignada
            task_id = message.get('task_id')
            task_data = message.get('task_data')

            logging.info(f"Nueva tarea recibida: {task_id}")

            # ejecuta la tarea en un hilo separado
            task_thread = threading.Thread(
                target=self.execute_task,
                args=(task_id, task_data)
            )
            task_thread.daemon = True
            task_thread.start()

            self.current_task[task_id] = {
                'data': task_data,
                'started_at': datetime.now().isoformat(),
                'status': 'executing'
            }

    def start(self):
        '''inicia al cliente'''
        if not self.connect_to_server():
            return
        
        # inicia hilos de seniales
        heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        heartbeat_thread.daemon = True
        heartbeat_thread.start()

        # envia el estado inicial
        self.send_status('Conectado y listo')

        self.listen_for_tasks()



if __name__ == "__main__":
    # Obtener la configuración del servidor desde variables de entorno
    server_host = os.environ.get('SERVER_HOST', '0.0.0.0')
    server_port = int(os.environ.get('SERVER_PORT', 8080))
    
    logging.info(f"Intentando conectar al servidor {server_host}:{server_port}")
    
    # Crear el cliente con la configuración del servidor
    client = ClientNode(server_host=server_host, server_port=server_port)

    # Intenta reconectar si se pierde la conexion
    while(True):
        client.start()
        if not client.connected:
            logging.info("Intentando reconectar en 10 seg ...")
            time.sleep(10)