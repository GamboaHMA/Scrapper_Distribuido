"""
Módulo para manejar el scraping de páginas web respetando robots.txt y límites de velocidad.
"""

import time
import logging
import urllib.parse
from urllib.robotparser import RobotFileParser
import re
import requests
from bs4 import BeautifulSoup

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
            response_code = getattr(e.response, 'status_code', None)
            if response_code in [403, 404, 410]:
                # No reintentar para estos códigos
                raise Exception(f"URL no accesible (Código {response_code}): {url}")
            else:
                status_code = response_code if response_code else "desconocido"
                logging.warning(f"Error HTTP {status_code} al descargar {url}, intento {attempt+1}/{MAX_RETRIES}")
                
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