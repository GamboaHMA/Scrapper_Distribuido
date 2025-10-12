#!/usr/bin/env python3
"""
Script para descargar al menos 1000 páginas web y guardarlas localmente
junto con sus URLs.
"""

import os
import re
import json
import time
import random
import logging
import threading
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# Configuración del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)

# Configuración global
MAX_PAGES = 50  # Objetivo
HTML_DIR = "../extracted_pages/html"
URL_FILE = "../extracted_pages/urls.json"
VISITED_FILE = "../extracted_pages/visited_urls.json"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
}
DELAY = 1  # Segundos de espera entre solicitudes al mismo dominio
MAX_RETRIES = 3
TIMEOUT = 10
MAX_THREADS = 10  # Número de hilos para descargas paralelas

# Semillas (URLs iniciales para comenzar la exploración)
SEED_URLS = [
    "https://es.wikipedia.org/wiki/Wikipedia:Portada",
    "https://www.python.org/",
    "https://www.un.org/es/",
    "https://www.gutenberg.org/",
    "https://www.nationalgeographic.com/",
    "https://www.nasa.gov/",
    "https://arxiv.org/",
    "https://stackoverflow.com/",
    "https://developer.mozilla.org/es/",
]

# Estructura para almacenar información de dominio (para respetar robots.txt y delays)
domain_info = {}
domain_lock = threading.Lock()
url_queue = Queue()
visited_urls = set()
downloaded_pages = 0
download_lock = threading.Lock()

# Crear directorios necesarios
os.makedirs(HTML_DIR, exist_ok=True)
os.makedirs(os.path.dirname(URL_FILE), exist_ok=True)

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
        
    # Rechazar URLs que contienen parámetros de sesión o tracking (opcional)
    if re.search(r'(sessionid|jsessionid|sid|session_id)', url.lower()):
        return False
        
    return True

def get_robots_parser(domain):
    """Obtiene y parsea el archivo robots.txt de un dominio."""
    with domain_lock:
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
        return robots_parser.can_fetch(HEADERS['User-Agent'], url)
    except Exception as e:
        logging.error(f"Error verificando robots.txt para {url}: {e}")
        return True  # En caso de duda, permitir

def respect_rate_limits(domain):
    """Respeta los límites de velocidad para un dominio específico."""
    with domain_lock:
        if domain not in domain_info:
            domain_info[domain] = {'last_access': 0}
            
        # Calcular tiempo a esperar
        elapsed = time.time() - domain_info[domain]['last_access']
        if elapsed < DELAY:
            time.sleep(DELAY - elapsed)
            
        # Actualizar tiempo de último acceso
        domain_info[domain]['last_access'] = time.time()

def download_page(url):
    """Descarga una página web si no ha sido visitada antes."""
    global downloaded_pages
    
    if url in visited_urls:
        return None
    
    # Marcar como visitada inmediatamente para evitar duplicados durante la ejecución paralela
    with download_lock:
        if url in visited_urls:  # Verificar nuevamente con el lock adquirido
            return None
        visited_urls.add(url)
    
    parsed_url = urllib.parse.urlparse(url)
    domain = parsed_url.netloc
    
    # Verificar robots.txt
    if not respect_robots_txt(url):
        logging.info(f"Ignorando {url} (no permitido por robots.txt)")
        return None
    
    # Respetar límites de velocidad
    respect_rate_limits(domain)
    
    # Intentar descarga con reintentos
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            
            if response.status_code == 200:
                # Verificar si el contenido es HTML
                content_type = response.headers.get('Content-Type', '').lower()
                if 'text/html' not in content_type:
                    logging.info(f"Ignorando {url} (no es HTML, es {content_type})")
                    return None
                
                # Salvar la página
                with download_lock:
                    # Generar un nombre de archivo único basado en el número de páginas descargadas
                    page_number = downloaded_pages
                    downloaded_pages += 1
                    
                    if downloaded_pages >= MAX_PAGES:
                        return url, response.text, page_number
                
                return url, response.text, page_number
            
            elif response.status_code in [403, 404, 410]:
                # No reintentar para estos códigos
                logging.info(f"URL no accesible {url} (Código {response.status_code})")
                return None
            
            else:
                logging.warning(f"Error al descargar {url} (Código {response.status_code}), intento {attempt+1}/{MAX_RETRIES}")
                
        except Exception as e:
            logging.warning(f"Error al descargar {url}, intento {attempt+1}/{MAX_RETRIES}: {e}")
            
        # Esperar antes de reintentar (con backoff exponencial)
        if attempt < MAX_RETRIES - 1:
            time.sleep(2 ** attempt)
    
    return None

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
    
    # Limitar el número de enlaces para evitar sobrecargar un solo dominio
    random.shuffle(links)  # Aleatorizar para mayor diversidad
    return links[:25]  # Limitar a 25 enlaces por página

def save_html(html_content, filename):
    """Guarda el contenido HTML en un archivo."""
    filepath = os.path.join(HTML_DIR, filename)
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
        return True
    except Exception as e:
        logging.error(f"Error al guardar el HTML en {filepath}: {e}")
        return False

def save_urls_json(urls_dict):
    """Guarda el diccionario de URLs en formato JSON."""
    try:
        with open(URL_FILE, 'w', encoding='utf-8') as f:
            json.dump(urls_dict, f, indent=2)
        return True
    except Exception as e:
        logging.error(f"Error al guardar el JSON de URLs: {e}")
        return False

def save_visited_urls():
    """Guarda las URLs visitadas para futuras ejecuciones."""
    try:
        with open(VISITED_FILE, 'w', encoding='utf-8') as f:
            json.dump(list(visited_urls), f, indent=2)
        return True
    except Exception as e:
        logging.error(f"Error al guardar las URLs visitadas: {e}")
        return False

def load_visited_urls():
    """Carga las URLs previamente visitadas."""
    global visited_urls
    try:
        if os.path.exists(VISITED_FILE):
            with open(VISITED_FILE, 'r', encoding='utf-8') as f:
                visited_urls = set(json.load(f))
            logging.info(f"Cargadas {len(visited_urls)} URLs visitadas previamente")
    except Exception as e:
        logging.error(f"Error al cargar las URLs visitadas: {e}")

def worker():
    """Función de trabajo para los hilos."""
    while True:
        url = url_queue.get()
        if url is None:  # Señal de terminación
            url_queue.task_done()
            break
            
        result = download_page(url)
        url_queue.task_done()
        
        if result:
            url, html, page_number = result
            
            # Guardar HTML
            filename = f"page_{page_number}.html"
            save_html(html, filename)
            
            # Añadir a la lista de resultados
            with download_lock:
                urls_mapping[url] = filename
                
            # Extraer y encolar nuevos enlaces
            new_links = extract_links(html, url)
            for link in new_links:
                if link not in visited_urls:
                    url_queue.put(link)

def main():
    """Función principal de ejecución."""
    global urls_mapping
    urls_mapping = {}
    
    # Cargar URLs visitadas previamente
    load_visited_urls()
    
    # Poner las semillas en la cola
    for seed in SEED_URLS:
        if seed not in visited_urls:
            url_queue.put(seed)
    
    # Iniciar trabajadores
    threads = []
    for _ in range(MAX_THREADS):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)
    
    # Monitoreo y espera
    with tqdm(total=MAX_PAGES, desc="Descargando páginas") as pbar:
        previous_count = 0
        while downloaded_pages < MAX_PAGES:
            time.sleep(1)
            current_count = downloaded_pages
            pbar.update(current_count - previous_count)
            previous_count = current_count
            
            # Si la cola está vacía pero no hemos alcanzado la meta, añadir más semillas
            if url_queue.empty() and downloaded_pages < MAX_PAGES:
                logging.info("Cola vacía, añadiendo más semillas...")
                # Podríamos añadir más semillas aquí o tomar URLs aleatorias ya visitadas
                # y explorar sus enlaces
                for seed in SEED_URLS:
                    url_queue.put(seed)
    
    # Señal de terminación para todos los hilos
    for _ in range(MAX_THREADS):
        url_queue.put(None)
    
    # Esperar a que todos los hilos terminen
    for t in threads:
        t.join()
    
    # Guardar URLs en formato JSON
    save_urls_json(urls_mapping)
    save_visited_urls()
    
    logging.info(f"Proceso completado. Se descargaron {len(urls_mapping)} páginas HTML únicas.")

if __name__ == "__main__":
    main()
