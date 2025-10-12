# web-mock/app.py
from flask import Flask, send_file, abort, request
import os
import json
from urllib.parse import urlparse, unquote

app = Flask(__name__)

# Cargar el mapeo de URLs desde tu archivo JSON
def load_url_mapping():
    with open('/app/urls.json', 'r') as f:
        return json.load(f)

# Cargar el mapeo al iniciar
URL_MAPPING = load_url_mapping()
print(f"✅ Cargado mapeo con {len(URL_MAPPING)} URLs")

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve_html(path):
    """
    Sirve archivos HTML basados en la URL solicitada.
    Usa el mapeo exacto del JSON.
    """
    try:
        # Reconstruir la URL completa que el worker está pidiendo
        full_url = request.url.replace('http://web-mock/', 'https://')
        
        # Buscar en el mapeo - probamos diferentes formatos
        possible_keys = [
            full_url,
            full_url.rstrip('/'),
            unquote(full_url),  # Por si hay URLs codificadas
        ]
        
        html_filename = None
        for key in possible_keys:
            if key in URL_MAPPING:
                html_filename = URL_MAPPING[key]
                break
        
        if html_filename:
            html_filepath = os.path.join('/app/html', html_filename)
            
            if os.path.exists(html_filepath):
                print(f"✅ Sirviendo {html_filename} para {full_url}")
                return send_file(html_filepath)
            else:
                print(f"❌ Archivo no encontrado: {html_filepath}")
                return serve_default_page(f"Archivo no encontrado: {html_filename}")
        else:
            print(f"❌ URL no mapeada: {full_url}")
            return serve_default_page(f"URL no encontrada: {full_url}")
            
    except Exception as e:
        print(f"❌ Error sirviendo {path}: {e}")
        return serve_default_page(f"Error: {str(e)}")

def serve_default_page(message):
    """Sirve una página por defecto cuando no se encuentra la URL"""
    default_html = f"""
    <html>
        <head><title>Mock Internet - Página no encontrada</title></head>
        <body>
            <h1>Mock Internet</h1>
            <p>{message}</p>
            <p>Esta es una página por defecto del servidor mock.</p>
            <p>URLs disponibles: {len(URL_MAPPING)}</p>
        </body>
    </html>
    """
    return default_html, 404

@app.route('/health')
def health():
    """Endpoint de salud para verificar que el servidor funciona"""
    html_files = [f for f in os.listdir('/app/html') if f.endswith('.html')]
    return {
        'status': 'healthy',
        'mapped_urls': len(URL_MAPPING),
        'html_files': len(html_files),
        'uptime': 'running'
    }

@app.route('/urls')
def list_urls():
    """Endpoint para listar todas las URLs mapeadas (útil para debugging)"""
    return {
        'total_urls': len(URL_MAPPING),
        'urls_sample': dict(list(URL_MAPPING.items())[:10])  # Primero 10 para no saturar
    }

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=False)

