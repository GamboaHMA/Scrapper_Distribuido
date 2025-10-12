import requests

def get_html_from_url(url):
    """
    Recibe una URL como string y devuelve el HTML completo de esa página.
    
    Args:
        url (str): La URL de la página web a descargar
        
    Returns:
        str: El contenido HTML de la página web
        
    Raises:
        Exception: Si ocurre un error durante la descarga
    """
    try:
        # Establecer un User-Agent para evitar ser bloqueado por algunos sitios
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Realizar la petición GET a la URL
        response = requests.get(url, headers=headers, timeout=10)
        
        # Verificar si la petición fue exitosa (código de estado 200)
        response.raise_for_status()
        
        # Devolver el contenido de la respuesta
        return response.text
    
    except requests.exceptions.RequestException as e:
        # Capturar y relanzar excepciones específicas de requests
        raise Exception(f"Error al descargar la URL: {url}. Error: {str(e)}")
    except Exception as e:
        # Capturar cualquier otra excepción
        raise Exception(f"Error inesperado al procesar la URL: {url}. Error: {str(e)}")
    
if __name__ == "__main__":
    try:
        html_content = get_html_from_url("https://docs.docker.com/desktop/release-notes")
        print(html_content)  # o haz lo que necesites con el HTML
    except Exception as e:
        print(e)