#!/usr/bin/env python3
"""
Cliente de prueba para el servicio de scraping.
Este script muestra cómo otro nodo se conectaría al worker.
"""

import requests
import json
import sys

def test_worker_service(url, worker_url="http://localhost:5000"):
    """
    Prueba el servicio de scraping enviando una URL al worker.
    
    Args:
        url (str): La URL a scrapear
        worker_url (str): La URL del servicio de scraping
    """
    print(f"Enviando solicitud de scraping para: {url}")
    
    try:
        # Realizar petición al endpoint de scraping
        response = requests.post(
            f"{worker_url}/scrape",
            json={"url": url},
            timeout=30
        )
        
        # Verificar si la petición fue exitosa
        if response.status_code == 200:
            data = response.json()
            
            # Mostrar información sobre los datos recibidos
            print("\nRespuesta exitosa!")
            print(f"URL: {data['url']}")
            print(f"Enlaces encontrados: {len(data['links'])}")
            print(f"Tamaño del HTML: {len(data['html'])} caracteres")
            
            # Mostrar los primeros 5 enlaces encontrados
            if data['links']:
                print("\nPrimeros 5 enlaces encontrados:")
                for i, link in enumerate(data['links'][:5]):
                    print(f"  {i+1}. {link}")
                
            return data
            
        else:
            print(f"\nError en la petición: Código {response.status_code}")
            print(f"Detalle: {response.json()}")
            return None
            
    except Exception as e:
        print(f"\nError al conectar con el servicio: {str(e)}")
        return None

if __name__ == "__main__":
    # Usar URL de línea de comandos o una predeterminada
    url = sys.argv[1] if len(sys.argv) > 1 else "https://www.python.org/"
    test_worker_service(url)