#!/usr/bin/env python3
"""
Cliente para consultar el Registry/DNS
"""

import socket
import json
import sys


def query_registry(query_type, registry_host='localhost', registry_port=5353, **kwargs):
    """
    Consulta el registry
    
    Args:
        query_type: Tipo de consulta ('list_all', 'find_by_type', 'find_by_id')
        registry_host: Host del registry
        registry_port: Puerto del registry
        **kwargs: Parámetros adicionales según el tipo de consulta
    
    Returns:
        Dict con la respuesta del registry
    """
    try:
        # Crear consulta
        query = {
            'query_type': query_type,
            **kwargs
        }
        
        # Conectar al registry
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect((registry_host, registry_port))
        
        # Enviar consulta
        sock.send(json.dumps(query).encode() + b'\n')
        
        # Recibir respuesta
        response = sock.recv(8192).decode().strip()
        sock.close()
        
        return json.loads(response)
        
    except Exception as e:
        return {'error': str(e)}


def main():
    if len(sys.argv) < 2:
        print("Uso:")
        print("  python3 registry_client.py list                     # Listar todos los servicios")
        print("  python3 registry_client.py type <tipo>               # Buscar por tipo")
        print("  python3 registry_client.py id <service_id>           # Buscar por ID")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == 'list':
        result = query_registry('list_all')
    elif command == 'type' and len(sys.argv) >= 3:
        service_type = sys.argv[2]
        result = query_registry('find_by_type', service_type=service_type)
    elif command == 'id' and len(sys.argv) >= 3:
        service_id = sys.argv[2]
        result = query_registry('find_by_id', service_id=service_id)
    else:
        print("Comando inválido")
        sys.exit(1)
    
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
