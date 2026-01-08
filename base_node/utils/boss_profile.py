"""
Boss Profile - Perfil para manejar conexiones con jefes (externos o del mismo tipo)
"""
import threading


class BossProfile:
    """
    Perfil que encapsula toda la información de una conexión con un jefe.
    
    Puede ser usado para:
    - Conexiones con jefes externos (ej: Router -> BD, Router -> Scrapper)
    - Conexiones con jefes del mismo tipo (ej: Scrapper subordinado -> Scrapper jefe)
    - Cualquier conexión persistente con un nodo jefe
    """
    
    def __init__(self, node_type, port):
        """
        Inicializa el perfil de un jefe.
        
        Args:
            node_type: Tipo de nodo ('bd', 'scrapper', 'router', etc.)
            port: Puerto del servicio
        """
        self.node_type = node_type
        self.port = port
        self.connection = None  # NodeConnection con el jefe
        self.available = False  # Estado de disponibilidad
        self.lock = threading.Lock()  # Lock para operaciones thread-safe
    
    def is_connected(self):
        """
        Verifica si hay una conexión activa con el jefe.
        
        Returns:
            bool: True si hay conexión activa, False en caso contrario
        """
        return self.available and self.connection and self.connection.is_connected()
    
    def set_connection(self, connection):
        """
        Establece la conexión con el jefe y marca como disponible.
        NOTA: Este método NO es thread-safe. El llamador debe adquirir self.lock antes de llamarlo.
        
        Args:
            connection: NodeConnection con el jefe
        """
        self.connection = connection
        self.available = True
    
    def clear_connection(self):
        """
        Limpia la conexión con el jefe y marca como no disponible.
        NOTA: Este método NO es thread-safe. El llamador debe adquirir self.lock antes de llamarlo.
        """
        self.connection = None
        self.available = False
    
    def get_connection(self):
        """
        Obtiene la conexión actual de forma thread-safe.
        
        Returns:
            NodeConnection o None
        """
        with self.lock:
            return self.connection
    
    def __repr__(self):
        """Representación string del perfil"""
        status = "conectado" if self.is_connected() else "desconectado"
        return f"BossProfile({self.node_type}:{self.port}, {status})"
