import time
import json

class MessageProtocol:
    # tipos de mensaje
    MESSAGE_TYPES = {
        'DISCOVERY': 'discovery',
        'LEADER_QUERY': 'leader_query',
        'LEADER_RESPONSE': 'leader_response',
        'LEADER_ANNOUNCE': 'leader_announce',
        'ELECTION': 'election',
        'ANSWER': 'answer',
        'COORDINATOR': 'coordinator',
        'HEARTBEAT': 'heartbeat',
        'HEARTBEAT_RESPONSE': 'heartbeat_response',
        'NODE_LIST': 'node_list',
        'JOIN_NETWORK': 'join_network',
        'LEAVE_NETWORK': 'leave_network',
        
        # SCRAPPER SECTION
        'IDENTIFICATION': 'identification',
        'TASK_ASSIGNMENT': 'task_assignment',
        'TASK_RESULT': 'task_result',
        'STATUS_UPDATE': 'status_update',
        'TASK_ACCEPTED': 'task_accepted',
        'TASK_REJECTION': 'task_rejection',
        'ELECTION_RESPONSE': 'election_response',
        'NEW_BOSS': 'new_boss',
        
        # TASK MANAGEMENT (Router <-> Scrapper)
        'NEW_TASK': 'new_task',
        'TASK_COMPLETED': 'task_completed',
        
        # ROUTER SECTION (Router <-> Clientes/BD/Scrapper)
        'CLIENT_REQUEST': 'client_request',
        'BD_QUERY': 'bd_query',
        'BD_QUERY_RESPONSE': 'bd_query_response',
        'URL_QUERY': 'url_query',  # Query de BD no-líder
        'SCRAPPER_RESULT': 'scrapper_result',
        'STATUS_REQUEST': 'status_request',
        'STATUS_RESPONSE': 'status_response',
        
        # REPLICATION (Jefe -> Subordinados)
        'EXTERNAL_BOSSES_INFO': 'external_bosses_info',
        
        # EXTERNAL BOSS COORDINATION (Nuevo jefe -> Jefe externo)
        'NEW_EXTERNAL_BOSS': 'new_external_boss',
        
        # DATABASE COMMUNICATION
        'SAVE_DATA': 'save_data',
        'SAVE_DATA_NO_LEADER': 'save_data_no_leader',
        'SAVE_CONFIRMATION': 'save_confirmation'
    }

    @staticmethod
    def create_message(msg_type, sender_id, node_type, data=None, timestamp=None):
        """Crea un mensaje JSON estandarizado"""
        message = {
            'type': msg_type,
            'sender_id': sender_id,
            'node_type': node_type,
            'timestamp': timestamp or time.time(),
            'data': data or {}
        }
        return json.dumps(message)
    
    @staticmethod
    def parse_message(json_str):
        """Parsea un mensaje JSON"""
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            return None