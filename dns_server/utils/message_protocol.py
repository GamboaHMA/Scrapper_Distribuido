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
        
        # DATABASE COMMUNICATION
        'SAVE_DATA': 'save_data',
        'SAVE_CONFIRMATION': 'save_confirmation',

        # OTHERS
        'LEADER_QUERY_TO_OTHER_BOSS': 'leader_query_to_other_boss',
        'LEADER_RESPONSE_TO_OTHER_BOSS': 'leader_response_to_other_boss',
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