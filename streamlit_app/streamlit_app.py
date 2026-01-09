import streamlit as st
import sys
import os
import time
import threading
from datetime import datetime
import uuid
import logging
import queue

# Agregar paths necesarios
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from base_node.utils import NodeConnection, MessageProtocol

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuración de la página
st.set_page_config(
    page_title="Scrapper Distribuido",
    page_icon="🕷️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilos personalizados
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        color: #1f77b4;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .success-box {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        border-radius: 0.25rem;
        padding: 1rem;
        margin: 1rem 0;
    }
    .error-box {
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        border-radius: 0.25rem;
        padding: 1rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Estado global de la aplicación
if 'connection' not in st.session_state:
    st.session_state.connection = None
if 'connected' not in st.session_state:
    st.session_state.connected = False
if 'results' not in st.session_state:
    st.session_state.results = []
if 'pending_tasks' not in st.session_state:
    st.session_state.pending_tasks = {}
if 'stats' not in st.session_state:
    st.session_state.stats = {
        'total_tasks': 0,
        'successful': 0,
        'failed': 0,
        'pending': 0
    }
if 'message_queue' not in st.session_state:
    st.session_state.message_queue = queue.Queue()

# Referencia global a la cola (para acceso desde threads)
message_queue = st.session_state.message_queue


class StreamlitClient:
    """Cliente que se conecta al router y maneja mensajes"""
    
    def __init__(self, router_ip, router_port=7070):
        self.router_ip = router_ip
        self.router_port = router_port
        self.connection = None
        self.client_id = f"streamlit-{uuid.uuid4().hex[:8]}"
        
    def connect(self):
        """Conecta con el router"""
        try:
            self.connection = NodeConnection(
                node_type='router',
                ip=self.router_ip,
                port=self.router_port,
                on_message_callback=self._handle_message,
                sender_node_type='client',
                sender_id=self.client_id
            )
            
            if self.connection.connect():
                # Enviar identificación
                identification_msg = {
                    'type': MessageProtocol.MESSAGE_TYPES['IDENTIFICATION'],
                    'sender_id': self.client_id,
                    'node_type': 'client',
                    'data': {
                        'is_boss': False,
                        'is_temporary': False,
                        'port': 0
                    }
                }
                
                if self.connection.send_message(identification_msg):
                    return True
            return False
        except Exception as e:
            logging.error(f"Error conectando: {e}")
            return False
    
    def disconnect(self):
        """Desconecta del router"""
        if self.connection:
            self.connection.disconnect()
            self.connection = None
    
    def send_scraping_task(self, url):
        """Envía una tarea de scraping"""
        if not self.connection or not self.connection.is_connected():
            logging.error("No hay conexión para enviar tarea")
            return None
        
        task_id = str(uuid.uuid4())
        
        task_msg = {
            'type': MessageProtocol.MESSAGE_TYPES['CLIENT_REQUEST'],
            'sender_id': self.client_id,
            'node_type': 'client',
            'data': {
                'task_id': task_id,
                'url': url,
                'timestamp': datetime.now().isoformat()
            }
        }
        
        logging.info(f"Enviando tarea {task_id} para URL: {url}")
        
        if self.connection.send_message(task_msg):
            # Agregar a tareas pendientes
            st.session_state.pending_tasks[task_id] = {
                'url': url,
                'status': 'pending',
                'timestamp': datetime.now().isoformat()
            }
            st.session_state.stats['pending'] += 1
            st.session_state.stats['total_tasks'] += 1
            logging.info(f"Tarea {task_id} agregada a pending_tasks")
            return task_id
        else:
            logging.error(f"Error enviando mensaje para tarea {task_id}")
        return None
    
    def _handle_message(self, node_connection, message):
        """Maneja mensajes recibidos del router"""
        try:
            msg_type = message.get('type')
            logging.info(f"Mensaje recibido: tipo={msg_type}")
            
            if msg_type == MessageProtocol.MESSAGE_TYPES['TASK_RESULT']:
                self._handle_task_result(message)
            elif msg_type == MessageProtocol.MESSAGE_TYPES['IDENTIFICATION']:
                logging.info("Confirmación de conexión recibida")
        except Exception as e:
            logging.error(f"Error procesando mensaje: {e}")
    
    def _handle_task_result(self, message):
        """Procesa resultados de tareas - ejecutado en thread separado"""
        try:
            data = message.get('data', {})
            task_id = data.get('task_id')
            success = data.get('success', False)
            result = data.get('result', {})
            
            logging.info(f"Resultado recibido para task {task_id}: success={success}")
            
            # Poner el resultado en la cola thread-safe GLOBAL
            result_data = {
                'task_id': task_id,
                'success': success,
                'result': result
            }
            message_queue.put(result_data)
            logging.info(f"Resultado agregado a message_queue")
            
        except Exception as e:
            logging.error(f"Error procesando resultado de tarea: {e}")


def process_pending_results():
    """Procesa resultados pendientes de la cola - ejecutado en thread principal"""
    try:
        queue_size = message_queue.qsize()
        if queue_size > 0:
            logging.info(f"Procesando {queue_size} resultados de la cola")
        
        while not message_queue.empty():
            result_data = message_queue.get_nowait()
            
            task_id = result_data['task_id']
            success = result_data['success']
            result = result_data['result']
            
            logging.info(f"Procesando resultado de task {task_id}, pending_tasks={list(st.session_state.pending_tasks.keys())}")
            
            if task_id in st.session_state.pending_tasks:
                task_info = st.session_state.pending_tasks[task_id]
                
                # Crear entrada de resultado
                result_entry = {
                    'task_id': task_id,
                    'url': task_info['url'],
                    'success': success,
                    'status': result.get('status', 'unknown'),
                    'content_length': result.get('html_length', 0),
                    'links_count': result.get('links_count', 0),
                    'links': result.get('links', []),
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'error': result.get('error', '')
                }
                
                # Agregar al inicio de la lista
                st.session_state.results.insert(0, result_entry)
                logging.info(f"✅ Resultado procesado y agregado! Total de resultados: {len(st.session_state.results)}")
                
                # Actualizar estadísticas
                del st.session_state.pending_tasks[task_id]
                st.session_state.stats['pending'] -= 1
                
                if success:
                    st.session_state.stats['successful'] += 1
                else:
                    st.session_state.stats['failed'] += 1
            else:
                logging.warning(f"❌ Task {task_id} no encontrado en pending_tasks. Tareas pendientes: {list(st.session_state.pending_tasks.keys())}")
    except queue.Empty:
        pass
    except Exception as e:
        logging.error(f"Error procesando resultados pendientes: {e}")
        import traceback
        traceback.print_exc()


def main():
    # Procesar resultados pendientes de la cola
    logging.info(f"main() ejecutándose - connected={st.session_state.get('connected', False)}, queue_size={message_queue.qsize()}")
    if st.session_state.connected:
        process_pending_results()
    else:
        logging.info("No procesando cola porque no está conectado")
    
    # Header
    st.markdown('<h1 class="main-header">🕷️ Sistema de Scraping Distribuido</h1>', unsafe_allow_html=True)
    
    # Sidebar - Configuración de conexión
    with st.sidebar:
        st.header("⚙️ Configuración")
        
        router_ip = st.text_input(
            "IP del Router",
            value=os.environ.get('ROUTER_IP', 'router-node'),
            help="Nombre del servicio o IP del router jefe"
        )
        
        router_port = st.number_input(
            "Puerto del Router",
            value=7070,
            min_value=1000,
            max_value=65535
        )
        
        st.divider()
        
        # Botón de conexión
        if not st.session_state.connected:
            if st.button("🔌 Conectar", use_container_width=True):
                with st.spinner("Conectando..."):
                    client = StreamlitClient(router_ip, router_port)
                    if client.connect():
                        st.session_state.connection = client
                        st.session_state.connected = True
                        st.success("✅ Conectado!")
                        st.rerun()
                    else:
                        st.error("❌ Error al conectar")
        else:
            st.success("🟢 Conectado")
            if st.button("🔌 Desconectar", use_container_width=True):
                if st.session_state.connection:
                    st.session_state.connection.disconnect()
                st.session_state.connection = None
                st.session_state.connected = False
                st.rerun()
        
        st.divider()
        
        # Información
        st.subheader("ℹ️ Información")
        if st.session_state.connected:
            st.write(f"**Cliente ID:** {st.session_state.connection.client_id}")
            st.write(f"**Router:** {router_ip}:{router_port}")
        else:
            st.info("Conecta al router para comenzar")
    
    # Contenido principal
    if not st.session_state.connected:
        st.info("👈 Conecta al router usando la barra lateral para comenzar")
        return
    
    # Métricas principales
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="📊 Total de Tareas",
            value=st.session_state.stats['total_tasks']
        )
    
    with col2:
        st.metric(
            label="✅ Exitosas",
            value=st.session_state.stats['successful'],
            delta=None
        )
    
    with col3:
        st.metric(
            label="❌ Fallidas",
            value=st.session_state.stats['failed'],
            delta=None
        )
    
    with col4:
        st.metric(
            label="⏳ Pendientes",
            value=st.session_state.stats['pending'],
            delta=None
        )
    
    st.divider()
    
    # Formulario para nueva tarea
    st.subheader("🚀 Nueva Tarea de Scraping")
    
    with st.form("scraping_form"):
        col1, col2 = st.columns([4, 1])
        
        with col1:
            url = st.text_input(
                "URL a scrapear",
                placeholder="https://ejemplo.com",
                help="Ingresa la URL completa del sitio web"
            )
        
        with col2:
            submit = st.form_submit_button("🕷️ Scrapear", use_container_width=True)
        
        if submit:
            if not url:
                st.error("Por favor ingresa una URL")
            elif not url.startswith(('http://', 'https://')):
                st.error("La URL debe comenzar con http:// o https://")
            else:
                task_id = st.session_state.connection.send_scraping_task(url)
                if task_id:
                    st.success(f"✅ Tarea enviada: {task_id[:8]}...")
                else:
                    st.error("❌ Error al enviar la tarea")
    
    st.divider()
    
    # Tareas pendientes
    if st.session_state.pending_tasks:
        with st.expander(f"⏳ Tareas Pendientes ({len(st.session_state.pending_tasks)})", expanded=True):
            for task_id, task_info in list(st.session_state.pending_tasks.items()):
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.write(f"🔗 **{task_info['url']}**")
                with col2:
                    st.write(f"*{task_info['timestamp'][:19]}*")
    
    # Resultados
    st.subheader("📋 Resultados")
    
    if not st.session_state.results:
        st.info("No hay resultados aún. Envía una tarea de scraping para comenzar.")
    else:
        # Filtros
        col1, col2 = st.columns([1, 3])
        with col1:
            filter_status = st.selectbox(
                "Filtrar por estado",
                ["Todos", "Exitosos", "Fallidos"]
            )
        
        # Filtrar resultados
        filtered_results = st.session_state.results
        if filter_status == "Exitosos":
            filtered_results = [r for r in st.session_state.results if r['success']]
        elif filter_status == "Fallidos":
            filtered_results = [r for r in st.session_state.results if not r['success']]
        
        # Mostrar resultados
        for result in filtered_results:
            with st.container():
                # Header del resultado
                col1, col2 = st.columns([3, 1])
                with col1:
                    if result['success']:
                        st.markdown(f"**✅ {result['url']}**")
                    else:
                        st.markdown(f"**❌ {result['url']}**")
                with col2:
                    st.write(f"🕐 {result['timestamp']}")
                
                # Detalles del resultado
                if result['success']:
                    stats_col1, stats_col2, stats_col3 = st.columns(3)
                    
                    with stats_col1:
                        html_length = result.get('content_length', 0)
                        st.metric("📄 Tamaño HTML", f"{html_length:,} bytes")
                    
                    with stats_col2:
                        links_count = result.get('links_count', 0)
                        st.metric("🔗 Enlaces", f"{links_count}")
                    
                    with stats_col3:
                        status = result.get('status', 'unknown')
                        st.metric("📊 Estado", status.upper())
                    
                    # Mostrar enlaces si existen
                    links = result.get('links', [])
                    if links:
                        with st.expander(f"🔗 Ver enlaces encontrados ({len(links)} mostrados)"):
                            for idx, link in enumerate(links, 1):
                                st.markdown(f"{idx}. `{link}`")
                else:
                    # Mostrar error
                    error_msg = result.get('error', 'Error desconocido')
                    st.error(f"❌ **Error:** {error_msg}")
                
                st.divider()
    
    # Debug info en sidebar
    with st.sidebar:
        st.divider()
        if st.session_state.connected:
            st.subheader("🔍 Debug")
            st.write(f"**Resultados:** {len(st.session_state.results)}")
            st.write(f"**Pendientes:** {len(st.session_state.pending_tasks)}")
            if st.session_state.pending_tasks:
                st.write("IDs pendientes:")
                for tid in list(st.session_state.pending_tasks.keys())[:3]:
                    st.caption(f"- {tid[:8]}...")
    
    # Auto-refresh cada 2 segundos
    time.sleep(2)
    st.rerun()


if __name__ == "__main__":
    main()
