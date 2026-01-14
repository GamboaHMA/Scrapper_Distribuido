// Cliente Web de Scrapping - JavaScript

// Estado de la aplicación
const state = {
    tasks: {},  // {task_id: {url, status, result, timestamp}}
    statusInterval: null
};

// Inicializar cuando el DOM esté listo
document.addEventListener('DOMContentLoaded', () => {
    initializeApp();
});

function initializeApp() {
    console.log('Inicializando cliente web...');
    
    // Configurar formulario
    const form = document.getElementById('scrape-form');
    form.addEventListener('submit', handleSubmit);
    
    // Iniciar actualización de estado
    updateStatus();
    state.statusInterval = setInterval(updateStatus, 5000); // Actualizar cada 5 segundos
    
    console.log('Cliente web inicializado');
}

async function handleSubmit(event) {
    event.preventDefault();
    
    const urlInput = document.getElementById('url-input');
    const url = urlInput.value.trim();
    
    if (!url) {
        showMessage('Por favor ingresa una URL', 'error');
        return;
    }
    
    // Deshabilitar botón
    const btn = document.getElementById('scrape-btn');
    const btnText = document.getElementById('btn-text');
    const btnLoader = document.getElementById('btn-loader');
    
    btn.disabled = true;
    btnText.classList.add('hidden');
    btnLoader.classList.remove('hidden');
    
    try {
        const response = await fetch('/api/scrape', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ url })
        });
        
        const data = await response.json();
        
        if (data.success) {
            showMessage(`Petición enviada exitosamente. ID: ${data.task_id}`, 'success');
            
            // Agregar tarea al estado
            state.tasks[data.task_id] = {
                url: url,
                status: 'pending',
                timestamp: new Date().toISOString()
            };
            
            // Limpiar formulario
            urlInput.value = '';
            
            // Actualizar vista
            renderResults();
            
            // Iniciar polling para esta tarea
            pollResult(data.task_id);
            
        } else {
            showMessage(`Error: ${data.message}`, 'error');
        }
        
    } catch (error) {
        console.error('Error enviando petición:', error);
        showMessage('Error de conexión. Intenta de nuevo.', 'error');
    } finally {
        // Rehabilitar botón
        btn.disabled = false;
        btnText.classList.remove('hidden');
        btnLoader.classList.add('hidden');
    }
}

async function updateStatus() {
    try {
        const response = await fetch('/api/status');
        const status = await response.json();
        
        // Actualizar indicadores de estado
        const connectionStatus = document.getElementById('connection-status');
        connectionStatus.textContent = status.connected ? 'Conectado' : 'Desconectado';
        connectionStatus.className = `status-value ${status.connected ? 'connected' : 'disconnected'}`;
        
        const routerIp = document.getElementById('router-ip');
        routerIp.textContent = status.router_ip || '-';
        
        const pendingCount = document.getElementById('pending-count');
        pendingCount.textContent = status.pending_requests;
        
        const completedCount = document.getElementById('completed-count');
        completedCount.textContent = status.completed_requests;
        
    } catch (error) {
        console.error('Error actualizando estado:', error);
    }
}

async function pollResult(taskId) {
    const maxAttempts = 60; // 5 minutos (60 * 5 segundos)
    let attempts = 0;
    
    const interval = setInterval(async () => {
        attempts++;
        
        if (attempts > maxAttempts) {
            clearInterval(interval);
            console.log(`Timeout esperando resultado para ${taskId}`);
            return;
        }
        
        try {
            const response = await fetch(`/api/result/${taskId}`);
            const data = await response.json();
            
            if (data.success && data.result) {
                clearInterval(interval);
                
                // Actualizar tarea en el estado
                state.tasks[taskId] = {
                    ...state.tasks[taskId],
                    status: 'completed',
                    result: data.result.result,
                    completedAt: data.result.timestamp
                };
                
                // Actualizar vista
                renderResults();
                
                console.log(`Resultado recibido para ${taskId}`);
            }
            
        } catch (error) {
            console.error(`Error obteniendo resultado para ${taskId}:`, error);
        }
    }, 5000); // Polling cada 5 segundos
}

function renderResults() {
    const container = document.getElementById('results-container');
    
    const taskIds = Object.keys(state.tasks);
    
    if (taskIds.length === 0) {
        container.innerHTML = '<p class="no-results">No hay resultados aún. Envía una petición de scrapping para comenzar.</p>';
        return;
    }
    
    // Ordenar por timestamp (más recientes primero)
    const sortedTasks = taskIds.sort((a, b) => {
        const timeA = new Date(state.tasks[a].timestamp);
        const timeB = new Date(state.tasks[b].timestamp);
        return timeB - timeA;
    });
    
    // Renderizar cada tarea
    container.innerHTML = sortedTasks.map(taskId => {
        const task = state.tasks[taskId];
        return renderResultCard(taskId, task);
    }).join('');
}

function renderResultCard(taskId, task) {
    const statusClass = task.status === 'completed' ? 'completed' : 'pending';
    const statusText = task.status === 'completed' ? 'Completada' : 'Pendiente...';
    
    const timestamp = new Date(task.timestamp).toLocaleString('es-ES');
    
    let resultHTML = '';
    if (task.status === 'completed' && task.result) {
        const result = task.result;
        
        // Formatear resultado
        const htmlLength = result.html_length || 0;
        const linksCount = result.links_count || 0;
        const links = result.links || [];
        
        resultHTML = `
            <div class="result-data">
                <strong>📊 Estadísticas:</strong><br>
                • Tamaño HTML: ${htmlLength.toLocaleString()} caracteres<br>
                • Enlaces encontrados: ${linksCount}<br>
                <br>
                <strong>🔗 Enlaces (primeros 10):</strong><br>
                ${links.length > 0 
                    ? links.map(link => `• <a href="${link}" target="_blank">${link}</a>`).join('<br>')
                    : 'No se encontraron enlaces'
                }
            </div>
        `;
    } else if (task.status === 'pending') {
        resultHTML = `
            <div class="result-data">
                <p>⏳ Esperando resultado del sistema distribuido...</p>
            </div>
        `;
    }
    
    return `
        <div class="result-card ${statusClass}">
            <div class="result-header">
                <div class="result-url">${escapeHtml(task.url)}</div>
                <span class="result-status ${statusClass}">${statusText}</span>
            </div>
            <div class="result-meta">
                <strong>ID:</strong> ${taskId} | 
                <strong>Enviada:</strong> ${timestamp}
            </div>
            ${resultHTML}
        </div>
    `;
}

function showMessage(text, type) {
    const messageDiv = document.getElementById('form-message');
    messageDiv.textContent = text;
    messageDiv.className = `message ${type}`;
    messageDiv.classList.remove('hidden');
    
    // Ocultar después de 5 segundos
    setTimeout(() => {
        messageDiv.classList.add('hidden');
    }, 5000);
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Limpiar intervalo al cerrar
window.addEventListener('beforeunload', () => {
    if (state.statusInterval) {
        clearInterval(state.statusInterval);
    }
});
