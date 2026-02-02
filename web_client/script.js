// Cliente Web de Scrapping - JavaScript

// Estado de la aplicación
const state = {
    tasks: {},  // {task_id: {url, status, result, timestamp}}
    statusInterval: null,
    currentMode: 'single',
    batchProgress: {
        total: 0,
        completed: 0,
        failed: 0
    }
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
    
    // Configurar selector de modo
    const modeSingle = document.getElementById('mode-single');
    const modeMultiple = document.getElementById('mode-multiple');
    
    modeSingle.addEventListener('change', () => {
        if (modeSingle.checked) {
            toggleMode('single');
        }
    });
    
    modeMultiple.addEventListener('change', () => {
        if (modeMultiple.checked) {
            toggleMode('multiple');
        }
    });
    
    // Iniciar actualización de estado
    updateStatus();
    state.statusInterval = setInterval(updateStatus, 5000); // Actualizar cada 5 segundos
    
    console.log('Cliente web inicializado');
}

function toggleMode(mode) {
    state.currentMode = mode;
    const singleGroup = document.getElementById('single-url-group');
    const multipleGroup = document.getElementById('multiple-url-group');
    const btnText = document.getElementById('btn-text');
    
    if (mode === 'single') {
        singleGroup.classList.remove('hidden');
        multipleGroup.classList.add('hidden');
        btnText.textContent = '🔍 Scrapear';
    } else {
        singleGroup.classList.add('hidden');
        multipleGroup.classList.remove('hidden');
        btnText.textContent = '🔍 Scrapear Todas';
    }
}

async function handleSubmit(event) {
    event.preventDefault();
    
    if (state.currentMode === 'single') {
        await handleSingleSubmit();
    } else {
        await handleMultipleSubmit();
    }
}

async function handleSingleSubmit() {
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
            
            const sentAt = Date.now();
            console.log(`[TIMING] Petición ${data.task_id} ENVIADA en timestamp: ${sentAt}`);
            
            // Agregar tarea al estado con timestamp de envío
            state.tasks[data.task_id] = {
                url: url,
                status: 'pending',
                timestamp: new Date().toISOString(),
                requestSentAt: sentAt  // Tiempo de envío
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

async function handleMultipleSubmit() {
    const urlsTextarea = document.getElementById('urls-textarea');
    const urlsText = urlsTextarea.value.trim();
    
    if (!urlsText) {
        showMessage('Por favor ingresa al menos una URL', 'error');
        return;
    }
    
    // Parsear URLs (una por línea)
    // Limpiar formato JSON: quitar comillas, comas, corchetes
    const urls = urlsText
        .split('\n')
        .map(line => line.trim())
        .map(line => line.replace(/^[\[\s]*"?/, ''))  // Quitar [ y " al inicio
        .map(line => line.replace(/"?,?\s*[\]\s]*$/, ''))  // Quitar ", y ] al final
        .filter(line => line.length > 0)
        .filter(line => line.match(/^https?:\/\/.+/));
    
    if (urls.length === 0) {
        showMessage('No se encontraron URLs válidas', 'error');
        return;
    }
    
    // Deshabilitar botón
    const btn = document.getElementById('scrape-btn');
    const btnText = document.getElementById('btn-text');
    const btnLoader = document.getElementById('btn-loader');
    const progressDiv = document.getElementById('batch-progress');
    const progressText = document.getElementById('progress-text');
    const progressFill = document.getElementById('progress-fill');
    
    btn.disabled = true;
    btnText.classList.add('hidden');
    btnLoader.classList.remove('hidden');
    progressDiv.classList.remove('hidden');
    
    // Resetear progreso
    state.batchProgress = {
        total: urls.length,
        completed: 0,
        failed: 0
    };
    
    updateBatchProgress();
    
    try {
        // Enviar todas las URLs
        for (let i = 0; i < urls.length; i++) {
            const url = urls[i];
            
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
                    const sentAt = Date.now();
                    console.log(`[TIMING] Petición ${data.task_id} ENVIADA en timestamp: ${sentAt}`);
                    
                    // Agregar tarea al estado con timestamp de envío
                    state.tasks[data.task_id] = {
                        url: url,
                        status: 'pending',
                        timestamp: new Date().toISOString(),
                        requestSentAt: sentAt  // Tiempo de envío
                    };
                    
                    // Iniciar polling para esta tarea
                    pollResult(data.task_id);
                    
                    state.batchProgress.completed++;
                } else {
                    console.error(`Error en URL ${url}:`, data.message);
                    state.batchProgress.failed++;
                }
                
            } catch (error) {
                console.error(`Error enviando URL ${url}:`, error);
                state.batchProgress.failed++;
            }
            
            updateBatchProgress();
            
            // Pequeña pausa entre peticiones para no saturar
            if (i < urls.length - 1) {
                await new Promise(resolve => setTimeout(resolve, 100));
            }
        }
        
        // Mostrar resumen
        const successCount = state.batchProgress.completed;
        const failCount = state.batchProgress.failed;
        showMessage(
            `Lote procesado: ${successCount} enviadas exitosamente, ${failCount} fallidas`,
            failCount === 0 ? 'success' : 'warning'
        );
        
        // Limpiar formulario
        urlsTextarea.value = '';
        
        // Actualizar vista
        renderResults();
        
        // Ocultar barra de progreso después de 3 segundos
        setTimeout(() => {
            progressDiv.classList.add('hidden');
        }, 3000);
        
    } catch (error) {
        console.error('Error en lote:', error);
        showMessage('Error procesando el lote de URLs', 'error');
    } finally {
        // Rehabilitar botón
        btn.disabled = false;
        btnText.classList.remove('hidden');
        btnLoader.classList.add('hidden');
    }
}

function updateBatchProgress() {
    const { total, completed, failed } = state.batchProgress;
    const processed = completed + failed;
    const percentage = total > 0 ? Math.round((processed / total) * 100) : 0;
    
    const progressText = document.getElementById('progress-text');
    const progressFill = document.getElementById('progress-fill');
    
    progressText.textContent = `Procesadas: ${processed} de ${total} (${completed} exitosas, ${failed} fallidas)`;
    progressFill.style.width = `${percentage}%`;
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
    const maxAttempts = 3000; // 5 minutos (3000 * 100ms = 300 segundos)
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
                
                // Calcular tiempo de respuesta INMEDIATAMENTE al recibir
                const receivedAt = Date.now();
                const sentAt = state.tasks[taskId].requestSentAt || receivedAt;
                const responseTimeMs = receivedAt - sentAt;
                
                console.log(`[TIMING] Resultado ${taskId} RECIBIDO en timestamp: ${receivedAt}`);
                console.log(`[TIMING] Tiempo transcurrido: ${responseTimeMs}ms (${(responseTimeMs/1000).toFixed(3)}s)`);
                console.log(`[TIMING] Sent: ${sentAt}, Received: ${receivedAt}, Diff: ${responseTimeMs}ms`);
                
                // Actualizar tarea en el estado
                state.tasks[taskId] = {
                    ...state.tasks[taskId],
                    status: 'completed',
                    result: data.result.result,
                    completedAt: data.result.timestamp,
                    requestSentAt: sentAt,
                    responseTime: (responseTimeMs / 1000).toFixed(3)  // 3 decimales para mayor precisión
                };
                
                // Actualizar vista
                renderResults();
                
                console.log(`Resultado recibido para ${taskId}`);
            }
            
        } catch (error) {
            console.error(`Error obteniendo resultado para ${taskId}:`, error);
        }
    }, 100); // Polling cada 100ms para medición precisa del tiempo
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
    // Determinar el estado basado en task.status y task.result.status
    let statusClass = 'pending';
    let statusText = 'Pendiente...';
    
    if (task.status === 'completed' && task.result) {
        if (task.result.status === 'error') {
            statusClass = 'error';
            statusText = '❌ Error';
        } else if (task.result.status === 'success') {
            statusClass = 'completed';
            statusText = '✓ Completada';
        } else {
            statusClass = 'completed';
            statusText = 'Completada';
        }
    }
    
    const timestamp = new Date(task.timestamp).toLocaleString('es-ES');
    
    let resultHTML = '';
    if (task.status === 'completed' && task.result) {
        const result = task.result;
        
        if (result.status === 'error') {
            // Mostrar error
            const errorMsg = result.error || 'Error desconocido';
            resultHTML = `
                <div class="result-data error-data">
                    <strong>❌ Error al scrapear:</strong><br>
                    <div class="error-message">${escapeHtml(errorMsg)}</div>
                    <p class="error-help">
                        Posibles causas:<br>
                        • La URL no es accesible<br>
                        • Tiempo de espera agotado<br>
                        • Problemas de red<br>
                        • Servidor rechazó la conexión
                    </p>
                </div>
            `;
        } else {
            // Formatear resultado exitoso
            const htmlLength = result.html_length || 0;
            const linksCount = result.links_count || 0;
            const links = result.links || [];
            
            // Validar que el resultado tiene contenido real
            // Si tiene 0 en todo, es un error disfrazado de éxito
            if (htmlLength === 0 && linksCount === 0) {
                resultHTML = `
                    <div class="result-data error-data">
                        <strong>❌ Error al scrapear:</strong><br>
                        <div class="error-message">No se pudo obtener contenido de la página</div>
                        <p class="error-help">
                            Posibles causas:<br>
                            • La URL no respondió con contenido HTML<br>
                            • El servidor rechazó la conexión<br>
                            • Problemas de red durante el scraping<br>
                            • La página está vacía o protegida
                        </p>
                    </div>
                `;
                // Cambiar el estado visual a error
                statusClass = 'error';
                statusText = '❌ Error';
            } else {
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
            }
        }
    } else if (task.status === 'pending') {
        resultHTML = `
            <div class="result-data">
                <p>⏳ Esperando resultado del sistema distribuido...</p>
            </div>
        `;
    }
    
    return `
        <div class="result-card ${statusClass}" data-task-id="${taskId}">
            <div class="result-header" onclick="toggleResultCard('${taskId}')">
                <div class="result-header-content">
                    <div class="result-url">${escapeHtml(task.url)}</div>
                    <span class="result-status ${statusClass}">${statusText}</span>
                </div>
                <div class="result-toggle">
                    <span class="toggle-arrow">▼</span>
                </div>
            </div>
            <div class="result-meta">
                <strong>ID:</strong> ${taskId} | 
                <strong>Enviada:</strong> ${timestamp}${task.responseTime ? ` | <span style="color: var(--success-color);">⚡ ${task.responseTime}s</span>` : ' | <span style="color: red;">⚡ 999999.999s [ERROR: NO SE MIDIÓ]</span>'}
            </div>
            <div class="result-content collapsed" id="content-${taskId}">
                ${resultHTML}
            </div>
        </div>
    `;
}

function toggleResultCard(taskId) {
    const content = document.getElementById(`content-${taskId}`);
    const arrow = document.querySelector(`[data-task-id="${taskId}"] .toggle-arrow`);
    
    if (content.classList.contains('collapsed')) {
        content.classList.remove('collapsed');
        content.classList.add('expanded');
        arrow.textContent = '▲';
    } else {
        content.classList.remove('expanded');
        content.classList.add('collapsed');
        arrow.textContent = '▼';
    }
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
// ============ DATABASE VIEWER ============

const dbState = {
    tables: [],
    currentTable: null,
    currentPage: 1,
    pageSize: 50,
    totalPages: 1
};

// Inicializar eventos de DB
document.getElementById('load-tables-btn').addEventListener('click', loadTables);
document.getElementById('export-all-btn').addEventListener('click', exportAllData);
document.getElementById('close-table-btn').addEventListener('click', closeTableViewer);
document.getElementById('prev-page-btn').addEventListener('click', () => changePage(-1));
document.getElementById('next-page-btn').addEventListener('click', () => changePage(1));

async function loadTables() {
    const btn = document.getElementById('load-tables-btn');
    const loadingDiv = document.getElementById('tables-loading');
    const tablesListDiv = document.getElementById('tables-list');
    const tableViewerDiv = document.getElementById('table-viewer');
    
    // Deshabilitar botón y ocultar contenido anterior
    btn.disabled = true;
    tablesListDiv.classList.add('hidden');
    tableViewerDiv.classList.add('hidden');
    
    // Mostrar indicador de carga
    loadingDiv.classList.remove('hidden');
    
    try {
        const response = await fetch('/api/tables');
        const data = await response.json();
        
        if (data.success) {
            dbState.tables = data.tables;
            renderTables();
            tablesListDiv.classList.remove('hidden');
            showMessage(`${data.tables.length} tabla(s) cargadas`, 'success');
        } else {
            showMessage(`Error: ${data.error || 'No se pudieron cargar las tablas'}`, 'error');
            tablesListDiv.classList.add('hidden');
        }
    } catch (error) {
        console.error('Error cargando tablas:', error);
        showMessage('Error de conexión al cargar tablas', 'error');
        tablesListDiv.classList.add('hidden');
    } finally {
        // Ocultar indicador de carga y rehabilitar botón
        loadingDiv.classList.add('hidden');
        btn.disabled = false;
    }
}

function renderTables() {
    const container = document.getElementById('tables-container');
    
    if (dbState.tables.length === 0) {
        container.innerHTML = '<p class="no-results">No hay tablas en la base de datos.</p>';
        return;
    }
    
    container.innerHTML = dbState.tables.map(table => `
        <div class="table-card" onclick="loadTableData('${escapeHtml(table)}')">
            <div class="table-card-name">📋 ${escapeHtml(table)}</div>
        </div>
    `).join('');
}

async function loadTableData(tableName, page = 1) {
    dbState.currentTable = tableName;
    dbState.currentPage = page;
    
    const tableViewer = document.getElementById('table-viewer');
    const dataContainer = document.getElementById('table-data-container');
    const tableTitleEl = document.getElementById('current-table-name');
    
    // Actualizar título y mostrar visor
    tableTitleEl.textContent = `📊 ${tableName}`;
    tableViewer.classList.remove('hidden');
    
    // Mostrar indicador de carga con spinner
    dataContainer.innerHTML = `
        <div style="text-align: center; padding: 40px;">
            <div class="spinner" style="margin: 0 auto 20px;"></div>
            <p style="color: var(--text-secondary);">⏳ Cargando datos de la tabla...</p>
            <p style="color: var(--text-secondary); font-size: 0.9rem;">Esperando respuesta del servidor...</p>
        </div>
    `;
    
    // Guardar tiempo de inicio JUSTO antes del fetch
    const requestStartTime = Date.now();
    console.log(`[TIMING] Petición tabla "${tableName}" ENVIADA en timestamp: ${requestStartTime}`);
    
    try {
        const response = await fetch(`/api/table/${tableName}?page=${page}&page_size=${dbState.pageSize}`);
        const data = await response.json();
        
        // Calcular tiempo de respuesta inmediatamente después de recibir
        const receivedAt = Date.now();
        const responseTime = ((receivedAt - requestStartTime) / 1000).toFixed(3);
        
        console.log(`[TIMING] Respuesta tabla "${tableName}" RECIBIDA en timestamp: ${receivedAt}`);
        console.log(`[TIMING] Tiempo transcurrido: ${receivedAt - requestStartTime}ms (${responseTime}s)`);
        console.log(`[TIMING] Sent: ${requestStartTime}, Received: ${receivedAt}, Diff: ${receivedAt - requestStartTime}ms`);
        
        if (data.success) {
            renderTableData(data, responseTime);
            updatePaginationControls(data.pagination);
        } else {
            dataContainer.innerHTML = `
                <div style="text-align: center; padding: 40px;">
                    <p class="error-message">❌ Error: ${escapeHtml(data.error || data.message || 'Error desconocido')}</p>
                    <button onclick="loadTableData('${escapeHtml(tableName)}', ${page})" class="btn-secondary" style="margin-top: 15px;">
                        🔄 Reintentar
                    </button>
                </div>
            `;
        }
    } catch (error) {
        console.error('Error cargando datos de tabla:', error);
        dataContainer.innerHTML = `
            <div style="text-align: center; padding: 40px;">
                <p class="error-message">❌ Error de conexión</p>
                <p style="color: var(--text-secondary); margin-top: 10px;">No se pudo conectar con el servidor</p>
                <button onclick="loadTableData('${escapeHtml(tableName)}', ${page})" class="btn-secondary" style="margin-top: 15px;">
                    🔄 Reintentar
                </button>
            </div>
        `;
    }
}

function renderTableData(data, responseTime = null) {
    const container = document.getElementById('table-data-container');
    
    if (!data.rows || data.rows.length === 0) {
        container.innerHTML = '<p class="no-results">No hay datos en esta tabla.</p>';
        return;
    }
    
    const columns = data.columns;
    const rows = data.rows;
    
    // Agregar información de la tabla con tiempo de respuesta
    const timeInfo = responseTime ? ` • <span style="color: var(--success-color); font-weight: 500;">⚡ ${responseTime}s</span>` : '';
    let tableHTML = `<div class="table-info">📊 ${data.pagination.total_rows} registro(s) • ${columns.length} columna(s)${timeInfo}</div>`;
    tableHTML += '<table class="data-table"><thead><tr>';
    columns.forEach(col => {
        tableHTML += `<th>${escapeHtml(col)}</th>`;
    });
    tableHTML += '</tr></thead><tbody>';
    
    rows.forEach(row => {
        tableHTML += '<tr>';
        columns.forEach(col => {
            const value = row[col];
            const displayValue = value === null || value === undefined ? '-' : String(value);
            tableHTML += `<td>${escapeHtml(displayValue)}</td>`;
        });
        tableHTML += '</tr>';
    });
    
    tableHTML += '</tbody></table>';
    container.innerHTML = tableHTML;
}

function updatePaginationControls(pagination) {
    dbState.totalPages = pagination.total_pages;
    dbState.currentPage = pagination.page;
    
    document.getElementById('page-info').textContent = 
        `Página ${pagination.page} de ${pagination.total_pages} (${pagination.total_rows} registros)`;
    
    document.getElementById('prev-page-btn').disabled = pagination.page <= 1;
    document.getElementById('next-page-btn').disabled = pagination.page >= pagination.total_pages;
}

function changePage(delta) {
    const newPage = dbState.currentPage + delta;

    if (newPage < 1 || newPage > dbState.totalPages) {
        return;
    }

    if (newPage >= 1 && newPage <= dbState.totalPages) {
        loadTableData(dbState.currentTable, newPage);
    }
}

function closeTableViewer() {
    document.getElementById('table-viewer').classList.add('hidden');
    dbState.currentTable = null;
}

async function exportAllData() {
    const btn = document.getElementById('export-all-btn');
    const originalText = btn.innerHTML;
    
    // Deshabilitar botón y mostrar estado de carga
    btn.disabled = true;
    btn.innerHTML = '⏳ Exportando...';
    
    try {
        const response = await fetch('/api/export');
        
        if (!response.ok) {
            throw new Error(`Error HTTP: ${response.status}`);
        }
        
        const data = await response.json();
        
        if (data.success) {
            // Crear archivo JSON para descarga
            const jsonStr = JSON.stringify(data.data, null, 2);
            const blob = new Blob([jsonStr], { type: 'application/json' });
            const url = URL.createObjectURL(blob);
            
            // Crear enlace de descarga temporal
            const a = document.createElement('a');
            a.href = url;
            a.download = `scrapper-data-${new Date().toISOString().split('T')[0]}.json`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
            
            showMessage(`✓ Datos exportados correctamente (${data.data.urls?.length || 0} URLs)`, 'success');
        } else {
            showMessage(`Error: ${data.error || 'No se pudieron exportar los datos'}`, 'error');
        }
    } catch (error) {
        console.error('Error exportando datos:', error);
        showMessage('Error de conexión al exportar datos', 'error');
    } finally {
        // Rehabilitar botón y restaurar texto
        btn.disabled = false;
        btn.innerHTML = originalText;
    }
}