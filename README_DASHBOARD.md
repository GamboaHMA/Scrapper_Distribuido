# Dashboard Web - Scrapper Distribuido

Interfaz gráfica web para controlar y monitorear el sistema de scrapping distribuido a través de la API REST.

## 🎯 Características

### ✨ **Dashboard Visual**
- **Estado en tiempo real** de la API y servidor
- **Estadísticas** de requests enviados, exitosos y fallidos
- **Indicadores visuales** de conexión
- **Actualización automática** cada 30 segundos

### 🔍 **Scraping Simple**
- Enviar **una URL** para scrapping
- Validación automática de URLs
- Feedback inmediato del resultado

### 📋 **Scraping Múltiple**
- Enviar **múltiples URLs** en lote
- Una URL por línea en el textarea
- Procesamiento y reporte individual de cada URL

### 📊 **Logs en Tiempo Real**
- **Historial** de todas las acciones
- **Códigos de colores** (éxito, error, info)
- **Timestamps** de cada acción
- **Auto-scroll** para mantener los logs recientes visibles

### 📱 **Diseño Responsive**
- **Adaptativo** a diferentes tamaños de pantalla
- **Optimizado** para desktop y móvil
- **Interfaz moderna** con gradientes y animaciones

## 🚀 Uso Rápido

### 1. Iniciar el servidor principal
```bash
# En una terminal
python3 server/server.py
```

### 2. Iniciar la API REST
```bash
# En otra terminal
./start_api.sh
```

### 3. Abrir el dashboard
```bash
# Opción 1: Script automático
./open_dashboard.sh

# Opción 2: Navegador manual
# Ir a: http://localhost:8082/dashboard
```

## 🌐 URLs Disponibles

- **Dashboard Principal**: `http://localhost:8082/dashboard`
- **Página de inicio**: `http://localhost:8082/`
- **API Health**: `http://localhost:8082/api/health`
- **API Status**: `http://localhost:8082/api/status`

## 🎨 Interfaz

### Estado de Conexión
- 🟢 **Verde**: API conectada al servidor
- 🔴 **Rojo**: API desconectada o error

### Estadísticas
- **Requests Enviados**: Total de solicitudes realizadas
- **Exitosos**: Solicitudes procesadas correctamente
- **Fallidos**: Solicitudes con error
- **Estado Servidor**: Estado del servidor principal

### Formularios
1. **Scraping Simple**:
   - Campo de URL con validación
   - Botón con indicador de carga
   - Limpieza automática tras éxito

2. **Scraping Múltiple**:
   - Textarea para múltiples URLs
   - Una URL por línea
   - Procesamiento en lote

### Logs
- **Verde**: Operaciones exitosas
- **Rojo**: Errores
- **Azul**: Información general
- **Gris**: Timestamps

## 🔧 Configuración

### Variables de Entorno
```bash
export API_HOST="localhost"    # IP de la API
export API_PORT="8082"         # Puerto de la API
```

### Personalización
El archivo `web_interface/dashboard.html` contiene toda la interfaz y se puede personalizar:

- **Colores**: Modificar las variables CSS en el `<style>`
- **Intervalos**: Cambiar el intervalo de actualización (30000ms por defecto)
- **API URL**: Modificar `API_BASE_URL` en JavaScript

## 📱 Ejemplos de Uso

### Scraping Simple
1. Escribir URL: `https://example.com`
2. Hacer clic en "Enviar URL"
3. Ver resultado en logs

### Scraping Múltiple
1. Escribir URLs separadas por línea:
   ```
   https://example.com
   https://test.com
   https://demo.org
   ```
2. Hacer clic en "Enviar URLs"
3. Ver progreso en logs

### Monitoreo
- El dashboard se actualiza automáticamente
- Las estadísticas se incrementan con cada operación
- Los logs muestran el historial completo

## 🛠️ Troubleshooting

### "API Desconectada"
1. Verificar que `./start_api.sh` esté corriendo
2. Verificar que el puerto 8082 esté libre
3. Comprobar la conexión al servidor principal (puerto 8080)

### "Error enviando URL"
1. Verificar que la URL sea válida (http:// o https://)
2. Comprobar que el servidor principal esté aceptando conexiones
3. Revisar los logs del servidor principal

### Dashboard no carga
1. Verificar la URL: `http://localhost:8082/dashboard`
2. Comprobar que no haya bloqueadores de contenido
3. Revisar la consola del navegador para errores JavaScript

## 🔄 Integración con Docker

Para usar el dashboard con Docker Swarm:

```bash
# Modificar docker-compose.yml para exponer puerto 8082
ports:
  - "8080:8080"  # Servidor principal
  - "8082:8082"  # API + Dashboard
```

## 🎯 Próximas Mejoras

- [ ] **Autenticación** básica
- [ ] **Histórico persistente** de estadísticas
- [ ] **Notificaciones push** del navegador
- [ ] **Configuración** en tiempo real
- [ ] **Métricas avanzadas** (tiempo de respuesta, etc.)
- [ ] **Tema oscuro/claro**
- [ ] **Exportación** de logs y estadísticas

## 📋 Tecnologías Utilizadas

- **Frontend**: HTML5, CSS3, JavaScript (Vanilla)
- **Backend**: Flask (Python)
- **Comunicación**: REST API + AJAX
- **Diseño**: CSS Grid + Flexbox
- **Iconos**: Emojis Unicode