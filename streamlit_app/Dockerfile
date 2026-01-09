FROM python:3.11-slim

WORKDIR /app

# Copiar archivos de la aplicación
COPY streamlit_app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código base compartido
COPY base_node/ /app/base_node/

# Copiar aplicación Streamlit
COPY streamlit_app/streamlit_app.py .

# Exponer puerto de Streamlit
EXPOSE 8501

# Variables de entorno
ENV ROUTER_IP=router-node
ENV ROUTER_PORT=7070

# Comando para ejecutar Streamlit
CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]
