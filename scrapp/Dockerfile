FROM python:3.11-slim

WORKDIR /app

# Copiar requirements y instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar script
COPY scrapper.py .

# Hacer ejecutable
RUN chmod +x scrapper.py

# Comando por defecto: espera URL como argumento
CMD ["python3", "scrapper.py"]