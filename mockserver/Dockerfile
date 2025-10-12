
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

# Crear directorio para HTMLs si no existe
RUN mkdir -p /app/html

EXPOSE 80

CMD ["python", "app.py"]
