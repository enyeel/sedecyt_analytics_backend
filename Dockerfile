# Imagen base
FROM python:3.11-slim

# Evita buffering de logs
ENV PYTHONUNBUFFERED=1

# Crear directorio de la app
WORKDIR /app

# Copiar requirements y luego instalarlos
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar todo el proyecto
COPY . .

# El comando CMD ahora usa Gunicorn, un servidor de producción para Python.
# --bind 0.0.0.0:$PORT : Le dice a Gunicorn que escuche en todas las interfaces de red
#                        en el puerto que Cloud Run le asigne a través de la variable $PORT.
# run:app : Asume que tu archivo se llama run.py y la instancia de Flask se llama app.
#           Si tu archivo es main.py y tu variable es server, sería "main:server".
CMD gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 8 --timeout 0 run:app

