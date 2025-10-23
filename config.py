# config.py
import os
# Para Cloud Run, el archivo estará en una ruta fija.
# Para local, podemos decirle que lo busque en la raíz del proyecto.
# La variable de entorno tiene prioridad.
CREDENTIALS_PATH = os.environ.get("GOOGLE_CREDENTIALS_PATH", "/app/credentials.json")
SHEET_ID = os.environ.get("SPREADSHEET_ID")