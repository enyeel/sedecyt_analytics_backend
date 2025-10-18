# run.py
from app import create_app
from dotenv import load_dotenv

# Carga las variables del archivo .env
load_dotenv()

# Crea la aplicación usando la fábrica
app = create_app()

if __name__ == '__main__':
    # El puerto 8080 es común y es el que Cloud Run usa por defecto
    app.run(debug=True, port=8080)