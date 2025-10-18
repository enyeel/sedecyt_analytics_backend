import os
from supabase import create_client, client
from dotenv import load_dotenv

load_dotenv()  # Carga las variables de entorno desde el archivo .env

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

supabase: client.Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

print("Supabase client initialized.")

