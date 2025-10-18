import os
from supabase import create_client, client
from dotenv import load_dotenv

load_dotenv()  # Carga las variables de entorno desde el archivo .env

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase: client.Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

print("Supabase client initialized.")

def get_all_from(tabale_name: str):
    try:
        response = supabase.table(tabale_name).select("*").execute()
    
        return response.data
    except Exception as e:
        print(f"Error fetching data from {tabale_name}: {e}")
        return {"error": f"Could not fetch data from {tabale_name}"}