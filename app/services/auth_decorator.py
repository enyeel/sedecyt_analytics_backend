from functools import wraps
from flask import request, jsonify
import os 
from supabase import create_client, Client

supabase_url = os.environ.get("SUPABASE_URL")
supabase_key = os.environ.get("SUPABASE_SERVICE_KEY")

# --- LÍNEAS DE DEPURACIÓN ---
print("--- INICIANDO DEPURACIÓN DE VARIABLES ---")
print(f"URL leída por Python: '{supabase_url}'")
print(f"Tipo de dato de la URL: {type(supabase_url)}")
print(f"Key leída por Python (primeros 5 caracteres): '{str(supabase_key)[:5]}...'")
print("--- FIN DE DEPURACIÓN ---")
# --- FIN DE LÍNEAS DE DEPURACIÓN ---

supabase: Client = create_client(supabase_url, supabase_key)

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None

        if 'Authorization' in request.headers:
            token = request.headers['Authorization'].split(" ")[1]  # Asumiendo formato "Bearer <token

        if not token: 
            return jsonify({'message': 'Falta el token de autorizacion'}), 401
        
        try:
            user_response = supabase.auth.get_user(token)

        except Exception as e:
            return jsonify({'message': 'Token invalido o expirado', 'error': str(e)}), 401
        
        return f(*args, **kwargs)
    
    return decorated

