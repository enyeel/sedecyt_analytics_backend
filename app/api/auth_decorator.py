from functools import wraps
from flask import request, jsonify
import os 
from app.core.connections.supabase_service import supabase


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
