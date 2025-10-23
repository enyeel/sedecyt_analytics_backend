from flask import jsonify, Blueprint
from app.services.auth_decorator import token_required


api_bp = Blueprint("api", __name__)


#----------------------ENDPOINTS----------------------#

# Endpoint de prueba para verificar que la API está funcionando
@api_bp.route("/health", methods = ["GET"])
def health_check():
    return jsonify({"status": "API desplegada automaticamente desde Git"}), 200

# Endpoint para obtener todos los datos de una tabla específica
@api_bp.route("tabla/<string:table_name>", methods=["GET"])
@token_required
def get_table_data(table_name):                                 #el nombre de la tabla se pasa como parámetro en la URL después de /tabla/
    from app.services.supabase_service import get_all_from
    
    print(f"Petición para obtener datos de la tabla: {table_name}")

    data = get_all_from(table_name)
    
    if isinstance(data, dict) and "error" in data:
        return jsonify(data), 404
    
    return jsonify(data), 200

# Endpoint para testing de ETL script
@api_bp.route("/run-etl", methods=["GET"])
def run_etl():
    from app.services.etl_script import run_etl_process

    print("Iniciando el proceso ETL desde el endpoint...")
    result = run_etl_process()

    if isinstance(result, dict) and "error" in result:
        return jsonify(result), 500

    return jsonify(result), 200 