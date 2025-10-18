from flask import jsonify, Blueprint
from app.services.auth_decorator import token_required


api_bp = Blueprint("api", __name__)

@api_bp.route("/chart/grafico1", methods=["GET"])
#@token_required

def grafico1():
    # Lógica para obtener y procesar los datos del gráfico 1
    data = {
        "labels": ["Enero", "Febrero", "Marzo", "Abril"],
        "values": [10, 20, 15, 30]
    }
    return jsonify(data)

@api_bp.route("/chart/grafico1_auth", methods=["GET"])
@token_required
def grafico1_auth():
    # Lógica para obtener y procesar los datos del gráfico 1
    data = {
        "labels": ["Enero", "Febrero", "Marzo", "Abril"],
        "values": [10, 20, 15, 30]
    }
    return jsonify(data)
 
 
@api_bp.route("/health", methods = ["GET"])
def health_check():
    return jsonify({"status": "API desplegada automaticamente desde Git"}), 200


@api_bp.route("tabla/<string:table_name>", methods=["GET"])
def get_table_data(table_name):
    from app.services.supabase_service import get_all_from
    
    print(f"Petición para obtener datos de la tabla: {table_name}")

    data = get_all_from(table_name)
    
    if isinstance(data, dict) and "error" in data:
        return jsonify(data), 404
    
    return jsonify(data), 200
