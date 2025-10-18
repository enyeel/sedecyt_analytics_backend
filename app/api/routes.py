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

