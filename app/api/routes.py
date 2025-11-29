from flask import jsonify, Blueprint, request
from app.api.auth_decorator import token_required
import os
import json


api_bp = Blueprint("api", __name__)


#----------------------ENDPOINTS----------------------#

from config.dashboards_config import DASHBOARDS_CONFIG # <--- Importamos la config directa

@api_bp.route("/dashboards/meta", methods=['GET'])
@token_required
def get_dashboards_meta():
    """
    Ruta 'Super Express' que devuelve metadatos desde memoria (RAM).
    No consulta la base de datos, por lo que es inmediara.
    """
    try:
        # Simplemente contamos la longitud de la lista en memoria
        count = len(DASHBOARDS_CONFIG)
        
        return jsonify({
            "count": count,
            "source": "memory" # Para que sepas que vino del caché/config
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint de prueba para verificar que la API está funcionando
@api_bp.route("/health", methods = ["GET"])
def health_check():
    return jsonify({"status": "API desplegada automaticamente desde Git"}), 200

# Endpoint para obtener todos los datos de una tabla específica
@api_bp.route("tabla/<string:table_name>", methods=["GET"])
@token_required
def get_table_data(table_name):                                 #el nombre de la tabla se pasa como parámetro en la URL después de /tabla/
    from app.core.connections.supabase_service import get_all_from
    
    print(f"Petición para obtener datos de la tabla: {table_name}")

    data = get_all_from(table_name)
    
    if isinstance(data, dict) and "error" in data:
        return jsonify(data), 404
    
    return jsonify(data), 200

#endpoint de prueba para verificar conexión con supabase
@api_bp.route("tabla-no-auth/<string:tabla>", methods=["GET"])
def get_table(tabla):                                 #el nombre de la tabla se pasa como parámetro en la URL después de /tabla/
    from app.core.connections.supabase_service import get_all_from
    
    print(f"Petición para obtener datos de la tabla: {tabla}")

    data = get_all_from(tabla)
    
    if isinstance(data, dict) and "error" in data:
        return jsonify(data), 404
    
    return jsonify(data), 200

# Endpoint de prueba para verificar la conexión con Google Sheets


@api_bp.route("/dashboards", methods=['GET'])
@token_required
def get_all_dashboards():
    """
    Endpoint to get a lightweight list of all available dashboards.
    """
    from app.services import dashboard_service
    print("Petición para obtener la lista de dashboards")
    dashboards_list = dashboard_service.get_all_dashboards_list()
    
    # For this example, we'll load from a mock JSON file.
    # base_dir = os.path.dirname(os.path.abspath(__file__))
    # file_path = os.path.join(base_dir, '..', 'data', 'inputs', 'mock_dashboards_list.json')
    # with open(file_path, 'r', encoding='utf-8') as f:
    #     dashboards_list = json.load(f)
    
    response = jsonify(dashboards_list)
    
    response.headers['Cache-Control'] = 'private, max-age=300'
    
    print("--- DEBUG: SENDING THIS JSON TO FRONTEND ---")
    print(response.get_data(as_text=True))
    
    return response, 200
    
    
@api_bp.route("/dashboards/<string:dashboard_slug>", methods=['GET'])
@token_required
def get_single_dashboard(dashboard_slug):
    """
    Endpoint to get the full data (including charts) for a single dashboard.
    """
    from app.services import dashboard_service
    print(f"Petición para obtener el dashboard con slug: {dashboard_slug}")

    # 1. Get the list of ALL dashboards, fully assembled with their charts.
    all_dashboards = dashboard_service.get_dashboards_with_data()
    
    # For this example, we'll load from a mock JSON file.
    # base_dir = os.path.dirname(os.path.abspath(__file__))
    # file_path = os.path.join(base_dir, '..', 'data', 'inputs', 'mock_dashboards_full.json')

    # with open(file_path, 'r', encoding='utf-8') as f:
    #     all_dashboards = json.load(f)

    # Buscamos el dashboard que coincida con el slug solicitado
    target_dashboard = next((d for d in all_dashboards if d.get('slug') == dashboard_slug), None)

    if not target_dashboard:
        return jsonify({"error": "Dashboard not found"}), 404
        
    print("--- DEBUG: SENDING THIS JSON TO FRONTEND ---")
    print(target_dashboard)

    # 3. Return the single, complete dashboard object.
    return jsonify(target_dashboard), 200

@api_bp.route("/companies/search", methods=['GET'])
@token_required
def search_company():
    """
    Searches for a single company by its trade name.
    Expects a query parameter: /api/companies/search?q=MyCompany
    """
    from app.core.connections.supabase_service import supabase
    
    query = request.args.get('q')
    if not query:
        return jsonify({"error": "Query parameter 'q' is required"}), 400

    try:
        # Use 'ilike' for a case-insensitive search
        response = supabase.table('companies').select('*').ilike('trade_name', f'%{query}%').limit(1).single().execute()
        return jsonify(response.data), 200
    except Exception as e:
        # Supabase client raises an exception if no rows are found with .single()
        return jsonify({"error": "Company not found"}), 404