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

#endpoint de prueba para verificar conexión con supabase
@api_bp.route("tabla-no-auth/<string:tabla>", methods=["GET"])
def get_table(tabla):                                 #el nombre de la tabla se pasa como parámetro en la URL después de /tabla/
    from app.services.supabase_service import get_all_from
    
    print(f"Petición para obtener datos de la tabla: {tabla}")

    data = get_all_from(tabla)
    
    if isinstance(data, dict) and "error" in data:
        return jsonify(data), 404
    
    return jsonify(data), 200

# Endpoint de prueba para verificar la conexión con Google Sheets

@api_bp.route("/sheets", methods=['GET'])
@token_required
def get_contactos_from_sheet():
    from ..services import google_sheets_service

    print("Petición para obtener datos de Google Sheets")

    try:
        # Solo le pasas el nombre de la pestaña, ¡y listo!
        df_contactos = google_sheets_service.read_worksheet_as_dataframe("Formulario Desarrollo Industria")
        contactos_json = df_contactos.to_dict(orient='records')
        return jsonify(contactos_json)
    except Exception as e:
        return jsonify({"error": str(e)}), 500