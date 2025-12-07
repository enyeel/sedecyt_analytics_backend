from flask import jsonify, Blueprint, request
from app.api.auth_decorator import token_required
import os
import json

api_bp = Blueprint("api", __name__)

#----------------------ENDPOINTS----------------------#

# Endpoint de prueba para verificar que la API está funcionando
@api_bp.route("/health", methods = ["GET"])
def health_check():
    return jsonify({"status": "API desplegada automaticamente desde Git"}), 200

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

# Endpoint para obtener todos los datos de una tabla específica
@api_bp.route("table/<string:table_name>", methods=["GET"])
@token_required
def get_table_data(table_name):                                 #el nombre de la tabla se pasa como parámetro en la URL después de /tabla/
    from app.core.connections.supabase_service import get_all_from
    
    print(f"Petición para obtener datos de la tabla: {table_name}")

    data = get_all_from(table_name)
    
    if isinstance(data, dict) and "error" in data:
        return jsonify(data), 404
    
    return jsonify(data), 200

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

# ----------------------VISTAS PARA TABLAS DEL FRONT ---------------------- #

# --- VISTA 1: EMPRESAS LIMPIAS ---
@api_bp.route("/data/companies-view", methods=['GET'])
@token_required
def get_companies_view():
    from app.core.connections.supabase_service import get_all_from
    import pandas as pd
    
    # 1. Traer datos
    # NOTA: Asegúrate de que 'municipality_catalog' sea el nombre real de tu tabla en Supabase
    # (En tu diagrama vi 'catalogo_municipios_inegi', usa el nombre que tengas en la BD)
    companies = get_all_from('companies')
    mun_catalog = get_all_from('municipality_catalog') 
    cert_catalog = get_all_from('certifications_catalog')
    
    if not companies: return jsonify([]), 200

    df_comp = pd.DataFrame(companies)
    df_mun = pd.DataFrame(mun_catalog) if mun_catalog else pd.DataFrame()
    df_cert = pd.DataFrame(cert_catalog) if cert_catalog else pd.DataFrame()
    
    # 2. Mapeos
    mun_map = {}
    if not df_mun.empty:
        # Asegúrate de que las columnas 'id' y 'nombre_municipio' coincidan con tu tabla
        mun_map = dict(zip(df_mun['id'], df_mun['municipality_name']))
        
    cert_map = {}
    if not df_cert.empty:
        cert_map = dict(zip(df_cert['id'].astype(str), df_cert['acronym']))

    # 3. Transformaciones (COALESCE Municipio)
    def get_municipality(row):
        if pd.notnull(row.get('municipality_id')):
            mun_name = mun_map.get(row['municipality_id'])
            if mun_name: return mun_name
        
        other = row.get('other_municipality')
        if other and str(other).strip(): return str(other)
        return "Sin Dato"

    # 4. Transformaciones (Certificaciones Array)
    def get_certs_string(cert_ids):
        if not isinstance(cert_ids, list) or not cert_ids: return ""
        names = [cert_map.get(str(cid), str(cid)) for cid in cert_ids]
        return ", ".join(names)

    # Aplicamos
    df_comp['Municipio'] = df_comp.apply(get_municipality, axis=1)
    
    if 'certification_ids' in df_comp.columns:
        df_comp['Certificaciones (Limpias)'] = df_comp['certification_ids'].apply(get_certs_string)
    else:
        df_comp['Certificaciones (Limpias)'] = "-"

    # 5. 🔥 SELECCIÓN FINAL DE COLUMNAS (Aquí metemos las nuevas)
    # El orden en esta lista es el orden en que aparecerán en la tabla
    df_final = df_comp[[
        'clean_rfc', 
        'clean_legal_name',   # <--- NUEVO
        'trade_name', 
        'sector', 
        'main_activity',      # <--- NUEVO
        'full_address',       # <--- NUEVO
        'postal_code',        # <--- NUEVO
        'industrial_park',    # <--- NUEVO
        'Municipio', 
        'employee_count', 
        'procurement_tier', 
        'Certificaciones (Limpias)'
    ]].rename(columns={
        'clean_rfc': 'RFC',
        'clean_legal_name': 'Razón Social',       # Rename bonito
        'trade_name': 'Nombre Comercial',
        'sector': 'Sector',
        'main_activity': 'Actividad Principal',   # Rename bonito
        'full_address': 'Dirección',              # Rename bonito
        'postal_code': 'C.P.',                    # Rename bonito
        'industrial_park': 'Parque Industrial',   # Rename bonito
        'employee_count': 'Empleados',
        'procurement_tier': 'Nivel Proveeduría'
    })
    
    df_final = df_final.fillna('-')
    
    column_order = [
        'RFC', 'Razón Social', 'Nombre Comercial', 'Sector', 
        'Actividad Principal', 'Dirección', 'C.P.', 'Parque Industrial',
        'Municipio', 'Empleados', 'Nivel Proveeduría', 'Certificaciones (Limpias)'
    ]
    
    # Enviamos un objeto con data y metadata
    return jsonify({
        "data": df_final.to_dict(orient='records'),
        "columns": column_order
    }), 200

# --- VISTA 2: CONTACTOS LIMPIOS ---
@api_bp.route("/data/contacts-view", methods=['GET'])
@token_required
def get_contacts_view():
    from app.core.connections.supabase_service import get_all_from
    import pandas as pd
    
    # 1. Traer datos
    contacts = get_all_from('contacts')
    if not contacts: return jsonify([]), 200

    df = pd.DataFrame(contacts)
    
    # 2. Seleccionar y Renombrar (Para que se vea bonito en la tabla)
    # Ajusta los nombres de columnas a lo que quieras mostrar
    df_clean = df[[
        'first_name', 'last_name', 'clean_email', 'clean_position', 
        'company_phone_e164', 'personal_phone_e164'
    ]].rename(columns={
        'first_name': 'Nombre',
        'last_name': 'Apellidos',
        'clean_email': 'Correo',
        'clean_position': 'Cargo',
        'company_phone_e164': 'Tel. Oficina',
        'personal_phone_e164': 'Celular'
    })
    
    # Rellenar nulos
    df_clean = df_clean.fillna('-')
    
    column_order = [
        'Nombre', 'Apellidos', 'Correo', 'Cargo', 'Tel. Oficina', 'Celular'
    ]
    
    return jsonify({
        "data": df_clean.to_dict(orient='records'),
        "columns": column_order
    }), 200
    
# --- VISTA 3: RESPUESTAS (EL MONSTRUO DESEMPAQUETADO) ---
@api_bp.route("/data/responses-view", methods=['GET'])
@token_required
def get_responses_view():
    from app.core.connections.supabase_service import get_all_from
    import pandas as pd
    
    # 1. Traer TODAS las tablas necesarias
    responses = get_all_from('responses')
    companies = get_all_from('companies')
    contacts = get_all_from('contacts')
    cert_catalog = get_all_from('certifications_catalog')
    
    if not responses: return jsonify([]), 200

    # 2. Convertir a Pandas
    df_resp = pd.DataFrame(responses)
    df_comp = pd.DataFrame(companies)
    df_cont = pd.DataFrame(contacts)
    df_cert = pd.DataFrame(cert_catalog) if cert_catalog else pd.DataFrame()
    
    # 3. Mapeos (Diccionarios para velocidad)
    comp_map = dict(zip(df_comp['id'], df_comp['trade_name'])) if not df_comp.empty else {}
    
    # Mapeo de Contactos
    cont_map = {}
    if not df_cont.empty:
        df_cont['full_name'] = df_cont['first_name'].fillna('') + ' ' + df_cont['last_name'].fillna('')
        cont_map = dict(zip(df_cont['id'], df_cont['full_name']))

    # Mapeo de Certificaciones (ID -> Nombre)
    cert_name_map = {}
    if not df_cert.empty:
        cert_name_map = dict(zip(df_cert['id'].astype(str), df_cert['acronym']))

    # 4. Pre-calcular Certificaciones por Empresa (Company ID -> String "ISO9001, IATF...")
    company_certs_map = {}
    if not df_comp.empty and 'certification_ids' in df_comp.columns:
        for _, row in df_comp.iterrows():
            c_ids = row.get('certification_ids')
            if isinstance(c_ids, list) and c_ids:
                names = [cert_name_map.get(str(cid), str(cid)) for cid in c_ids]
                company_certs_map[row['id']] = ", ".join(names)
            else:
                company_certs_map[row['id']] = "-"

    # 5. Procesamiento Fila por Fila (Aplanar y Limpiar)
    flat_data = []
    
    for _, row in df_resp.iterrows():
        company_id = row.get('company_id')
        
        # --- A. PROCESAR CHECKBOXES ORIGINALES (ISO IDS) ---
        # El campo en DB suele ser 'iso_certification_ids' o similar (revisa tu DB)
        # Asumimos que viene como lista [1, 2] o string "1, 2"
        raw_iso_ids = row.get('iso_certification_ids') 
        iso_original_text = "-"
        
        if isinstance(raw_iso_ids, list) and raw_iso_ids:
            # Traducimos IDs a Nombres: [14, 16] -> "ISO9001, IATF16949"
            names = [cert_name_map.get(str(x), str(x)) for x in raw_iso_ids]
            iso_original_text = ", ".join(names)
        elif isinstance(raw_iso_ids, str) and raw_iso_ids.strip():
            # Por si viene como string "14,16"
            ids = raw_iso_ids.replace('{','').replace('}','').split(',')
            names = [cert_name_map.get(x.strip(), x.strip()) for x in ids]
            iso_original_text = ", ".join(names)

        # Objeto base
        base_obj = {
            "Fecha": str(row.get('response_date', ''))[:10],
            "Empresa": comp_map.get(company_id, 'ID Desconocido'),
            "Contacto": cont_map.get(row.get('contact_id'), 'ID Desconocido'),
            "¿Expansión?": "Sí" if row.get('has_expansion_plans') else "No",
            "¿Ingeniería?": "Sí" if row.get('has_engineering_area') else "No",
            
            # 🔥 COLUMNA 1: LO QUE CLICKEARON
            "Certs (Selección Original)": iso_original_text,
            
            # 🔥 COLUMNA 3: LA VERDAD FINAL (De la empresa)
            "Certificaciones (Limpias)": company_certs_map.get(company_id, "-")
        }
        
        # Desempaquetar 'additional_data'
        json_data = row.get('additional_data')
        if isinstance(json_data, dict):
            for question, answer in json_data.items():
                clean_q = question.replace('<strong>', '').replace('</strong>', '').strip()
                
                # 🔥 COLUMNA 2: LO QUE ESCRIBIERON
                if "otra certificación" in clean_q.lower():
                    clean_q = "Certs (Texto Original)"
                
                elif "proyecto de expansión" in clean_q.lower():
                    clean_q = "Proyecto Expansión (Detalle)"
                
                elif "Contact ID" in clean_q:
                    clean_q = "HubSpot ID"

                base_obj[clean_q] = str(answer)
        
        flat_data.append(base_obj)

    df_final = pd.DataFrame(flat_data)
    df_final = df_final.fillna('-')

    # 6. LIMPIEZA Y ORDENAMIENTO MAESTRO
    
    # Lista de columnas a ELIMINAR (Blacklist)
    cols_to_drop = [
        'Conversion Page', 
        'Conversion Title', 
        'Contact last name', 
        'Contact first name'
    ]
    df_final.drop(columns=cols_to_drop, errors='ignore', inplace=True)

    # Lista del ORDEN DESEADO (Whitelist)
    # Nota: Asegúrate de que los textos coincidan con cómo salen después del clean_q
    desired_order = [
        "Fecha", 
        "Empresa", 
        "Contacto", 
        "¿Expansión?", 
        "¿Ingeniería?", 
        "HubSpot Contact ID", 
        "Contact email", 
        "Proveeduría",
        "Certs (Selección Original)", # 1. Checkboxes traducidos
        "Certs (Texto Original)",     # 2. Input de texto libre
        "Certificaciones (Limpias)",  # 3. Resultado consolidado
        "Principales clientes",
        "Necesidades y problemáticas",
        "¿Capacidad de transformadores a adquirir?",
        "Principal producto o servicio que proporciona",
        "Proyecto Expansión (Detalle)",
        "¿Cuál es la demanda de electricidad esperada ma...",
        "¿Cuál es su demanda max actual de energia regis..."
    ]

    # Reordenar: Primero las deseadas (si existen), luego el resto (si sobró alguna col no mapeada)
    existing_cols = df_final.columns.tolist()
    final_cols = [c for c in desired_order if c in existing_cols]
    
    # Agregamos cualquier otra columna que haya sobrado (por si Hubspot manda algo nuevo)
    # y que no esté en la lista de borrar
    leftover_cols = [c for c in existing_cols if c not in final_cols and c not in cols_to_drop]
    
    df_final = df_final[final_cols + leftover_cols]

    # 7. Retornar Data + Column Order para el Frontend
    return jsonify({
        "data": df_final.to_dict(orient='records'),
        "columns": final_cols + leftover_cols # Le dice al frontend el orden explícito
    }), 200