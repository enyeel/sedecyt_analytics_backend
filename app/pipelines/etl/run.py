from dotenv import load_dotenv
import json
import os
import pandas as pd

# Import the new modularized services
from app.core.connections.google_sheets_service import read_worksheet_as_dataframe
from app.pipelines.etl.processing import clean_and_process_data
from app.pipelines.etl.certifications import analyze_other_certifications
from app.core.connections import supabase_service
from app.core.connections.supabase_service import upload_dataframe_to_supabase 
from config.certifications_catalog_data import CERTIFICATIONS_CATALOG

output_dir = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'data','outputs')
os.makedirs(output_dir, exist_ok=True)

def load_config(file_path='config/cleaning_map.json'):
    """Loads and returns the configuration dictionary from a JSON file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)
    
def get_id_map(table_name, key_column):
    """
    Helper para traer IDs de Supabase.
    Retorna un dict: { 'RFC_VALOR': 123, ... }
    """
    print(f"Fetching ID map for table '{table_name}'...")
    try:
        # Seleccionamos solo ID y la columna llave para ser eficientes
        response = supabase_service.supabase.table(table_name).select(f"id, {key_column}").execute()
        data = response.data
        return {row[key_column]: row['id'] for row in data}
    except Exception as e:
        print(f"Error fetching map for {table_name}: {e}")
        return {}

def run_etl_process():
    print("--- Inicio del ETL SEDECyT Analytics ---")

    # 1. Cargar Catálogo de Certificaciones (Primero, para tener IDs)
    # -------------------------------------------------------------
    print("\nStep 0: Syncing Certifications Catalog...")
    # Subimos el catálogo estático que tienes en el código
    df_catalog = pd.DataFrame(CERTIFICATIONS_CATALOG)
    # Upsert basado en 'acronym' para no duplicar
    upload_dataframe_to_supabase(df_catalog, 'certifications_catalog') 
    
    # Traemos el catálogo fresco con sus IDs de BD
    db_cert_catalog = supabase_service.get_all_from('certifications_catalog')
    
    # 2. Extracción y Limpieza Básica
    # -------------------------------------------------------------
    print("\nStep 1 & 2: Extract and Transform...")
    df_raw = read_worksheet_as_dataframe("Formulario Desarrollo Industria")
    config = json.load(open('config/cleaning_map.json', 'r', encoding='utf-8'))
    
    # Esto nos da los DFs limpios pero SIN IDs relacionales aún
    processed_data = clean_and_process_data(df_raw, config) 
    
    # 3. Subir Tablas Maestras (Companies y Contacts)
    # -------------------------------------------------------------
    # Catalogos: Usamos on_conflict para que si existe el nombre/rfc, solo actualice (o ignore) y no falle.

    # Para companies, la llave única es 'clean_rfc'
    upload_dataframe_to_supabase(processed_data['companies'], 'companies', on_conflict_col='clean_rfc')
    
    # Para contacts, la llave única es 'clean_email'
    upload_dataframe_to_supabase(processed_data['contacts'], 'contacts', on_conflict_col='clean_email')
    
    # Para certifications_catalog (si lo subes), la llave es 'name'
    # upload_dataframe_to_supabase(df_cert_catalog, 'certifications_catalog', on_conflict_col='name')
    
    # 4. Recuperar los IDs generados (El truco de magia)
    # -------------------------------------------------------------
    print("\nStep 4: Fetching generated Foreign Keys...")
    company_map = get_id_map('companies', 'clean_rfc')
    contact_map = get_id_map('contacts', 'clean_email')

    # 5. Preparar Tabla de Responses (Unión de FKs y Certs)
    # -------------------------------------------------------------
    print("\nStep 5: Assembling Responses Table...")
    df_responses = processed_data['responses'].copy()

    # A) Mapear Foreign Keys
    # Usamos .map para traducir clean_rfc -> id
    df_responses['company_id'] = df_responses['clean_rfc'].map(company_map)
    df_responses['contact_id'] = df_responses['clean_email'].map(contact_map)

    # B) Validación rápida: Descartar filas huérfanas si falló el join
    missing_companies = df_responses['company_id'].isna().sum()
    if missing_companies > 0:
        print(f"⚠️ Warning: {missing_companies} responses could not be linked to a company.")
        df_responses = df_responses.dropna(subset=['company_id'])

    # C) Análisis de Certificaciones (Obtener IDs)
    # Pasamos el catálogo de BD para que busque los IDs reales
    df_cert_analysis = analyze_other_certifications(df_responses, db_cert_catalog)
    
    # Pegar el resultado (lista de IDs) al dataframe principal
    # Asumiendo que el orden se mantiene o haciendo un merge por clean_rfc
    # Para seguridad, hagamos merge:
    df_responses = df_responses.merge(
        df_cert_analysis[['clean_rfc', 'other_certifications_ids']], 
        on='clean_rfc', 
        how='left'
    )

    # Renombrar la columna para que coincida con Supabase (other_certifications es jsonb)
    df_responses.rename(columns={'other_certifications_ids': 'other_certifications'}, inplace=True)

    # Asegurarse de que sea una lista válida para JSONB (reemplazar NaN con [])
    df_responses['other_certifications'] = df_responses['other_certifications'].apply(
        lambda x: x if isinstance(x, list) else []
    )

    # 6. Subir Responses
    # -------------------------------------------------------------
    print("\nStep 6: Uploading Responses...")
    
    # Seleccionar solo las columnas que existen en la tabla de Supabase
    # clean_rfc y clean_email ya no se necesitan, usamos los IDs
    cols_to_upload = [
        'company_id', 'contact_id', 'response_date', 
        'has_expansion_plans', 'has_engineering_area', 
        'additional_data', 'other_certifications' # 'iso_certification_ids' si lo tienes
    ]
    
    # Filtrar columnas que realmente tenemos en el DF
    final_cols = [c for c in cols_to_upload if c in df_responses.columns]
    
    upload_dataframe_to_supabase(df_responses[final_cols], 'responses')

    print("\n✅ ETL Completo: Tabla responses cargada correctamente.")

if __name__ == '__main__':
    load_dotenv()
    run_etl_process()