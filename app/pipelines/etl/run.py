from dotenv import load_dotenv
import json
import os
import pandas as pd
import numpy as np 
from thefuzz import process, fuzz

# Import services
from app.core.connections.google_sheets_service import read_worksheet_as_dataframe
from app.pipelines.etl.processing import clean_and_process_data
from app.pipelines.etl.certifications import analyze_other_certifications
from app.core.connections import supabase_service
from config.certifications_catalog_data import CERTIFICATIONS_CATALOG

def load_config(file_path='config/cleaning_map.json'):
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)
    
def get_id_map(table_name, key_column):
    print(f"Fetching ID map for table '{table_name}'...")
    try:
        response = supabase_service.supabase.table(table_name).select(f"id, {key_column}").execute()
        data = response.data
        return {row[key_column]: row['id'] for row in data}
    except Exception as e:
        print(f"Error fetching map for {table_name}: {e}")
        return {}
    
def find_cert_id(text, catalog_df):
    """Busca el ID de una certificación dado un texto."""
    if not text or text in ['OTRAS', 'NULL', '']: return None
    text_upper = str(text).upper().strip()
    
    # 1. Match directo (Acrónimo o Nombre)
    match = catalog_df[
        (catalog_df['acronym'] == text_upper) | 
        (catalog_df['name'].str.upper() == text_upper)
    ]
    if not match.empty:
        return int(match.iloc[0]['id'])
    
    # 2. Match por Keywords
    for _, row in catalog_df.iterrows():
        keywords = [k.upper() for k in row['search_keywords']]
        if text_upper in keywords:
            return int(row['id'])
            
    return None

def convert_checkboxes_to_ids(cert_string, db_cert_catalog):
    """Convierte lista de strings de checkboxes a lista de IDs."""
    if not isinstance(cert_string, list): return []
    ids = set()
    for cert_text in cert_string:
        found_id = find_cert_id(cert_text, db_cert_catalog)
        if found_id:
            ids.add(found_id)
    return list(ids)

def run_etl_process():
    print("--- Inicio del ETL SEDECyT Analytics ---")

    # ---------------------------------------------------------
    # Step 0: Catálogos
    # ---------------------------------------------------------
    print("\nStep 0: Syncing Certifications Catalog...")
    df_catalog = pd.DataFrame(CERTIFICATIONS_CATALOG)
    supabase_service.upload_dataframe_to_supabase(df_catalog, 'certifications_catalog', on_conflict_col='name')
    
    # Descargar catálogo con IDs reales
    db_cert_catalog = pd.DataFrame(supabase_service.get_all_from('certifications_catalog'))
    
    # ---------------------------------------------------------
    # Step 1 & 2: Extracción y Limpieza Base
    # ---------------------------------------------------------
    print("\nStep 1 & 2: Extract and Transform...")
    df_raw = read_worksheet_as_dataframe("Formulario Desarrollo Industria")
    config = load_config()
    processed_data = clean_and_process_data(df_raw, config) 
    
    # Asegurar fechas correctas
    processed_data['responses']['response_date'] = pd.to_datetime(processed_data['responses']['response_date'])

    # ---------------------------------------------------------
    # Step 2.5: Procesamiento de Certificaciones (HISTORIAL COMPLETO)
    # ---------------------------------------------------------
    print("\nStep 2.5: Processing Certifications (Full History)...")
    
    # A) IDs de Texto Libre (Para todas las filas)
    # analyze_other_certifications devuelve un DF con 'other_certifications_ids'
    df_cert_analysis_full = analyze_other_certifications(
        processed_data['responses'], 
        db_cert_catalog.to_dict('records')
    )
    
    # Pegamos esos IDs al dataframe principal de respuestas
    processed_data['responses'] = processed_data['responses'].merge(
        df_cert_analysis_full[['clean_rfc', 'response_date', 'other_certifications_ids']], 
        on=['clean_rfc', 'response_date'], 
        how='left'
    )

    # B) IDs de Checkboxes (Para todas las filas)
    # Nota: Usamos iso_certifications que viene limpio en 'companies' o 'responses'
    # Si iso_certifications está en companies (por tu cleaning map), necesitamos traerlo a responses
    # Asumiremos que tu cleaning map lo pone en companies, así que lo mapeamos por RFC temporalmente
    # o mejor, si el raw tenía esa columna, debió procesarse. 
    
    # *FIX RÁPIDO:* Recalculamos 'iso_certifications' desde el RAW para asegurar que esté en responses fila x fila
    # O usamos el que ya tienes en companies si es 1 a 1. 
    # Para ser precisos con el historial, vamos a procesar la columna 'iso_certifications' si existe en responses.
    # Si no existe en responses (porque el map lo mandó a companies), usamos un truco:
    
    if 'iso_certifications' not in processed_data['responses'].columns:
        # Traemos la columna raw original y la limpiamos aquí rápido para tener el histórico
        # Ojo: Esto asume que la columna se llama "Certificaciones ISO " en el excel
        col_name = "Certificaciones ISO " 
        if col_name in df_raw.columns:
            from app.pipelines.etl.cleaning import clean_certifications_to_array
            processed_data['responses']['iso_certifications'] = df_raw[col_name].apply(clean_certifications_to_array)
    
    # Ahora sí convertimos a IDs
    processed_data['responses']['iso_certification_ids'] = processed_data['responses']['iso_certifications'].apply(
        lambda x: convert_checkboxes_to_ids(x, db_cert_catalog)
    )

    # ---------------------------------------------------------
    # Step 3: Upload Master Tables (LATEST SNAPSHOT)
    # ---------------------------------------------------------
    print("\nStep 3: Uploading Master Tables (Latest Snapshot)...")
    
    # 1. Ordenar por fecha y tomar la última respuesta por empresa
    df_latest_snapshot = processed_data['responses'].sort_values('response_date', ascending=False).drop_duplicates(subset=['clean_rfc'], keep='first')
    
    # 2. Preparar el DF de Companies
    # Tomamos la base limpia de companies
    df_companies = processed_data['companies'].copy()
    
    # 3. Traer los IDs de certificaciones DEL SNAPSHOT
    # Hacemos merge con el snapshot que ya tiene los IDs calculados (Paso 2.5)
    df_companies = df_companies.merge(
        df_latest_snapshot[['clean_rfc', 'iso_certification_ids', 'other_certifications_ids']],
        on='clean_rfc',
        how='left'
    )
    
    # 4. Crear la columna MAESTRA (Unión de ambos)
    def merge_cert_lists(row):
        ids_checkbox = row['iso_certification_ids'] if isinstance(row['iso_certification_ids'], list) else []
        ids_text = row['other_certifications_ids'] if isinstance(row['other_certifications_ids'], list) else []
        return list(set(ids_checkbox + ids_text))

    df_companies['certification_ids'] = df_companies.apply(merge_cert_lists, axis=1)
    
    # Subir Companies
    # Excluimos columnas temporales para no ensuciar, pero mandamos certification_ids
    cols_companies = [c for c in df_companies.columns if c not in ['iso_certification_ids', 'other_certifications_ids']]
    supabase_service.upload_dataframe_to_supabase(df_companies[cols_companies], 'companies', on_conflict_col='clean_rfc')
    
    # Subir Contacts
    supabase_service.upload_dataframe_to_supabase(processed_data['contacts'], 'contacts', on_conflict_col='clean_email')

    # ---------------------------------------------------------
    # Step 4: Foreign Keys
    # ---------------------------------------------------------
    print("\nStep 4: Fetching Foreign Keys...")
    company_map = get_id_map('companies', 'clean_rfc')
    contact_map = get_id_map('contacts', 'clean_email')

    # ---------------------------------------------------------
    # Step 5 & 6: Upload Responses (History)
    # ---------------------------------------------------------
    print("\nStep 5 & 6: Uploading Responses (History)...")
    df_responses = processed_data['responses'].copy()
    
    # Map FKs
    df_responses['company_id'] = df_responses['clean_rfc'].map(company_map)
    df_responses['contact_id'] = df_responses['clean_email'].map(contact_map)
    
    # Renombrar para coincidir con BD
    df_responses.rename(columns={'other_certifications_ids': 'other_certifications'}, inplace=True)
    
    # Limpieza final de nulos en listas
    for col in ['other_certifications', 'iso_certification_ids']:
        df_responses[col] = df_responses[col].apply(lambda x: x if isinstance(x, list) else [])

    # Filtrar y Subir
    cols_to_upload = [
        'company_id', 'contact_id', 'response_date', 
        'has_expansion_plans', 'has_engineering_area', 
        'additional_data', 'other_certifications', 'iso_certification_ids'
    ]
    # Validar que las columnas existan
    final_cols = [c for c in cols_to_upload if c in df_responses.columns]
    
    # Validar FKs
    df_responses = df_responses.dropna(subset=['company_id'])
    
    supabase_service.upload_dataframe_to_supabase(df_responses[final_cols], 'responses', on_conflict_col='company_id, response_date')

    print("\n✅ ETL Completo: Snapshot maestro y Historial de respuestas sincronizados.")

if __name__ == '__main__':
    load_dotenv()
    run_etl_process()