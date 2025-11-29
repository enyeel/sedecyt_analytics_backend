import os
from supabase import create_client, Client
import pandas as pd
import numpy as np
import unicodedata
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

print("Supabase client initialized.")

def get_all_from(table_name: str):
    """
    Recupera TODOS los registros de una tabla, superando el límite de 1000 de Supabase.
    """
    all_data = []
    page_size = 1000
    start = 0
    
    print(f"Fetching full data from '{table_name}'...")
    
    while True:
        try:
            # Pedimos un rango: del 0 al 999, luego 1000 a 1999...
            response = supabase.table(table_name).select("*").range(start, start + page_size - 1).execute()
            data = response.data
            
            if not data:
                break
                
            all_data.extend(data)
            
            # Si nos devolvió menos de lo que pedimos, es la última página
            if len(data) < page_size:
                break
                
            start += page_size
            
        except Exception as e:
            print(f"Error fetching data from {table_name} (range {start}): {e}")
            return {"error": f"Could not fetch data from {table_name}"}
            
    print(f"  -> Total fetched from {table_name}: {len(all_data)}")
    return all_data

def _clean_value(v):
    """
    Función recursiva para limpiar valores individuales, listas o diccionarios
    de tipos NumPy incompatibles con JSON.
    """
    # 1. Si es un entero de NumPy (int64, int32, etc)
    if isinstance(v, (np.integer, np.int64)):
        return int(v)
    
    # 2. Si es un flotante de NumPy
    elif isinstance(v, (np.floating, np.float64)):
        return None if np.isnan(v) else float(v)
    
    # 3. Si es una LISTA (Aquí estaba el error antes)
    elif isinstance(v, list):
        return [_clean_value(item) for item in v]
    
    # 4. Si es un DICCIONARIO
    elif isinstance(v, dict):
        return {k: _clean_value(val) for k, val in v.items()}
    
    # 5. Manejo de NaN standard de Pandas/Python
    elif pd.isna(v):
        return None
    
    # 6. Si es un float nativo de Python
    if isinstance(v, (int, np.integer)):
        return int(v)
    
    # 7. Si es un float nativo de Python
    if isinstance(v, (float, np.float64)):
        # Truco: si es 11.0, lo baja a 11. Si es 11.5, lo deja pasar (o falla en BD)
        if v.is_integer():
            return int(v)
        return v
    
    # 8. Todo lo demás (str, int nativo, bool) se queda igual
    return v

ACCENT_MAP = str.maketrans("ÁÉÍÓÚÜÑ", "AEIOUUN")

def normalize_text(text):
    """
    Normalización estándar: Mayúsculas, sin acentos (NFKD), 
    sin puntuación y con espacios simples.
    """
    if not text: return ""
    text = str(text).upper()
    text = unicodedata.normalize('NFKD', text)
    text = text.encode('ASCII', 'ignore').decode('utf-8')
    text = text.replace('.', '').replace(',', '').replace('/', '')
    return " ".join(text.split())

def get_municipalities_map():
    """
    Descarga el catálogo completo de municipios (paginado) y construye 
    un mapa optimizado { 'NOMBRE LIMPIO': ID }.
    """
    try:
        print("📡 Descargando catálogo de municipios...")
        
        master_map = {}
        current_batch = 0
        batch_size = 1000
        
        while True:
            # Paginación para evitar límites de Supabase
            response = supabase.table('municipality_catalog')\
                .select('id, municipality_name, keywords')\
                .range(current_batch * batch_size, (current_batch + 1) * batch_size - 1)\
                .execute()
            
            batch_data = response.data
            if not batch_data:
                break 
                
            for item in batch_data:
                mun_id = item['id']
                
                # 1. Mapear Nombre Oficial
                clean_official = normalize_text(item['municipality_name'])
                if clean_official:
                    master_map[clean_official] = mun_id

                # 2. Mapear Keywords
                keywords = item.get('keywords')
                if keywords and isinstance(keywords, list):
                    for kw in keywords:
                        clean_kw = normalize_text(kw)
                        if clean_kw:
                            master_map[clean_kw] = mun_id

            # Si el lote es menor al límite, ya terminamos
            if len(batch_data) < batch_size:
                break
                
            current_batch += 1

        print(f"✅ Mapa de municipios cargado y listo ({len(master_map)} referencias).")
        return master_map

    except Exception as e:
        print(f"❌ Error crítico descargando el catálogo de municipios: {e}")
        return {}

def upload_dataframe_to_supabase(df: pd.DataFrame, table_name: str, on_conflict_col: str = None):
    """
    Sube un DF a Supabase, limpiando recursivamente tipos NumPy y fechas.
    """
    if df.empty:
        print(f"❌ The DataFrame for {table_name} is empty. Nothing to upload.")
        return

    # 1. Convertir Timestamps a string ISO
    df_formatted = df.copy()
    for col in df_formatted.select_dtypes(include=['datetime64[ns]', 'datetimetz']).columns:
        df_formatted[col] = df_formatted[col].dt.strftime('%Y-%m-%d %H:%M:%S')

    # 2. Convertir a lista de diccionarios
    records_to_upload = df_formatted.to_dict(orient='records')

    # 3. Limpieza PROFUNDA
    final_records = []
    for record in records_to_upload:
        # IMPORTANTE: Asegúrate de tener importada o definida _clean_value
        clean_rec = {k: _clean_value(v) for k, v in record.items()}
        final_records.append(clean_rec)

    print(f"Preparing to upload {len(final_records)} records to '{table_name}'...")

    try:
        # 4. Subida con Upsert (CORREGIDO: Usamos final_records en ambos casos)
        if on_conflict_col:
            # Si hay columna de conflicto, la usamos
            query = supabase.table(table_name).upsert(final_records, on_conflict=on_conflict_col)
        else:
            # Si no, upsert normal (por ID)
            query = supabase.table(table_name).upsert(final_records)
        
        data, count = query.execute()
        print(f"✅ Successfully uploaded to '{table_name}'.")

    except Exception as e:
        print(f"❌ An error occurred during the upload to {table_name}: {e}")
        # Debug avanzado
        if final_records:
            print("   DEBUG: First record keys:", final_records[0].keys())