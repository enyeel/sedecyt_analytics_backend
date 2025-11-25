import os
from supabase import create_client, Client
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

print("Supabase client initialized.")

def get_all_from(table_name: str):
    try:
        response = supabase.table(table_name).select("*").execute()
        return response.data
    except Exception as e:
        print(f"Error fetching data from {table_name}: {e}")
        return {"error": f"Could not fetch data from {table_name}"}

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
    
    # 6. Todo lo demás (str, int nativo, bool) se queda igual
    return v

def upload_dataframe_to_supabase(df: pd.DataFrame, table_name: str, on_conflict_col: str = None):
    """
    Sube un DF a Supabase, limpiando recursivamente tipos NumPy y fechas.
    """
    if df.empty:
        print(f"❌ The DataFrame for {table_name} is empty. Nothing to upload.")
        return

    # 1. Convertir Timestamps a string ISO (Pandas lo hace rápido en bloque)
    df_formatted = df.copy()
    for col in df_formatted.select_dtypes(include=['datetime64[ns]', 'datetimetz']).columns:
        df_formatted[col] = df_formatted[col].dt.strftime('%Y-%m-%d %H:%M:%S')

    # 2. Convertir a lista de diccionarios
    records_to_upload = df_formatted.to_dict(orient='records')

    # 3. Limpieza PROFUNDA usando la función auxiliar
    final_records = []
    for record in records_to_upload:
        # Aplicamos _clean_value a cada valor del diccionario
        clean_rec = {k: _clean_value(v) for k, v in record.items()}
        final_records.append(clean_rec)

    print(f"Preparing to upload {len(final_records)} records to '{table_name}'...")

    try:
        # 4. Subida con Upsert
        query = supabase.table(table_name).upsert(final_records)
        
        if on_conflict_col:
            query = supabase.table(table_name).upsert(final_records, on_conflict=on_conflict_col)
        
        data, count = query.execute()
        print(f"✅ Successfully uploaded to '{table_name}'.")

    except Exception as e:
        print(f"❌ An error occurred during the upload to {table_name}: {e}")
        # Debug avanzado: Imprimir el tipo de dato del primer elemento de una lista si falla
        if final_records:
            for k, v in final_records[0].items():
                if isinstance(v, list) and len(v) > 0:
                    print(f"   DEBUG: List column '{k}' contains types: {[type(x) for x in v]}")