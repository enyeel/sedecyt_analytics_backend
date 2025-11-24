import os
from supabase import create_client, Client
import pandas as pd
from dotenv import load_dotenv

load_dotenv()  # Carga las variables de entorno desde el archivo .env

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

print("Supabase client initialized.")

def get_all_from(tabale_name: str):
    try:
        response = supabase.table(tabale_name).select("*").execute()
    
        return response.data
    except Exception as e:
        print(f"Error fetching data from {tabale_name}: {e}")
        return {"error": f"Could not fetch data from {tabale_name}"}

def upload_dataframe_to_supabase(df: pd.DataFrame, table_name: str, on_conflict_col: str = None):
    """
    Sube un DF a Supabase.
    Args:
        df: DataFrame a subir.
        table_name: Tabla destino.
        on_conflict_col: (Opcional) Nombre de la columna única para upsert.
    """
    if df.empty:
        print(f"❌ The DataFrame for {table_name} is empty. Nothing to upload.")
        return

    # 1. Convertir Timestamps a string ISO (Evita error JSON date)
    df_formatted = df.copy()
    for col in df_formatted.select_dtypes(include=['datetime64[ns]', 'datetimetz']).columns:
        df_formatted[col] = df_formatted[col].dt.strftime('%Y-%m-%d %H:%M:%S')

    # 2. Convertir a lista de diccionarios
    records_to_upload = df_formatted.to_dict(orient='records')

    # 3. Limpieza SEGURA de NaNs (CORRECCIÓN AQUÍ) 🛠️
    # Iteramos explícitamente para manejar listas sin romper pd.isna()
    final_records = []
    for record in records_to_upload:
        clean_rec = {}
        for k, v in record.items():
            # A. Si es lista o dict (ej: search_keywords), pasa directo. 
            # (Las listas no pueden ser NaN en este contexto)
            if isinstance(v, (list, dict)):
                clean_rec[k] = v
            # B. Si es un valor simple, revisamos si es NaN
            elif pd.isna(v):
                clean_rec[k] = None
            # C. Si no, es un valor normal
            else:
                clean_rec[k] = v
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