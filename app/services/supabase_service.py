import os
from supabase import create_client, client
import pandas as pd
from dotenv import load_dotenv

load_dotenv()  # Carga las variables de entorno desde el archivo .env

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase: client.Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

print("Supabase client initialized.")

def get_all_from(tabale_name: str):
    try:
        response = supabase.table(tabale_name).select("*").execute()
    
        return response.data
    except Exception as e:
        print(f"Error fetching data from {tabale_name}: {e}")
        return {"error": f"Could not fetch data from {tabale_name}"}

def upload_dataframe_to_supabase(df: pd.DataFrame, table_name: str):
    """
    Uploads a pandas DataFrame to a specified Supabase table.

    Args:
        df (pd.DataFrame): The DataFrame to upload.
        table_name (str): The name of the target table in Supabase.
    """
    if df.empty:
        print("❌ The DataFrame is empty. Nothing to upload.")
        return

    # Convert DataFrame to a list of dictionaries, which is the format Supabase client expects.
    records_to_upload = df.to_dict(orient='records')

    print(f"Preparing to upload {len(records_to_upload)} records to the '{table_name}' table...")

    try:
        # The insert method can take a list of dictionaries directly for a bulk insert.
        data, count = supabase.table(table_name).upsert(records_to_upload).execute()
        
        response_data = data[1]
        
        if len(response_data) == len(records_to_upload):
            print(f"✅ Successfully uploaded {len(response_data)} records to '{table_name}'.")
        else:
            print(f"⚠️ Warning: Upload might be incomplete. Expected {len(records_to_upload)}, but Supabase reported inserting {len(response_data)}.")

    except Exception as e:
        print(f"❌ An error occurred during the upload: {e}")
        print("\nTroubleshooting tips:")
        print("1. Ensure your Supabase credentials in `.env` are correct.")
        print("2. Check that the target table exists and its column names match the DataFrame.")