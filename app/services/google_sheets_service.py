import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
import config

# Alcances de la API (se puede quedar aquí)
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def get_gspread_client():
    """
    Autentica usando la ruta de credenciales del archivo config.
    """
    # 2. Usa la ruta de las credenciales desde el config
    creds = Credentials.from_service_account_file(config.CREDENTIALS_PATH, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client


def read_worksheet_as_dataframe(worksheet_name: str) -> pd.DataFrame:
    """
    Lee una hoja de cálculo completa usando el ID del archivo config.
    Solo necesita saber el nombre de la pestaña a leer.
    """
    # 3. Verifica y usa el SHEET_ID desde el config
    if not config.SHEET_ID:
        raise ValueError("La variable de entorno SPREADSHEET_ID no está configurada.")
    
    client = get_gspread_client()
    sheet = client.open_by_key(config.SHEET_ID)
    worksheet = sheet.worksheet(worksheet_name)

    data = worksheet.get_all_records()
    df = pd.DataFrame(data)
    return df

