# google_sheets_service.py

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
import os

# Alcances de la API (lectura/escritura en Google Sheets)
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def get_gspread_client(credentials_path: str):
    """
    Autentica con Google Sheets API y devuelve un cliente gspread.
    """
    creds = Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client


def read_worksheet_as_dataframe(credentials_path: str, sheet_id: str, worksheet_name: str) -> pd.DataFrame:
    """
    Lee una worksheet completa y la devuelve como DataFrame.

    :param credentials_path: Ruta al archivo JSON de credenciales.
    :param sheet_id: ID único del Google Sheet (de la URL).
    :param worksheet_name: Nombre de la pestaña dentro del Google Sheet.
    """
    client = get_gspread_client(credentials_path)
    sheet = client.open_by_key(sheet_id)
    worksheet = sheet.worksheet(worksheet_name)

    data = worksheet.get_all_records()
    df = pd.DataFrame(data)
    return df


def read_range_as_dataframe(credentials_path: str, sheet_id: str, worksheet_name: str, cell_range: str) -> pd.DataFrame:
    """
    Lee un rango específico de celdas y lo devuelve como DataFrame.

    :param credentials_path: Ruta al archivo JSON de credenciales.
    :param sheet_id: ID único del Google Sheet.
    :param worksheet_name: Nombre de la pestaña.
    :param cell_range: Rango en notación A1 (ej. "A1:F500").
    """
    client = get_gspread_client(credentials_path)
    sheet = client.open_by_key(sheet_id)
    worksheet = sheet.worksheet(worksheet_name)

    values = worksheet.get(cell_range)  # Devuelve una lista de listas
    if not values:
        return pd.DataFrame()  # DataFrame vacío si no hay datos

    df = pd.DataFrame(values[1:], columns=values[0])  # Fila 0 = headers
    return df


def list_worksheets(credentials_path: str, sheet_id: str) -> list:
    """
    Lista los nombres de todas las worksheets disponibles en un Google Sheet.
    """
    client = get_gspread_client(credentials_path)
    sheet = client.open_by_key(sheet_id)
    return [ws.title for ws in sheet.worksheets()]


def save_user_data(credentials_path: str, sheet_id: str, worksheet_name: str, user_data: list):
    """
    Guarda los datos de un usuario en la worksheet especificada.

    :param credentials_path: Ruta al archivo JSON de credenciales.
    :param sheet_id: ID único del Google Sheet.
    :param worksheet_name: Nombre de la pestaña.
    :param user_data: Diccionario con los datos del usuario (debe coincidir con los headers).
    """
    try:
            client = get_gspread_client(credentials_path)
            sheet = client.open_by_key(sheet_id)
            worksheet = sheet.worksheet(worksheet_name)
            worksheet.append_row(user_data, value_input_option='USER_ENTERED')
            print(f"Fila agregada exitosamente en '{worksheet_name}': {user_data}")
            return True
    except Exception as e:
        print(f"Error al agregar la fila: {e}")
        return False