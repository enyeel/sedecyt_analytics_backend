import pandas as pd
import ast
import os
from app.pipelines.etl import cleaning as cleaner
from config.certifications_catalog_data import CERTIFICATIONS_CATALOG

# 1. Crear un mapa de Acrónimo -> ID (Simulado o traído de BD)
# NOTA: Idealmente esto se trae de Supabase, pero como tienes el archivo de config local,
# podemos usarlo para mapear si asumimos que el orden/IDs coinciden o si subes el catálogo primero.
# Para hacerlo robusto, buscaremos por el 'acronym' que definiste en el config.

def map_acronyms_to_ids(found_acronyms: list, catalog_map: dict) -> list:
    """Convierte lista de acrónimos ['ISO9001'] a IDs [1]"""
    if not found_acronyms:
        return []
    
    # Mapear y filtrar nulos (si un acrónimo no está en el mapa)
    ids = [catalog_map.get(acr) for acr in found_acronyms]
    return [i for i in ids if i is not None]

def _get_other_certification_text(json_str: str) -> str:
    """
    Extracts the free-text value for 'other certifications' from a JSON string.
    Handles CSV parsing quirks and ensures safe evaluation.
    """
    if pd.isna(json_str) or not str(json_str).strip():
        return ''

    cleaned_str = str(json_str).strip()
    
    # Handle cases where the string is quoted (common from DataFrame to_csv/to_dict)
    if cleaned_str.startswith('"') and cleaned_str.endswith('"'):
        cleaned_str = cleaned_str[1:-1].replace("''", "'") # Fix for escaped quotes

    try:
        # ast.literal_eval is the safe way to parse a string literal of a Python object
        data_dict = ast.literal_eval(cleaned_str)
        
        key = 'En caso de contar con otra certificación, especificar.'
        
        if isinstance(data_dict, dict):
            return str(data_dict.get(key, '')).strip()
            
        return ''
    except (ValueError, SyntaxError):
        # This will catch malformed strings that aren't valid Python literals
        return ''

def analyze_other_certifications(df_responses: pd.DataFrame, db_catalog_data: list) -> pd.DataFrame:
    """
    Analiza y devuelve el DataFrame enriquecido con IDs de certificaciones.
    Recibe db_catalog_data: Lista de dicts [{'id': 1, 'acronym': 'ISO9001'}, ...] traída de Supabase.
    """
    # Crear mapa { 'ISO9001': 1, 'ISO14001': 2 }
    acronym_to_id_map = {item['acronym']: item['id'] for item in db_catalog_data}
    
    if 'additional_data' not in df_responses.columns:
        print("Warning: 'additional_data' column not found in responses DataFrame.")
        return pd.DataFrame(columns=['clean_rfc', 'response_date', 'other_cert_text_clean', 'other_certifications'])

    # 1. Extract raw text from the JSONB field
    df_analysis = df_responses[['clean_rfc', 'response_date', 'additional_data']].copy()
    df_analysis['other_cert_text_raw'] = df_analysis['additional_data'].apply(_get_other_certification_text)
    df_analysis['other_cert_text_clean'] = df_analysis['other_cert_text_raw'].apply(cleaner.clean_text_for_analysis)

    # 1. Extraer Acrónimos (Strings)
    df_analysis['found_acronyms'] = df_analysis['other_cert_text_clean'].apply(
        cleaner.extract_certifications_acronyms
    )

    # 2. NUEVO: Convertir Acrónimos a IDs usando el mapa
    df_analysis['other_certifications_ids'] = df_analysis['found_acronyms'].apply(
        lambda x: map_acronyms_to_ids(x, acronym_to_id_map)
    )

    return df_analysis