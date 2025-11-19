import pandas as pd
import ast
import os
from app.pipelines.etl import cleaning as cleaner

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

def analyze_other_certifications(df_responses: pd.DataFrame) -> pd.DataFrame:
    """
    Takes the responses DataFrame and performs a detailed analysis on 'other certifications'.
    
    1. Extracts the raw text from the 'additional_data' JSONB column.
    2. Cleans the extracted text for analysis.
    3. Extracts known certification acronyms from the cleaned text.
    
    Returns a DataFrame ready for analysis or export.
    """
    if 'additional_data' not in df_responses.columns:
        print("Warning: 'additional_data' column not found in responses DataFrame.")
        return pd.DataFrame(columns=['clean_rfc', 'response_date', 'other_cert_text_clean', 'other_certifications'])

    # 1. Extract raw text from the JSONB field
    df_analysis = df_responses[['clean_rfc', 'response_date', 'additional_data']].copy()
    df_analysis['other_cert_text_raw'] = df_analysis['additional_data'].apply(_get_other_certification_text)

    # 2. Apply final text cleaning for analysis
    df_analysis['other_cert_text_clean'] = df_analysis['other_cert_text_raw'].apply(cleaner.clean_text_for_analysis)

    # 3. Extract acronyms based on the catalog
    df_analysis['other_certifications_acronyms'] = df_analysis['other_cert_text_clean'].apply(
        cleaner.extract_certifications_acronyms
    )

    # 4. Format acronyms into a comma-separated string for easy viewing in CSV
    df_analysis['other_certifications'] = df_analysis['other_certifications_acronyms'].apply(
        lambda acronyms: ', '.join(acronyms) if acronyms else ''
    )

    # 5. Select and return the final columns for the analysis output
    final_df = df_analysis[[
        'clean_rfc', 
        'response_date', 
        'other_cert_text_clean', 
        'other_certifications'
    ]].copy()

    return final_df