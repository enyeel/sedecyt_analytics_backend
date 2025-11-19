import pandas as pd
from app.pipelines.etl import cleaning as cleaner

def clean_and_process_data(df: pd.DataFrame, config: dict) -> dict:
    """
    Applies cleaning functions, finalizes critical IDs, and structures data into tables.
    
    Args:
        df: The raw DataFrame from the source (e.g., Google Sheets).
        config: The configuration dictionary from cleaning_map.json.

    Returns:
        A dictionary containing three DataFrames: 'companies', 'contacts', and 'responses'.
    """
    # 1. Initial Cleaning: Apply cleaning function for each column defined in the map.
    df_clean = _apply_initial_cleaning(df, config)
    
    # 2. Post-Processing: Apply complex logic that depends on multiple columns.
    df_clean = _finalize_company_ids(df_clean)
    df_clean = _rescue_contact_names(df_clean)
    
    # 3. JSONB Creation: Consolidate extra data into a single JSONB column.
    df_clean = _create_jsonb_column(df, df_clean, config)
    
    # 4. Structuring: Split the cleaned data into final DataFrames for each table.
    processed_data = _structure_data_into_tables(df_clean, config)
    
    return processed_data

def _apply_initial_cleaning(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Iterates through the cleaning_map and applies the specified cleaning function."""
    df_clean = pd.DataFrame(index=df.index)
    
    for original_col, params in config['cleaning_map'].items():
        target_col = params['target_db_col']
        clean_func_name = params['clean_func']
        
        if original_col not in df.columns:
            print(f"Warning: Source column '{original_col}' not found in DataFrame. Skipping.")
            continue

        try:
            clean_function = getattr(cleaner, clean_func_name)
            df_clean[target_col] = df[original_col].apply(clean_function)
        except AttributeError:
            print(f"ERROR: Cleaning function '{clean_func_name}' not found. Copying data as-is.")
            df_clean[target_col] = df[original_col]
            
    return df_clean

def _finalize_company_ids(df_clean: pd.DataFrame) -> pd.DataFrame:
    """Generates a unique ID for companies where the RFC cleaning failed."""
    print("Finalizing company IDs for entries with failed RFC cleaning...")
    mask_failed_rfc = df_clean['clean_rfc'].str.startswith('ID_FALLO', na=False)
    
    if mask_failed_rfc.any():
        trade_name_formatted = (
            df_clean.loc[mask_failed_rfc, 'trade_name'].astype(str)
                .str.replace(r'[^\w\s]', '', regex=True)
                .str.strip()
                .str.replace(r'\s+', '-', regex=True)
        )
        df_clean.loc[mask_failed_rfc, 'clean_rfc'] += '_' + trade_name_formatted
        
    return df_clean

def _rescue_contact_names(df_clean: pd.DataFrame) -> pd.DataFrame:
    """Applies the name rescue logic row-wise."""
    print("Correcting contact names and last names...")
    # Use .loc to ensure we are modifying the original df_clean
    df_clean.loc[:, ['first_name', 'last_name']] = df_clean.apply(
        cleaner.rescue_names, 
        axis=1
    )[['first_name', 'last_name']]
    return df_clean

def _create_jsonb_column(df_raw: pd.DataFrame, df_clean: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Consolidates unstructured columns into a single 'additional_data' JSON column."""
    jsonb_cols = [col for col in config['jsonb_columns'] if col in df_raw.columns]
    df_jsonb = df_raw[jsonb_cols].copy()
    
    df_clean['additional_data'] = df_jsonb.apply(
        lambda row: row.where(pd.notnull(row), None).to_dict(), axis=1
    )
    return df_clean

def _structure_data_into_tables(df_clean: pd.DataFrame, config: dict) -> dict:
    """Splits the master cleaned DataFrame into separate ones for each destination table."""
    
    # Companies table (unique by clean_rfc)
    company_cols = [p['target_db_col'] for p in config['cleaning_map'].values() if p['target_table'] == 'companies']
    df_companies = df_clean[['clean_rfc'] + [col for col in company_cols if col != 'clean_rfc']].drop_duplicates(subset=['clean_rfc'])
    
    # Contacts table (unique by clean_email)
    contact_cols = [p['target_db_col'] for p in config['cleaning_map'].values() if p['target_table'] == 'contacts']
    df_contacts = df_clean[['clean_email'] + [col for col in contact_cols if col != 'clean_email']].drop_duplicates(subset=['clean_email'])
    
    # Responses table (transactional data)
    response_cols = [p['target_db_col'] for p in config['cleaning_map'].values() if p['target_table'] == 'responses']
    df_responses = df_clean[response_cols + ['clean_rfc', 'clean_email', 'additional_data']].copy().reset_index(drop=True)
    df_responses['response_date'] = pd.to_datetime(df_responses['response_date'], errors='coerce')

    return {'companies': df_companies, 'contacts': df_contacts, 'responses': df_responses}
