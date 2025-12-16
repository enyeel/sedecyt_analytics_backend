import pandas as pd
import os
from app.pipelines.etl import cleaning as cleaner
from app.pipelines.etl.cleaning import rescue_names, normalize_text
from app.core.connections.supabase_service import get_all_from

def _prepare_catalog_maps(catalog_data: list, name_col: str, id_col: str = 'id') -> tuple:
    value_to_id_map = {}
    fuzzy_candidates = []

    for item in catalog_data:
        record_id = item[id_col]
        
        # AQUI EL TRUCO: Normalizamos también la BD para que "Rincón" sea "RINCON"
        official_name = normalize_text(str(item[name_col]).upper().strip()) 
        
        value_to_id_map[official_name] = record_id
        fuzzy_candidates.append(official_name)
        
        keywords = item.get('keywords')
        if keywords:
            for kw in keywords:
                # Normalizamos las keywords de la BD también
                kw_clean = normalize_text(str(kw).upper().strip())
                value_to_id_map[kw_clean] = record_id
                fuzzy_candidates.append(kw_clean)
                
    return value_to_id_map, fuzzy_candidates

def _standardize_catalogs(df_clean: pd.DataFrame, debug_dir: str = None) -> pd.DataFrame:
    print("Standardizing catalogs (Municipios & Parques)...")

    # --- A. CARGA DE DATOS (Solo una vez) ---
    # Idealmente esto se cargaría fuera y se pasaría como argumento, pero aquí está bien por ahora.
    raw_municipios = get_all_from('municipality_catalog') # Tu tabla de municipios
    raw_parques = get_all_from('industrial_parks_catalog') # Tu tabla de parques
    
    # --- B. PREPARACIÓN DE MAPAS ---
    muni_map, muni_candidates = _prepare_catalog_maps(raw_municipios, 'municipality_name')
    park_map, park_candidates = _prepare_catalog_maps(raw_parques, 'park_name')
    
    print(f"🔎 DEBUG MAPAS:")
    print(f"   - Municipios cargados: {len(muni_map)}")
    print(f"   - Parques cargados: {len(park_map)}")
    print(f"   - Ejemplo Keys Parques (normalizadas): {list(park_map.keys())[:5]}")

    # --- C. APLICACIÓN DE LÓGICA (Vectorizada o Apply) ---
    # --- 1. MUNICIPIOS (Con el SUPER PODER de limpieza extra) ---
    if 'other_municipality' in df_clean.columns:
        
        # Definimos el ruido específico de esta región
        RUIDO_MUNICIPIOS = ['AGS', 'AGUASCALIENTES', 'EDO', 'MEX', 'ZONA CENTRO']
        
        muni_results = df_clean['other_municipality'].apply(
            lambda x: cleaner.smart_catalog_match(
                x, 
                muni_map, 
                muni_candidates, 
                threshold=90, 
                extra_removals=RUIDO_MUNICIPIOS # <--- ¡AQUÍ ESTÁ LA CLAVE!
            )
        )
        df_clean['municipality_id'] = muni_results.apply(lambda x: x[0])
        df_clean['other_municipality'] = muni_results.apply(lambda x: x[1])

    # --- 2. PARQUES INDUSTRIALES (Sin ruido específico por ahora) ---
    if 'industrial_park' in df_clean.columns:
        
        # Para parques, a lo mejor "PARQUE" o "INDUSTRIAL" es ruido, 
        # pero a veces ayuda al fuzzy, así que lo dejamos vacío o probamos.
        RUIDO_PARQUES = [] 
        
        park_results = df_clean['industrial_park'].apply(
            lambda x: cleaner.smart_catalog_match(
                x, 
                park_map, 
                park_candidates, 
                threshold=87,
                extra_removals=RUIDO_PARQUES
            )
        )
        df_clean['industrial_park_id'] = park_results.apply(lambda x: x[0])
        df_clean['other_industrial_park'] = park_results.apply(lambda x: x[1])

    return df_clean

def clean_and_process_data(df: pd.DataFrame, config: dict, debug_output_dir: str = None) -> dict:
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
    
    # [DEBUG] Exportar tras limpieza inicial
    if debug_output_dir:
        df_clean.to_csv(os.path.join(debug_output_dir, 'debug_02_initial_cleaning.csv'), index=False, encoding='utf-8-sig')
    
    # 2. Post-Processing: Apply complex logic that depends on multiple columns.
    df_clean = _finalize_company_ids(df_clean)
    df_clean = _rescue_contact_names(df_clean)
    
    # --- NUEVA LÍNEA ---
    df_clean = _standardize_catalogs(df_clean, debug_output_dir)
    
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
    
    # --- 1. TABLA COMPANIES ---
    # Obtener columnas definidas en el JSON
    company_cols_json = [p['target_db_col'] for p in config['cleaning_map'].values() if p['target_table'] == 'companies']
    
    # Definir las columnas calculadas (nuestra magia nueva)
    generated_cols = [
        'municipality_id', 'other_municipality', 
        'industrial_park_id', 'other_industrial_park'
    ]
    
    # Juntar todo: clean_rfc + JSON + Generadas
    all_desired_cols = ['clean_rfc'] + company_cols_json + generated_cols
    
    # --- EL FIX: DEDUPLICACIÓN ---
    # Usamos dict.fromkeys para borrar duplicados MANTENIENDO el orden (set() desordena)
    unique_cols = list(dict.fromkeys(all_desired_cols))
    
    # Verificar que existen en el DataFrame para que no truene si falta alguna
    final_cols = [c for c in unique_cols if c in df_clean.columns]
    
    # Crear el DF final
    df_companies = df_clean[final_cols].copy().drop_duplicates(subset=['clean_rfc'])
    
    
    # --- 2. TABLA CONTACTS ---
    contact_cols_json = [p['target_db_col'] for p in config['cleaning_map'].values() if p['target_table'] == 'contacts']
    
    # Mismo proceso de deduplicación para contactos (por seguridad)
    all_contact_cols = ['clean_email'] + contact_cols_json
    unique_contact_cols = list(dict.fromkeys(all_contact_cols))
    final_contact_cols = [c for c in unique_contact_cols if c in df_clean.columns]
    
    df_contacts = df_clean[final_contact_cols].drop_duplicates(subset=['clean_email'])
    
    
    # --- 3. TABLA RESPONSES ---
    response_cols_json = [p['target_db_col'] for p in config['cleaning_map'].values() if p['target_table'] == 'responses']
    
    # Aquí agregamos las llaves foráneas para que la respuesta sepa de quién es
    all_response_cols = response_cols_json + ['clean_rfc', 'clean_email', 'additional_data']
    unique_response_cols = list(dict.fromkeys(all_response_cols))
    final_response_cols = [c for c in unique_response_cols if c in df_clean.columns]

    df_responses = df_clean[final_response_cols].copy().reset_index(drop=True)
    
    # Asegurar formato de fecha si existe la columna
    if 'response_date' in df_responses.columns:
        df_responses['response_date'] = pd.to_datetime(df_responses['response_date'], errors='coerce')

    return {'companies': df_companies, 'contacts': df_contacts, 'responses': df_responses}