import pandas as pd
import json
import app.services.data_cleaning_service as cleaner # Tu módulo con todas las funciones
import numpy as np
# from db_connector import insert_or_update_data # Función para interactuar con Supabase

def load_config(file_path='config.json'):
    """Carga y devuelve el diccionario de configuración."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

# Asumimos que la variable 'cleaner' es tu módulo cleaning_service

def clean_and_process_data(df: pd.DataFrame, config: dict) -> dict:
    """
    Aplica la limpieza definida en el config.json, *finaliza el RFC* y estructura los datos.
    Devuelve un diccionario con DataFrames listos para cada tabla.
    """
    # 1. Crear un DataFrame para las columnas limpias
    df_clean = pd.DataFrame(index=df.index)
    
    # 2. Bucle Inteligente: Iterar sobre el cleaning_map (limpieza columna por columna)
    for original_col, params in config['cleaning_map'].items():
        
        target_col = params['target_db_col']
        clean_func_name = params['clean_func']
        
        # 🌟 La Magia: Usar getattr() para llamar dinámicamente a la función
        try:
            clean_function = getattr(cleaner, clean_func_name)
            
            # Aplicar la función a la columna del DataFrame
            if pd.api.types.is_string_dtype(df[original_col].dtype):
                # Se usa 'astype(str)' para manejar floats/nans y luego aplica la limpieza
                df_clean[target_col] = df[original_col].astype(str).str.strip().apply(clean_function)
            else:
                df_clean[target_col] = df[original_col].apply(clean_function)
                
        except AttributeError:
            print(f"ERROR: Función de limpieza '{clean_func_name}' no encontrada en cleaning_service.py.")
            df_clean[target_col] = df[original_col]
        except KeyError:
            print(f"ERROR: Columna original '{original_col}' no encontrada en el DataFrame.")
            
    # ----------------------------------------------------------------------
    # 🌟 PASO 2.5: Lógica de Finalización de RFC y NOMBRES/APELLIDOS
    # ----------------------------------------------------------------------
    
    # Ambos campos ('rfc_limpio' y 'nombre_comercial') están listos en df_clean
    mask_fallo = df_clean['rfc_limpio'].str.startswith('ID_FALLO', na=False)
    
    if mask_fallo.any():
        print("Finalizando RFC: Concatenando nombre comercial a IDs fallidos...")
        
        # 1. Formatear el nombre comercial de las filas que fallaron
        nombre_comercial_format = (
            df_clean.loc[mask_fallo, 'nombre_comercial'].astype(str)
                .str.replace(r'[^\w\s]', '', regex=True) 
                .str.strip()
                .str.replace(r'\s+', '-', regex=True) # Espacios por guiones
        )
        
        # 2. Aplicar la concatenación directamente en la columna final
        df_clean.loc[mask_fallo, 'rfc_limpio'] = (
            df_clean.loc[mask_fallo, 'rfc_limpio'].astype(str) + 
            '_' + 
            nombre_comercial_format
        )
    # El campo 'rfc_limpio' ahora contiene los IDs finales y correctos.

    # B. RESCATE DE NOMBRES Y APELLIDOS (Tu nueva lógica)
    print("Corrigiendo nombres y apellidos...")
    
    # Aplicamos la función fila por fila SÓLO a las columnas afectadas
    df_clean[['nombre_limpio', 'apellido_limpio']] = df_clean.apply(
        cleaner.rescue_names, 
        axis=1
    )[['nombre_limpio', 'apellido_limpio']]
    
    # ----------------------------------------------------------------------
    
    # 3. Crear el Objeto JSONB (continúa como estaba)
    # Seleccionar solo las columnas flexibles definidas en el JSONB
    df_jsonb = df[config['jsonb_columns']].copy()
    
    # Convertir cada fila de esas columnas en un objeto JSON (diccionario)
    df_clean['datos_adicionales'] = df_jsonb.apply(
        lambda row: row.where(pd.notnull(row), None).to_dict(), axis=1
    )
    
    # 4. Estructurar DataFrames para cada tabla (el paso más importante)
    # Estos DataFrames heredan el 'rfc_limpio' ya finalizado del paso 2.5
    
    # Columnas que van a EMPRESAS (usando rfc_limpio como clave)
    cols_empresas = [p['target_db_col'] for p in config['cleaning_map'].values() if p['target_table'] == 'empresas']
    # Aquí es clave que el 'rfc_limpio' ya esté completo
    df_empresas = df_clean[['rfc_limpio'] + [col for col in cols_empresas if col != 'rfc_limpio']].drop_duplicates(subset=['rfc_limpio'])
    
    # Columnas que van a CONTACTOS (usando email_limpio como clave)
    cols_contactos = [p['target_db_col'] for p in config['cleaning_map'].values() if p['target_table'] == 'contactos']
    df_contactos = df_clean[['email_limpio'] + [col for col in cols_contactos if col != 'email_limpio']].drop_duplicates(subset=['email_limpio'])
    
    # Columnas que van a RESPUESTAS (Transacciones)
    cols_respuestas = [p['target_db_col'] for p in config['cleaning_map'].values() if p['target_table'] == 'respuestas']
    # El 'rfc_limpio' que se copia aquí ya es el ID final y correcto
    df_respuestas = df_clean[cols_respuestas + ['rfc_limpio', 'email_limpio', 'datos_adicionales', 'fecha_respuesta', 'certificaciones_array']].copy()

    # ... (Paso 4: Estructurar DataFrames) ...

    # ----------------------------------------------------------------------
    # 🔑 REDUCCIÓN PARA LA TABLA MAESTRA (EMPRESAS)
    # ----------------------------------------------------------------------

    # 1. Asegurar índice único antes de operaciones complejas
    df_respuestas = df_respuestas.reset_index(drop=True)

    # 2. Asegurar que las fechas sean datetime para ordenar
    # NOTA: Asumo que 'fecha_respuesta' ya existe en df_respuestas (o se debe crear desde 'Conversion Date')
    df_respuestas['fecha_respuesta'] = pd.to_datetime(df_respuestas['fecha_respuesta'], errors='coerce')

    # 3. Ordenar por fecha (más reciente al inicio)
    df_latest = df_respuestas.sort_values(by='fecha_respuesta', ascending=False)

    # 4. Eliminar duplicados, manteniendo SOLO la primera (la más reciente) por rfc_limpio
    df_latest = df_latest.drop_duplicates(subset=['rfc_limpio'], keep='first')
    df_empresas = df_empresas.merge(
        df_latest,
        on='rfc_limpio',
        how='left'
    )
    # Ahora df_empresas tiene la columna certificaciones_array más reciente.

    # ... (Continúa el return) ...
    
    return {
        'empresas': df_empresas,
        'contactos': df_contactos,
        'respuestas': df_respuestas
    }