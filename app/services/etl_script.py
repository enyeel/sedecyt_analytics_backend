import pandas as pd
import json
import numpy as np
# from db_connector import insert_or_update_data # Función para interactuar con Supabase
import app.services.data_cleaning_service as cleaner # Tu módulo con todas las funciones
from app.services.google_sheets_service import read_worksheet_as_dataframe
from config import CREDENTIALS_PATH, SHEET_ID
import json
import re
import ast
import os

output_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data','outputs')
os.makedirs(output_dir, exist_ok=True)

def load_config(file_path='config/cleaning_map.json'):
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
    
    # ... (Dentro de la función clean_and_process_data) ...

    # 4. Estructurar DataFrames para cada tabla (el paso más importante)
    
    # Columnas que van a RESPUESTAS (Transacciones)
    cols_respuestas = [p['target_db_col'] for p in config['cleaning_map'].values() if p['target_table'] == 'respuestas']
    # Debemos incluir 'fecha_respuesta' aquí para poder ordenar
    df_respuestas = df_clean[cols_respuestas + ['rfc_limpio', 'email_limpio', 'datos_adicionales']].copy()
    # *****************************************************************************************
    # 🚨 SOLUCIÓN AL ERROR: Asegurar que el DataFrame tenga un índice limpio (0, 1, 2, 3...)
    df_respuestas = df_respuestas.reset_index(drop=True)
    # *****************************************************************************************
    
    # ----------------------------------------------------------------------
    # 🔑 REDUCCIÓN PARA LA TABLA MAESTRA (EMPRESAS)
    # ----------------------------------------------------------------------

    # 1. Asegurar que las fechas sean datetime para ordenar
    # ASUMO: Que 'fecha_respuesta' es una de las columnas en cols_respuestas
    # Si no lo es, debes asegurarte de que la columna se llame correctamente (ej. 'Conversion Date' limpia)
    df_respuestas['fecha_respuesta'] = pd.to_datetime(df_respuestas['fecha_respuesta'], errors='coerce')

    # 2. Ordenar por fecha (más reciente al inicio)
    df_latest = df_respuestas.sort_values(by='fecha_respuesta', ascending=False)

    # 3. Eliminar duplicados, manteniendo SOLO la primera (la más reciente) por rfc_limpio
    df_latest = df_latest.drop_duplicates(subset=['rfc_limpio'], keep='first')

    # 4. Asignar el dato más reciente a df_empresas (el merge)
    # ... (El resto del código de merge con df_empresas continúa) ...

    # ... (El resto del return) ...
    
    return {
        'empresas': df_empresas,
        'contactos': df_contactos,
        'respuestas': df_respuestas
    }

# Función para rescatar RFCs faltantes o inválidos (Se queda temporalmente aquí)
def rescue_company_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Busca RFCs faltantes en otros campos (Nombre Comercial, Razón Social)
    y limpia entradas de basura (N/A, No).
    """
    
    # 1. Normalizar la columna RFC (reemplazar nulos, "NO", "N/A" por un string vacío)
    df['RFC_clean_temp'] = df['RFC'].fillna('')
    df['RFC_clean_temp'] = df['RFC_clean_temp'].replace(['NO', 'N/A', 'NO APLICA', 'NO TIENE'], '')
    df['RFC_clean_temp'] = df['RFC_clean_temp'].astype(str).str.strip() # <--- ¡Aseguramos que la columna sea string!

    # 2. Rescate 1: Buscar en Razón Social o Nombre Comercial
    for index, row in df.iterrows():
        
        # OBTENEMOS EL VALOR Y LO FORZAMOS A STRING antes de usar len()
        rfc_temp = str(row['RFC_clean_temp']) 
        
        # Si el RFC está vacío o es muy corto (probablemente basura)
        if len(rfc_temp) < 5: 
            
            # Buscar en 'Razón o denominación social de la empresa'
            social = str(row['Razón o denominación social de la empresa']).upper()
            
            # Intento de rescate: busca si hay algo que se parezca a un RFC
            # (Patrón: 3-4 letras, 6 números, 3-4 caracteres)
            rfc_pattern = re.search(r'[A-Z&Ñ]{3,4}\d{6}[A-Z0-9]{3,4}', social)
            
            if rfc_pattern:
                df.loc[index, 'RFC_clean_temp'] = rfc_pattern.group(0).strip()
    
    return df

def run_etl_process():
    print("Corriendo script")
    # Obtener, seleccionar y preparar datos a procesar
    print("Obteniendo datos...")
    df_raw = read_worksheet_as_dataframe(
        CREDENTIALS_PATH, SHEET_ID, "Formulario Desarrollo Industria"
    )
    # Imprimir las columnas para verificar
    print("Columnas disponibles en el DataFrame:")
    print(df_raw.columns.tolist())
    print(f"Número total de filas obtenidas: {len(df_raw)}")

    selected_rows = [0, 63, 100, 130, 140, 213, 243]  # Filas específicas para pruebas

    df_selected_rows = df_raw.copy()
    # df_selected_rows = df_selected_rows.iloc[selected_rows].copy()  # Filas específicas para pruebas
    
    # Cargar Configuración
    config = load_config()

    # Aplicar la limpieza y estructuración
    df_clean = clean_and_process_data(df_selected_rows, config)
    
    # Mostrar resultados (¡Listo para subir a Supabase!)
    print("\n--- DATA FRAME FINAL LISTO PARA SUBIR A EMPRESAS ---")
    print(df_clean['empresas'].head())
    print("\n--- DATA FRAME FINAL LISTO PARA SUBIR A CONTACTOS ---")
    print(df_clean['contactos'].head())
    print("\n--- DATA FRAME FINAL LISTO PARA SUBIR A RESPUESTAS (TRANSACCIONES) ---")
    print(df_clean['respuestas'].head())
    
    # 🌟 Siguiente Paso: Aquí se llamaría a la función de conexión a Supabase
    # insert_or_update_data(processed_data)

    # 4. Exportar DataFrames limpios para revisión
    df_clean['empresas'].to_csv(os.path.join(output_dir, 'output_empresas_limpio.csv'), index=False, encoding='utf-8')
    df_clean['contactos'].to_csv(os.path.join(output_dir, 'output_contactos_limpio.csv'), index=False, encoding='utf-8')
    df_clean['respuestas'].to_csv(os.path.join(output_dir, 'output_respuestas_limpio.csv'), index=False, encoding='utf-8')
    
    print("\n✅ ¡Datos exportados a archivos CSV! Revísalos antes de subir a Supabase.")

    # Asumimos que df_clean = clean_and_process_data(df_selected_rows, config) ya se ejecutó
    df_respuestas_clean = df_clean['respuestas'].copy()

    import ast # <-- ¡Asegúrate de que esto esté en tu script!

    def get_otra_certificacion(data_str):
        """
        Extrae el valor del campo de texto libre de las certificaciones,
        manejando el formato de string de Python envuelto en comillas de CSV.
        """
        if pd.isna(data_str) or not str(data_str).strip():
            return ''

        # 1. Preparación y Limpieza del String
        cleaned_str = str(data_str).strip()
        
        # 1.1 Quitar las comillas DOBLES exteriores (típicas del parseo de CSV)
        # Ej: " {'clave': 'valor'} " -> {'clave': 'valor'}
        if cleaned_str.startswith('"') and cleaned_str.endswith('"'):
            cleaned_str = cleaned_str[1:-1]
            
        # 2. Evaluación Segura con AST (Abstract Syntax Tree)
        # ast.literal_eval es la herramienta de Python para convertir un string
        # que parece un diccionario de Python (usa comillas simples) a un diccionario real.
        try:
            data = ast.literal_eval(cleaned_str)
            
            clave = 'En caso de contar con otra certificación, especificar.'
            
            # 3. Extraer el valor de la clave (si el resultado es un diccionario)
            if isinstance(data, dict):
                # Obtiene el valor, lo convierte a string (por si es numérico) y lo limpia
                return str(data.get(clave, '')).strip()
                
            return '' # (Fallo 3: Era una lista, tupla, etc.)
            
        except Exception:
            # Aquí cae si el string está malformado (Fallo 4: Error irrecuperable)
            return ''
        
    # Aplicar la función a la columna relevante
    df_respuestas_clean['otra_certificacion_txt_raw'] = df_respuestas_clean['datos_adicionales'].apply(get_otra_certificacion)

    # ***************************************************************
    # 🚨 PASO NUEVO: Aplicar la limpieza de texto final
    from app.services.data_cleaning_service import clean_text_for_analysis # Asumiendo que está ahí

    df_respuestas_clean['otra_certificacion_txt_limpio'] = df_respuestas_clean['otra_certificacion_txt_raw'].apply(clean_text_for_analysis)
    # ***************************************************************

    # 3. Seleccionar las columnas para tu análisis
    # Usa la columna LIMPIA ahora
    df_analisis = df_respuestas_clean[['rfc_limpio', 'fecha_respuesta', 'otra_certificacion_txt_limpio']].copy() 

    # 4. Exportar el CSV (y ahora será un CSV mucho más limpio y fácil de leer)
    nombre_archivo = 'analisis_otras_certificaciones_limpio.csv'
    df_analisis.to_csv(os.path.join(output_dir, nombre_archivo), index=False, encoding='utf-8')
    print(f"CSV de análisis generado para revisión manual: {nombre_archivo}")
    
    df_clean['respuestas'].to_csv(os.path.join(output_dir, 'output_respuestas_limpio.csv'), index=False, encoding='utf-8')

    # ***************************************************************
    # 🚨 PRUEBA RÁPIDA DE EXTRACCIÓN DE ACRÓNIMOS
    # ***************************************************************

    # Aplicar la función de extracción (asumiendo que la función extract_certifications_acronyms 
    # está definida o importada en este script para la prueba)
    df_analisis['other_certifications_acronyms'] = df_analisis['otra_certificacion_txt_limpio'].apply(
        cleaner.extract_certifications_acronyms
    )

    # Convertir la lista de acrónimos a una cadena separada por comas para el CSV
    df_analisis['other_certifications'] = df_analisis['other_certifications_acronyms'].apply(
        lambda x: ', '.join(x) if x else ''
    )


    # Seleccionar las columnas para la prueba final
    df_test_output = df_analisis[['rfc_limpio', 'otra_certificacion_txt_limpio', 'other_certifications']].copy()

    # Exportar el CSV de prueba
    nombre_archivo_test = 'analisis_certificaciones_TEST_ACRONIMOS.csv'
    df_test_output.to_csv(os.path.join(output_dir, nombre_archivo_test), index=False, encoding='utf-8')

    print(f"\n✅ ¡CSV de PRUEBA de acrónimos generado para revisión: {nombre_archivo_test}!")
    print("Revisa este archivo para validar si las 'search_keywords' están funcionando.")

    return df_analisis

if __name__ == '__main__':
    run_etl_process()