from app.services.google_sheets_service import read_worksheet_as_dataframe
from app.services.etl_script import clean_and_process_data, load_config
from config import CREDENTIALS_PATH, SHEET_ID
import pandas as pd
import re

print("Corriendo script")

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

if __name__ == '__main__':

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

    # ... (código de limpieza) ...

    # 4. Exportar DataFrames limpios para revisión
    # df_clean['empresas'].to_csv('output_empresas_limpio.csv', index=False, encoding='utf-8')
    # df_clean['contactos'].to_csv('output_contactos_limpio.csv', index=False, encoding='utf-8')
    # df_clean['respuestas'].to_csv('output_respuestas_limpio.csv', index=False, encoding='utf-8')
    
    print("\n✅ ¡Datos exportados a archivos CSV! Revísalos antes de subir a Supabase.")
# run.py
from app import create_app
from dotenv import load_dotenv

# Carga las variables del archivo .env
load_dotenv()

# Crea la aplicación usando la fábrica
app = create_app()

if __name__ == '__main__':
    # El puerto 8080 es común y es el que Cloud Run usa por defecto
    app.run(debug=True, port=8080)
