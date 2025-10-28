from dotenv import load_dotenv
import json
import os

# Import the new modularized services
from app.services.google_sheets_service import read_worksheet_as_dataframe
from app.services.data_processing_service import clean_and_process_data
from app.services.certification_analysis_service import analyze_other_certifications

output_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data','outputs')
os.makedirs(output_dir, exist_ok=True)

def load_config(file_path='config/cleaning_map.json'):
    """Loads and returns the configuration dictionary from a JSON file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def run_etl_process():
    """
    Orchestrates the entire ETL process.
    1. Loads data from the source.
    2. Cleans and processes the data into structured tables.
    3. Runs specific sub-analyses (e.g., certifications).
    4. Exports the results to CSV files for review.
    """
    # --- 1. EXTRACTION ---
    print("Step 1: Extracting data from Google Sheets...")
    df_raw = read_worksheet_as_dataframe("Formulario Desarrollo Industria")
    print(f"Número total de filas obtenidas: {len(df_raw)}")

    # --- 2. TRANSFORMATION ---
    print("\nStep 2: Transforming data...")
    config = load_config()
    processed_data = clean_and_process_data(df_raw, config)
    
    print("Main data structured into 'companies', 'contacts', and 'responses'.")
    
    # --- 3. SUB-ANALYSIS (Certifications) ---
    print("\nStep 3: Running certification analysis...")
    df_cert_analysis = analyze_other_certifications(processed_data['responses'])
    print("Certification analysis complete.")

    # --- 4. LOAD (Export to CSV for now) ---
    print("\nStep 4: Exporting processed data to CSV files...")
    for name, df in processed_data.items():
        file_path = os.path.join(output_dir, f'output_{name}_clean.csv')
        df.to_csv(file_path, index=False, encoding='utf-8')
        print(f"  - Saved {name} data to {file_path}")
    
    # Export the analysis file
    analysis_file_path = os.path.join(output_dir, 'analysis_other_certifications.csv')
    df_cert_analysis.to_csv(analysis_file_path, index=False, encoding='utf-8')
    print(f"  - Saved certification analysis to {analysis_file_path}")

    print("\n✅ ETL process completed successfully!")

    # The function now returns the processed data dictionary for potential further use.
    return {"status": "ETL process finished successfully.", "output_directory": output_dir}

if __name__ == '__main__':
    load_dotenv()
    run_etl_process()