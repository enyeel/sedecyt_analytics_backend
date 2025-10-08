from app.services.google_sheets_service import read_worksheet_as_dataframe
from app.services.data_processing_service import intelligent_cleaner
from app.services.data_cleaning_service import limpiar_telefonos
from config import CREDENTIALS_PATH
import pandas as pd

SHEET_ID = "1Pkwk6REsRnZa1NNvQLtcdBVKIYuvIDY29XvjpRd8UKQ"
df = read_worksheet_as_dataframe(CREDENTIALS_PATH, SHEET_ID, "Formulario Desarrollo Industria")
print(df.head())

dataframe = pd.DataFrame(df)

# Assuming 'raw_df' is the full DataFrame loaded from a sheet or API

# 1. Define the columns you need for a specific task
columns_for_graph = ['Cargo', 'Apellidos', 'Planes de expansión ', 'Código postal']
print(dataframe[columns_for_graph].head())

# 2. Create a small, temporary DataFrame with only those columns
subset_df = dataframe[columns_for_graph].copy()

# 3. Pass this subset to your new intelligent cleaner
cleaned_subset_df = intelligent_cleaner(subset_df)

# 4. Now, 'cleaned_subset_df' is ready for graphing or further calculations!
print(cleaned_subset_df.info())
print(cleaned_subset_df.head())
print(cleaned_subset_df)


# checar nombres de columnas

limpiar_telefonos(dataframe["Teléfono de contacto"].astype(str).tolist())