from app.services.google_sheets_service import read_worksheet_as_dataframe
from app.services.data_cleaning_service import limpiar_telefonos
from config import CREDENTIALS_PATH
import pandas as pd

SHEET_ID = "1Pkwk6REsRnZa1NNvQLtcdBVKIYuvIDY29XvjpRd8UKQ"
df = read_worksheet_as_dataframe(CREDENTIALS_PATH, SHEET_ID, "Formulario Desarrollo Industria")
print(df.head())

dataframe = pd.DataFrame(df)

limpiar_telefonos(dataframe["Teléfono de contacto"].astype(str).tolist())
from app import create_app

app = create_app()

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)

