from app.services.google_sheets_service import read_worksheet_as_dataframe
from config import CREDENTIALS_PATH

SHEET_ID = "1Pkwk6REsRnZa1NNvQLtcdBVKIYuvIDY29XvjpRd8UKQ"
df = read_worksheet_as_dataframe(CREDENTIALS_PATH, SHEET_ID, "Formulario Desarrollo Industria")
print(df.head())
