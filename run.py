from app.services.google_sheets_service import read_worksheet_as_dataframe
from config import GOOGLE_CREDENTIALS

SHEET_ID = "1Pkwk6REsRnZa1NNvQLtcdBVKIYuvIDY29XvjpRd8UKQ"
df = read_worksheet_as_dataframe(GOOGLE_CREDENTIALS, SHEET_ID, "Respuestas")
print(df.head())
