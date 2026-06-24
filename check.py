import traceback
import gspread
from google.oauth2.service_account import Credentials

SPREADSHEET_ID = "1Z39dIQrgdhSoWdD5AE9jIMtfn1ahTxl-femjqxyER0Q"

scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

try:
    creds = Credentials.from_service_account_file("service_account.json", scopes=scopes)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)

    print("OK")
    print("Название таблицы:", spreadsheet.title)

except Exception:
    print("GOOGLE ERROR:")
    traceback.print_exc()