from google.oauth2 import service_account
from googleapiclient.discovery import build

# Path to the service account key file downloaded earlier
key_file_path = "credentials.json"

# Define the scopes for the Google Sheets API
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# Authenticate using the service account credentials
credentials = service_account.Credentials.from_service_account_file(
    key_file_path, scopes=SCOPES
)

# Create a Google Sheets service object
service = build("sheets", "v4", credentials=credentials)

# ID of the Google Sheet you want to read from
spreadsheet_id = "19OoA_AhjjOU1JrdTMYfRQ2oRv-i2_BnopJxbirKUthc"

# Range of cells you want to read, e.g., 'Sheet1!A1:B2'
range_name = "Sheet1!A1:B3"

# Call the Sheets API to retrieve values
sheet = service.spreadsheets()
result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
values = result.get("values", [])

if not values:
    print("No data found.")
else:
    print("Name, Value:")
    for row in values:
        # Print columns A and B, which correspond to indices 0 and 1
        print(f"{row[0]}, {row[1]}")
