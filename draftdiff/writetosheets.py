import os

import pandas as pd
from draftdiff import aggregation
from google.oauth2 import service_account
from googleapiclient.discovery import build


def sheet_exists(service, spreadsheet_id, sheet_name):
    """Check if a sheet with the given name exists in the spreadsheet."""
    spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = spreadsheet.get("sheets", [])
    for sheet in sheets:
        if sheet.get("properties", {}).get("title") == sheet_name:
            return True
    return False


def create_new_sheet(spreadsheet_id, sheet_name, key_file_path):
    # Path to your service account key file
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    # Authenticate and construct service
    credentials = service_account.Credentials.from_service_account_file(
        key_file_path, scopes=SCOPES
    )
    service = build("sheets", "v4", credentials=credentials)

    # The ID of the spreadsheet to update
    spreadsheet_id = spreadsheet_id

    try:
        if sheet_exists(service, spreadsheet_id, sheet_name):
            raise ValueError(f"A sheet with the name '{sheet_name}' already exists.")
        # Request to add a new sheet
        requests = [
            {
                "addSheet": {
                    "properties": {
                        "title": f"{sheet_name}",
                        "gridProperties": {"rowCount": 1000, "columnCount": 20},
                    }
                }
            }
        ]

        # Create the batch update request
        body = {"requests": requests}

        # Execute the request
        response = (
            service.spreadsheets()
            .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
            .execute()
        )
        print(f"New sheet added successfully: {response}")

    except ValueError as e:
        print(e)
    except Exception as e:
        print(f"An error occurred: {e}")


def read_from_google_sheets(spreadsheet_id, sheet_name, key_file_path) -> pd.DataFrame:
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    # Authenticate using the service account credentials
    credentials = service_account.Credentials.from_service_account_file(
        key_file_path, scopes=SCOPES
    )
    service = build("sheets", "v4", credentials=credentials)
    sheet = service.spreadsheets()
    result = (
        sheet.values().get(spreadsheetId=spreadsheet_id, range=sheet_name).execute()
    )
    values = result.get("values", [])
    if not values:
        print("No data found.")
        return pd.DataFrame()
    else:
        df = pd.DataFrame(values)
        return df


def write_to_google_sheets(df, spreadsheet_id, sheet_name, start_cell, key_file_path):
    """
    Write a Pandas DataFrame to a Google Sheet.

    Args:
        df (pandas.DataFrame): The DataFrame to be written to the Google Sheet.
        spreadsheet_id (str): The ID of the Google Sheet.
        sheet_name (str): The name of the sheet within the Google Sheet.
        start_cell (str): The starting cell for writing the DataFrame (e.g., "A1").
        key_file_path (str): The path to the service account key file.
    """
    # Define the scopes for the Google Sheets API
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    # Authenticate using the service account credentials
    credentials = service_account.Credentials.from_service_account_file(
        key_file_path, scopes=SCOPES
    )

    # Create a Google Sheets service object
    service = build("sheets", "v4", credentials=credentials)

    # Construct the range name
    range_name = f"{sheet_name}!{start_cell}"

    # Convert the DataFrame to a list of values
    values = [df.columns.tolist()] + df.values.tolist()

    # Create a value range object
    body = {"values": values}

    # Call the Sheets API to update the values
    result = (
        service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="USER_ENTERED",
            body=body,
        )
        .execute()
    )

    print(f"{result.get('updatedCells')} cells updated.")


def get_sheet_id(key_file_path, spreadsheet_id, sheet_name):
    # Define the scopes for the Google Sheets API
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    # Authenticate using the service account credentials
    credentials = service_account.Credentials.from_service_account_file(
        key_file_path, scopes=SCOPES
    )

    # Create a Google Sheets service object
    service = build("sheets", "v4", credentials=credentials)
    try:
        # Fetch spreadsheet metadata
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

        # Iterate through each sheet
        for sheet in spreadsheet.get("sheets", []):
            properties = sheet.get("properties", {})
            title = properties.get("title", "")
            sheet_id = properties.get("sheetId")

            # Check if the sheet name matches
            if title == sheet_name:
                return sheet_id

        # If no matching sheet is found
        raise ValueError(
            f"Sheet '{sheet_name}' not found in spreadsheet '{spreadsheet_id}'"
        )

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def create_pivot_table(
    key_file_path,
    spreadsheet_id,
    destinationSheetName,
    sourceSheetName,
    rowSourceColumn,
    columnSourceColumn,
    valueSourceColumn,
):
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    credentials = service_account.Credentials.from_service_account_file(
        key_file_path, scopes=SCOPES
    )

    # The ID of the spreadsheet
    SPREADSHEET_ID = spreadsheet_id
    sourceSheetId = get_sheet_id(
        key_file_path=key_file_path,
        spreadsheet_id=spreadsheet_id,
        sheet_name=sourceSheetName,
    )
    destinationSheetId = get_sheet_id(
        key_file_path=key_file_path,
        spreadsheet_id=spreadsheet_id,
        sheet_name=destinationSheetName,
    )

    # Initialize the Sheets API
    service = build("sheets", "v4", credentials=credentials)

    # Fetch the data from the sheet to determine the last row and column
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=SPREADSHEET_ID, range=sourceSheetName)
        .execute()
    )
    values = result.get("values", [])

    # Calculate the end row and column indices
    source_end_row_index = len(values)
    source_end_column_index = max(len(row) for row in values)

    # Define the pivot table request
    pivot_table_request = {
        "updateCells": {
            "range": {
                "sheetId": destinationSheetId,  # Change this to the ID of the sheet where you want the pivot table
                "startRowIndex": 0,
                "startColumnIndex": 0,
            },
            "rows": [
                {
                    "values": [
                        {
                            "pivotTable": {
                                "source": {
                                    "sheetId": sourceSheetId,
                                    "startRowIndex": 0,
                                    "startColumnIndex": 0,
                                    "endRowIndex": source_end_row_index,
                                    "endColumnIndex": source_end_column_index,
                                },
                                "rows": [
                                    {
                                        "sourceColumnOffset": rowSourceColumn,
                                        "showTotals": True,
                                        "sortOrder": "ASCENDING",
                                    }
                                ],
                                "columns": [
                                    {
                                        "sourceColumnOffset": columnSourceColumn,
                                        "sortOrder": "ASCENDING",
                                        "showTotals": True,
                                    }
                                ],
                                "values": [
                                    {
                                        "sourceColumnOffset": valueSourceColumn,
                                        "summarizeFunction": "SUM",
                                    }
                                ],
                            }
                        }
                    ]
                }
            ],
            "fields": "pivotTable",
        }
    }

    # Make the API request
    request = service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID, body={"requests": [pivot_table_request]}
    )
    response = request.execute()

    print(response)


def test():
    spreadsheet_id = "19OoA_AhjjOU1JrdTMYfRQ2oRv-i2_BnopJxbirKUthc"
    sheet_name = "Sheet2"
    start_cell = "A1"
    key_file_path = "credentials.json"
    create_new_sheet(
        spreadsheet_id=spreadsheet_id,
        sheet_name="new sheet test",
        key_file_path=key_file_path,
    )


def test2():
    spreadsheet_id = "19OoA_AhjjOU1JrdTMYfRQ2oRv-i2_BnopJxbirKUthc"
    key_file_path = "credentials.json"
    destinationSheetName = "2024-07-09-test"
    sourceSheetName = "2024-07-09-data"
    rowSourceColumn = 1
    columnSourceColumn = 0
    valueSourceColumn = 2

    create_new_sheet(
        spreadsheet_id=spreadsheet_id,
        sheet_name=destinationSheetName,
        key_file_path=key_file_path,
    )
    create_pivot_table(
        key_file_path=key_file_path,
        spreadsheet_id=spreadsheet_id,
        destinationSheetName=destinationSheetName,
        sourceSheetName=sourceSheetName,
        rowSourceColumn=rowSourceColumn,
        columnSourceColumn=columnSourceColumn,
        valueSourceColumn=valueSourceColumn,
    )


def main():
    token = os.environ["STRATZ_API_TOKEN"]
    df = aggregation.build_target_heroes_counter_heroes_df_for_dotabuff_id_in_last_n_days(
        token, "181567803", 30
    )
    spreadsheet_id = "19OoA_AhjjOU1JrdTMYfRQ2oRv-i2_BnopJxbirKUthc"
    sheet_name = "Sheet2"
    start_cell = "A1"
    key_file_path = "credentials.json"

    write_to_google_sheets(df, spreadsheet_id, sheet_name, start_cell, key_file_path)

    weighted_metrics_df = (
        aggregation.calculate_weighted_avg_metrics_in_counter_heroes_df(df)
    )

    write_to_google_sheets(
        weighted_metrics_df,
        spreadsheet_id,
        "Sheet3",
        start_cell,
        key_file_path,
    )

    print("finished writing to google sheets")


if __name__ == "__main__":
    test2()
