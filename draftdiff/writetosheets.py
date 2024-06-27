import os

import pandas as pd
from draftdiff import aggregation
from google.oauth2 import service_account
from googleapiclient.discovery import build


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
    main()
