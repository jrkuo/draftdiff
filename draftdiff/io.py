import datetime
import io
import json
import os
from enum import Enum
from typing import Dict, List

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from draftdiff import util, writetosheets
from google.oauth2 import service_account
from googleapiclient.discovery import build
from loguru import logger


class IOLocation(Enum):
    LOCAL = "LOCAL"
    SHEETS = "SHEETS"
    S3 = "S3"


def get_io_location() -> IOLocation:
    if os.environ["IO_LOCATION"] == "local":
        return IOLocation.LOCAL
    elif os.environ["IO_LOCATION"] == "sheets":
        return IOLocation.SHEETS
    elif os.environ["IO_LOCATION"] == "s3":
        return IOLocation.S3
    else:
        raise ValueError(f"unexpected env: {os.environ['IO_LOCATION']}")


def get_file_paths(data_path):
    io_location = get_io_location()
    file_paths = []
    match io_location:
        case IOLocation.LOCAL:
            folder_path = f"./data/{data_path}"
            for item in os.listdir(folder_path):
                item_path = os.path.join(folder_path, item)
                if os.path.isdir(item_path):
                    file_paths.append(item_path.replace("./data/", "", 1))
        case IOLocation.SHEETS:
            raise ValueError("cannot get file paths from sheets")
        case IOLocation.S3:
            folder_location = f"{data_path}"
            paginator = boto3.client("s3").get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket="draftdiff", Prefix=folder_location)
            for page in pages:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        file_paths.append(obj["Key"][:-10])
    return file_paths


def read_html(data_partition: str):
    io_location = get_io_location()
    match io_location:
        case IOLocation.LOCAL:
            path = f"./data/{data_partition}/data.html"
            with open(path, "rb") as rf:
                html_text = rf.read().decode("utf-8")
        case IOLocation.SHEETS:
            raise ValueError("cannot read html from sheets")
        case IOLocation.S3:
            object_location = f"{data_partition}/data.html"
            s3_response = boto3.client("s3").get_object(
                Bucket="draftdiff", Key=object_location
            )
            html_text = s3_response["Body"].read().decode("utf-8")
    return html_text


def write_html_to_html(data: str, data_partition: str):
    io_location = get_io_location()
    match io_location:
        case IOLocation.LOCAL:
            path = f"./data/{data_partition}/data.html"
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as wf:
                wf.write(data.encode())
        case IOLocation.SHEETS:
            raise ValueError("cannot write html to sheets")
        case IOLocation.S3:
            object_location = f"{data_partition}/data.html"
            boto3.client("s3").put_object(
                Bucket="draftdiff",
                Key=object_location,
                Body=data.encode("utf-8"),
                ContentType="text/html",
            )


def read_json(data_partition: str):
    io_location = get_io_location()
    match io_location:
        case IOLocation.LOCAL:
            path = f"./data/{data_partition}/data.json"
            with open(path, "r") as rf:
                json_data = json.load(rf)
        case IOLocation.SHEETS:
            raise ValueError("cannot read JSON from sheets")
        case IOLocation.S3:
            object_location = f"{data_partition}/data.json"
            s3_response = boto3.client("s3").get_object(
                Bucket="draftdiff", Key=object_location
            )
            json_data = json.loads(s3_response["Body"].read().decode("utf-8"))
    return json_data


def write_dictlist_to_json(data: List[Dict], data_partition: str):
    io_location = get_io_location()
    match io_location:
        case IOLocation.LOCAL:
            path = f"./data/{data_partition}/data.json"
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as wf:
                json.dump(data, wf, indent=4)
        case IOLocation.SHEETS:
            raise ValueError("cannot write JSON to sheets")
        case IOLocation.S3:
            object_location = f"{data_partition}/data.json"
            boto3.client("s3").put_object(
                Bucket="draftdiff",
                Key=object_location,
                Body=json.dumps(data, indent=4).encode("utf-8"),
            )


def read_df(df_partition: str):
    io_location = get_io_location()
    match io_location:
        case IOLocation.LOCAL:
            path = f"./data/{df_partition}/data.csv"
            df = pd.read_csv(path)
        case IOLocation.SHEETS:
            spreadsheet_id = "19OoA_AhjjOU1JrdTMYfRQ2oRv-i2_BnopJxbirKUthc"
            sheet_name = "Sheet1"
            key_file_path = "credentials.json"
            df = writetosheets.read_from_google_sheets(
                spreadsheet_id=spreadsheet_id,
                sheet_name=sheet_name,
                key_file_path=key_file_path,
            )
        case IOLocation.S3:
            object_location = f"{df_partition}/data.parquet"
            s3_response = boto3.client("s3").get_object(
                Bucket="draftdiff", Key=object_location
            )
            df = pd.read_parquet(io.BytesIO(s3_response["Body"].read()))
    return df


def write_df_to_df(df: pd.DataFrame, df_partition: str):
    io_location = get_io_location()
    match io_location:
        case IOLocation.LOCAL:
            path = f"./data/{df_partition}/data.csv"
            os.makedirs(os.path.dirname(path), exist_ok=True)
            df.to_csv(path, header=True, index=False)
        case IOLocation.SHEETS:
            spreadsheet_id = "19OoA_AhjjOU1JrdTMYfRQ2oRv-i2_BnopJxbirKUthc"
            sheet_name = "Sheet1"
            start_cell = "A1"
            key_file_path = "credentials.json"
            writetosheets.write_to_google_sheets(
                df=df,
                spreadsheet_id=spreadsheet_id,
                sheet_name=sheet_name,
                start_cell=start_cell,
                key_file_path=key_file_path,
            )
        case IOLocation.S3:
            object_location = f"{df_partition}/data.parquet"

            buffer = io.BytesIO()
            table = pa.Table.from_pandas(df)
            pq.write_table(table, buffer)

            buffer.seek(0)  # Rewind the buffer
            boto3.client("s3").upload_fileobj(buffer, "draftdiff", object_location)


def test():
    data = [{"X": 1, "Y": 1}, {"X": 2, "Y": 2}]
    df = pd.json_normalize(data)
    html = str(data)

    os.environ["IO_LOCATION"] = "local"
    write_html_to_html(html, "test/xy")
    html2 = read_html("test/xy")
    write_dictlist_to_json(data, "test/xy")
    data2 = read_json("test/xy")
    write_df_to_df(df=df, df_partition="test/xy")
    df2 = read_df(df_partition="test/xy")

    os.environ["IO_LOCATION"] = "s3"
    write_html_to_html(html, "test/xy")
    html2 = read_html("test/xy")
    write_dictlist_to_json(data, "json/test/xy")
    data2 = read_json("json/test/xy")
    write_df_to_df(df=df, df_partition="parquet/test/xy")
    df2 = read_df(df_partition="parquet/test/xy")

    os.environ["IO_LOCATION"] = "sheets"
    write_df_to_df(df=df, df_partition="test/xy")
    df2 = read_df(df_partition="test/xy")
    return


def test2():
    os.environ["IO_LOCATION"] = "local"
    ds = util.get_current_ds()
    player_id = "181567803"
    n = 30
    data_path = f"dotabuff/ds={ds}/player_id={player_id}/days={n}"
    file_paths = get_file_paths(data_path)

    os.environ["IO_LOCATION"] = "s3"
    file_paths2 = get_file_paths(data_path)
    return


if __name__ == "__main__":
    test2()
