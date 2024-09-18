import io
import os

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from draftdiff import aggregation


def write_df_to_parquet_to_s3(df, bucket_name, s3_key):
    buffer = io.BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buffer)

    # Create a boto3 client
    s3_client = boto3.client("s3")

    buffer.seek(0)  # Rewind the buffer
    s3_client.upload_fileobj(buffer, bucket_name, s3_key)

    print(f"File uploaded to s3://{bucket_name}/{s3_key}")


def main():
    token = os.environ["STRATZ_API_TOKEN"]
    df = aggregation.build_target_heroes_counter_heroes_df_for_dotabuff_id_in_last_n_days(
        token, "181567803", 30
    )

    write_df_to_parquet_to_s3(
        df,
        bucket_name="draftdiff",
        s3_key="181567803/target_heroes_counter_heroes_df_for_dotabuff_id_181567803_in_last_30_days.parquet",
    )

    weighted_metrics_df = (
        aggregation.calculate_weighted_avg_metrics_in_counter_heroes_df(df)
    )

    write_df_to_parquet_to_s3(
        weighted_metrics_df,
        bucket_name="draftdiff",
        s3_key="181567803/weighted_metrics_df_for_dotabuff_id_181567803_in_last_30_days.parquet",
    )


if __name__ == "__main__":
    main()
