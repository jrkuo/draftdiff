import os

import requests
from loguru import logger


def create_s3_secret_scope():
    secret_request = {
        "scope": "draftdiff",
        "initial_manage_principal": "users",
        "scope_backend_type": "DATABRICKS",
    }
    logger.info("Sending POST request for secret scope to databricks")
    url = f"{os.environ['DATABRICKS_HOST']}/api/2.0/secrets/scopes/create"
    header = {"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}"}
    response = requests.post(url, json=secret_request, headers=header)
    # Check if the request was successful
    if response.status_code == 200:
        # Print the response JSON
        print(response.status_code)
    else:
        print("Error:", response.status_code)
    pass


def set_s3_secrets():
    secret_request_s3_access = {
        "scope": "draftdiff",
        "key": "s3-access-key",
        "string_value": f"{os.environ['S3_ACCESS_KEY']}",
    }
    url = f"{os.environ['DATABRICKS_HOST']}/api/2.0/secrets/put"
    header = {"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}"}
    response = requests.post(url, json=secret_request_s3_access, headers=header)
    # Check if the request was successful
    if response.status_code == 200:
        # Print the response JSON
        print(response.status_code)
    else:
        print("Error:", response.status_code)
    secret_request_s3_secret = {
        "scope": "draftdiff",
        "key": "s3-secret-key",
        "string_value": f"{os.environ['S3_SECRET_KEY']}",
    }
    response = requests.post(url, json=secret_request_s3_secret, headers=header)

    # Check if the request was successful
    if response.status_code == 200:
        # Print the response JSON
        print(response.status_code)
    else:
        print("Error:", response.status_code)
    pass


if __name__ == "__main__":
    create_s3_secret_scope()
    set_s3_secrets()
