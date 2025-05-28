import os

import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from google.oauth2 import service_account

from .... import config
from ...utils.logger import app_logger


def query(query_str, service_account_file_path) -> pd.DataFrame:
    """
    Query the BigQuery service and return the results as a pandas DataFrame.

    :param query_str: SQL query string to be executed on BigQuery.
    :param service_account_file_path: Path to the service account file for authentication.
    :raises FileNotFoundError: If the service account file is not found.
    :raises ValueError: If the query string is empty.
    :return: DataFrame containing the results of the query.
    """
    # Set up BigQuery client using service account credentials
    if not os.path.exists(service_account_file_path):
        app_logger.error(f"Service account file not found: {service_account_file_path}")
        raise FileNotFoundError(
            f"Service account file not found: {service_account_file_path}"
        )

    if not query_str:
        app_logger.error("Query string is empty")
        raise ValueError("Query string cannot be empty")

    credentials = service_account.Credentials.from_service_account_file(
        service_account_file_path
    )
    client = bigquery.Client(credentials=credentials, project=config.BIGQUERY_PROJECT)
    app_logger.info("BigQuery client created successfully")

    # Execute the query
    app_logger.info("Executing query")
    job = client.query(query_str)
    job.result()  # Wait for the job to complete.
    data_df = job.to_dataframe()
    app_logger.info("Query executed successfully")

    return data_df
