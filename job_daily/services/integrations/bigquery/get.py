import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from tenacity import retry, stop_after_attempt, wait_exponential

from .... import config
from ...utils.logger import app_logger


@retry(stop=stop_after_attempt(7), wait=wait_exponential(multiplier=1, min=2, max=60))
def get_df(file_path="", query_string="") -> pd.DataFrame:
    """
    Query the active companies data from BigQuery and return it as a DataFrame.
    """

    if query_string:
        query = query_string
    elif file_path:
        with open(file_path, "r") as file:
            query = file.read()
    else:
        app_logger.error("Either query_string or file_path must be provided")
        raise ValueError("Either query_string or file_path must be provided")

    # Set up BigQuery client using service account credentials
    credentials = service_account.Credentials.from_service_account_file(
        config.SERVICE_ACCOUNT_FILE_PATH
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    return client.query(query).to_dataframe()
