import os

from . import config
from .services.integrations import bigquery
from .services.utils.logger import app_logger


def main():
    """
    Main function to run the job.
    """
    # Sync clients and contacts data
    app_logger.info("Getting SQL query for Asana data modelling")
    with open("job_daily/data/query_asana_data_modelling.sql", "r") as file:
        query_str = file.read()

    app_logger.info("Running SQL query for Asana data modelling")
    bigquery.query(query_str, config.LOOKER_STUDIO_SERVICE_ACCOUNT_FILE_PATH)


if __name__ == "__main__":
    main()
