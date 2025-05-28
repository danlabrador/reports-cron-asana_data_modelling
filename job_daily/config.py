# Configuration settings for the cron job project
import os

from dotenv import load_dotenv

load_dotenv()

# Environment settings
ENV = os.getenv("ENV", "prod")
PROJECT = "finance-cron-churn_rate_data_modelling"

# Set PROJECT_ROOT to the parent directory of the current file's directory
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# BigQuery settings
BIGQUERY_SERVICE_ACCOUNT_FILE_PATH = os.path.join(
    PROJECT_ROOT, os.getenv("BIGQUERY_SERVICE_ACCOUNT_FILE_PATH")
)
LOOKER_STUDIO_SERVICE_ACCOUNT_FILE_PATH = os.path.join(
    PROJECT_ROOT, os.getenv("LOOKER_STUDIO_SERVICE_ACCOUNT_FILE_PATH")
)
PROD_BIGQUERY_PROJECT = "mag-datawarehouse"
DEV_BIGQUERY_PROJECT = "dev-mag-datawarehouse"
BIGQUERY_PROJECT = PROD_BIGQUERY_PROJECT if ENV == "prod" else DEV_BIGQUERY_PROJECT

# cspell: ignore dotenv
