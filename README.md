# Asana Data Modelling ETL Pipeline

A Python-based ETL job that processes and models Asana data for reporting purposes. This project synchronizes data between Asana and BigQuery, making it available for visualization in Looker Studio.

## Table of Contents

- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Usage](#usage)
- [Dependencies](#dependencies)
- [Environment Variables](#environment-variables)
- [Security](#security)
- [Contact](#contact)
- [License](#license)

## Project Structure

```
.
├── job_daily/               # Main job directory
│   ├── data/               # SQL queries and data files
│   ├── services/           # Service integrations and utilities
│   ├── main.py            # Main job execution script
│   ├── config.py          # Configuration settings
│   └── __main__.py        # Entry point for running the job
├── requirements.txt        # Python dependencies
├── .env.example           # Example environment variables
└── .gitignore            # Git ignore rules
```

## Prerequisites

- Python 3.x
- Google Cloud Platform account with:
  - BigQuery access
  - Looker Studio access
- Service account credentials for:
  - BigQuery/Data Warehouse
  - Looker Studio

## Setup

1. Clone the repository
2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Copy `.env.example` to `.env` and configure the service account paths:
   ```bash
   cp .env.example .env
   ```
5. Ensure the service account JSON files are present in the project root:
   - `secret_mag_datawarehouse_sa.json`
   - `secret_mag_lookerstudio_sa.json`

## Usage

Run the job using Python:

```bash
python -m job_daily
```

The job will:

1. Execute the Asana data modelling SQL query
2. Process the data in BigQuery
3. Make the data available for Looker Studio dashboards

## Dependencies

Key dependencies include:

- google-cloud-bigquery
- pandas
- python-dotenv
- colorlog (for logging)

For a complete list of dependencies and their versions, see `requirements.txt`.

## Environment Variables

- `BIGQUERY_SERVICE_ACCOUNT_FILE_PATH`: Path to the BigQuery service account JSON file
- `LOOKER_STUDIO_SERVICE_ACCOUNT_FILE_PATH`: Path to the Looker Studio service account JSON file

## Security

- Service account credentials are stored in JSON files
- Environment variables are used for configuration
- Sensitive files are excluded from version control via .gitignore

## Contributing

1. Fork the repository
2. Create a new branch for your feature:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. Make your changes and commit them:
   ```bash
   git commit -m "Add your commit message"
   ```
4. Push to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```
5. Open a Pull Request

### Documentation

- Update README.md if you add new features or dependencies
- Document any new environment variables or configuration changes
- Keep code comments up to date
