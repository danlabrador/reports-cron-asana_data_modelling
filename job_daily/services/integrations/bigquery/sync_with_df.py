"""
This module provides functionality to synchronize a pandas DataFrame with a BigQuery table.

It performs the following actions:
 - Checks for the existence of the target table in BigQuery.
 - Updates the table schema if new columns are detected.
 - Inserts new rows and updates existing rows based on a reference ID.

Internal helper functions are prefixed with an underscore.
"""

import ast
import json
import os
from typing import Tuple

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.oauth2 import service_account
from tenacity import retry, stop_after_attempt, wait_exponential

from .... import config
from ...utils.logger import app_logger


def _get_bq_field_type(column_name: str, series: pd.Series) -> str:
    if column_name == "_synced_at":
        return "TIMESTAMP"
    dtype = series.dtype
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOL"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "STRING"


def _normalize_value(v: any) -> any:
    """
    Normalize a value by converting string representations of booleans, JSON objects/lists, or numbers
    into their appropriate Python types.

    :param v: The value to normalize.
    :return: The normalized value.
    """
    if isinstance(v, str):
        lower_v = v.lower()
        if lower_v == "true":
            return True
        elif lower_v == "false":
            return False
        if v.startswith("{") or v.startswith("["):
            try:
                return json.loads(v)
            except Exception:
                try:
                    return ast.literal_eval(v)
                except Exception:
                    pass
    return v


def _prepare_param_value(value: any, bq_type: str) -> any:
    """
    Prepare the parameter value for BigQuery.
    If the column type is STRING and the value is a dict or list, convert it to a JSON string.
    Otherwise, return the value unchanged.
    """
    if value is None:
        return value

    if bq_type == "STRING":
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return value

    # Convert boolean-like values to actual booleans
    if bq_type == "BOOL":
        if isinstance(value, bool):
            return value
        try:
            import numpy as np

            if isinstance(value, np.bool_):
                converted = bool(value)
                return converted
        except ImportError:
            pass
        if isinstance(value, str):
            lower = value.lower()
            if lower == "true":
                return True
            elif lower == "false":
                return False
        if isinstance(value, (int, float)):
            converted = bool(value)
            return converted
        return value

    return value


def _log_retry(retry_state: any) -> None:
    """
    Log retry attempts with details from the retry state.

    :param retry_state: The state object provided by tenacity on retry.
    """
    waiting = retry_state.next_action.sleep
    attempt = retry_state.attempt_number
    exception = None
    if retry_state.outcome.failed:
        exception = retry_state.outcome.exception()
    if exception:
        app_logger.info(
            f"Retry attempt {attempt} due to error: {exception}. Retrying in {waiting:.2f} seconds."
        )
    else:
        app_logger.info(f"Retry attempt {attempt}: Retrying in {waiting:.2f} seconds.")


def _equals(a: any, b: any) -> bool:
    """
    Compare two values for equality after normalizing them.

    :param a: First value.
    :param b: Second value.
    :return: True if values are considered equal, False otherwise.
    """
    a = _normalize_value(a)
    b = _normalize_value(b)

    try:
        if hasattr(a, "item"):
            a = a.item()
        if hasattr(b, "item"):
            b = b.item()
    except Exception:
        pass

    if isinstance(a, (dict, list)) and isinstance(b, (dict, list)):
        try:
            return json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)
        except Exception:
            pass

    if isinstance(a, bool) and isinstance(b, bool):
        return a == b

    try:
        return float(a) == float(b)
    except Exception:
        pass

    return a == b


def _ensure_table_exists(
    client: bigquery.Client, table_id: str, table_ref: str, data: pd.DataFrame
) -> bigquery.Table:
    """
    Ensure that the BigQuery table exists with a schema based on the DataFrame columns.

    If the table exists, update its schema with any new columns. If not, create it.

    :param client: BigQuery client instance.
    :param table_id: Fully-qualified table ID (project.dataset.table).
    :param table_ref: Table reference formatted string.
    :param data: DataFrame containing the data to sync.
    :return: The BigQuery table object.
    """
    try:
        table_obj = client.get_table(table_id)
        existing_fields = {field.name for field in table_obj.schema}
        new_fields = []
        for col in data.columns:
            if col not in existing_fields:
                field_type = _get_bq_field_type(col, data[col])
                new_fields.append(bigquery.SchemaField(col, field_type))
        if "_synced_at" not in existing_fields:
            new_fields.append(bigquery.SchemaField("_synced_at", "TIMESTAMP"))
        if new_fields:
            table_obj.schema = table_obj.schema + new_fields
            client.update_table(table_obj, ["schema"])
            app_logger.info(
                f"Updated table schema {table_ref} with new columns: {[f.name for f in new_fields]}"
            )
        return table_obj
    except NotFound:
        schema = []
        for col in data.columns:
            field_type = _get_bq_field_type(col, data[col])
            schema.append(bigquery.SchemaField(col, field_type))
        if "_synced_at" not in {f.name for f in schema}:
            schema.append(bigquery.SchemaField("_synced_at", "TIMESTAMP"))
        table_obj = bigquery.Table(table_id, schema=schema)
        client.create_table(table_obj)
        app_logger.info(f"Created table {table_ref}")
        return table_obj


def _get_existing_dataframe(
    client: bigquery.Client, table_ref: str, reference_id: str, ref_ids: list
) -> pd.DataFrame:
    """
    Query BigQuery for rows matching the provided reference IDs and return the results as a DataFrame.

    :param client: BigQuery client instance.
    :param table_ref: Table reference (formatted string).
    :param reference_id: The column used as the unique identifier.
    :param ref_ids: List of reference IDs to query.
    :return: DataFrame with existing rows.
    """
    query = f"SELECT * FROM {table_ref} WHERE {reference_id} IN UNNEST(@ref_ids)"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("ref_ids", "STRING", ref_ids)]
    )
    app_logger.info(
        f"Querying BigQuery for {len(ref_ids)} reference IDs from {table_ref}"
    )
    result = client.query(
        query, job_config=job_config
    ).result()  # Wait for the query to complete
    return result.to_dataframe()


def _prepare_rows(
    data: pd.DataFrame, reference_id: str, existing_df: pd.DataFrame
) -> Tuple[list, list]:
    """
    Compare each row in the DataFrame with the existing data and prepare lists for insertions and updates.

    :param data: Input DataFrame to sync.
    :param reference_id: Unique identifier column name.
    :param existing_df: DataFrame with existing rows from BigQuery.
    :return: Tuple of (rows_to_insert, rows_to_update).
    """
    rows_to_insert = []
    rows_to_update = []

    for _, new_row in data.iterrows():
        ref_value = new_row[reference_id]
        existing_row = existing_df[existing_df[reference_id] == ref_value]

        if existing_row.empty:
            # Prepare row for insert: convert None to empty string
            row = {
                col: new_row[col] if col != "_synced_at" else None
                for col in new_row.index
            }
            row["_synced_at"] = None  # Placeholder for CURRENT_TIMESTAMP()
            rows_to_insert.append(row)
        else:
            row_changed = False
            updated_row = {}
            for col in new_row.index:
                if col in [reference_id, "_synced_at"]:
                    continue
                new_val = new_row[col]
                if col not in existing_row.columns or not _equals(
                    existing_row.iloc[0][col], new_val
                ):
                    row_changed = True
                    updated_row[col] = new_row[col]
            if row_changed:
                updated_row["_synced_at"] = None  # Placeholder for CURRENT_TIMESTAMP()
                updated_row[reference_id] = new_row[reference_id]
                rows_to_update.append(updated_row)

    return rows_to_insert, rows_to_update


def _execute_batch_insert(
    client: bigquery.Client,
    table_ref: str,
    data: pd.DataFrame,
    rows_to_insert: list,
    column_types: dict,
) -> None:
    """
    Execute a batch insert of new rows into the BigQuery table in smaller, more manageable batches.

    :param client: BigQuery client instance.
    :param table_ref: Table reference (formatted string).
    :param data: Original DataFrame (used for columns order).
    :param rows_to_insert: List of rows to insert.
    """
    if not rows_to_insert:
        return

    num_columns = len(data.columns)
    # Calculate the maximum number of rows per batch to not exceed 10,000 parameters
    max_rows_per_batch = 10000 // num_columns if num_columns > 0 else 500
    batch_size = min(
        500, max_rows_per_batch
    )  # Use 500 or the maximum safe batch size, whichever is smaller

    total_inserts = len(rows_to_insert)
    total_batches = (total_inserts + batch_size - 1) // batch_size
    inserted_count = 0

    for batch_index in range(total_batches):
        start_index = batch_index * batch_size
        end_index = min(start_index + batch_size, total_inserts)
        current_batch = rows_to_insert[start_index:end_index]

        values_clause = []
        parameters = []
        for i, row in enumerate(current_batch):
            placeholders = []
            # Use a unique index for parameter names based on the overall row index
            for col in data.columns:
                param_name = f"{col}_{start_index + i}"
                placeholders.append("@" + param_name)
                parameters.append(
                    bigquery.ScalarQueryParameter(
                        param_name,
                        column_types[col],
                        _prepare_param_value(row[col], column_types[col]),
                    )
                )
            placeholders.append("CURRENT_TIMESTAMP()")
            values_clause.append("(" + ", ".join(placeholders) + ")")

        app_logger.info(
            f"Inserting batch {batch_index + 1}/{total_batches} with {len(current_batch)} rows..."
        )
        insert_query = f"""
            INSERT INTO {table_ref} ({', '.join(data.columns)}, _synced_at)
            VALUES {', '.join(values_clause)}
        """
        insert_job_config = bigquery.QueryJobConfig(query_parameters=parameters)
        job = client.query(insert_query, job_config=insert_job_config)

        # Poll for job status using a timeout-based try/except loop
        poll_interval = 5  # seconds
        while True:
            try:
                job.result(timeout=poll_interval)
                break  # Job completed successfully
            except TimeoutError:
                app_logger.info(f"Insert batch {batch_index + 1} still running...")

        inserted_count += len(current_batch)
        app_logger.info(
            f"Batch {batch_index + 1} inserted. Total rows inserted so far: {inserted_count}/{total_inserts}."
        )

    app_logger.info(f"Inserted {inserted_count} rows in total.")


def _execute_batch_update(
    client: bigquery.Client,
    table_ref: str,
    reference_id: str,
    rows_to_update: list,
    column_types: dict,
) -> None:
    """
    Execute batch updates for rows that have changes.

    :param client: BigQuery client instance.
    :param table_ref: Table reference (formatted string).
    :param reference_id: Unique identifier column name.
    :param rows_to_update: List of rows (dicts) to update.
    """
    if not rows_to_update:
        return
    total_updates = len(rows_to_update)
    step = max(total_updates // 10, 1)

    for count, row in enumerate(rows_to_update, start=1):
        set_clauses = [
            f"{col} = @{col}"
            for col in row.keys()
            if col not in [reference_id, "_synced_at"]
        ]
        set_clauses.append("_synced_at = CURRENT_TIMESTAMP()")
        update_query = f"""
            UPDATE {table_ref}
            SET {', '.join(set_clauses)}
            WHERE {reference_id} = @{reference_id}
        """
        update_job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    col,
                    column_types[col],
                    _prepare_param_value(row[col], column_types[col]),
                )
                for col in row.keys()
                if col != "_synced_at"
            ]
        )
        job = client.query(update_query, job_config=update_job_config)
        job.result()  # Ensure completion

        if count % step == 0 or count == total_updates:
            percent = int((count / total_updates) * 100)
            app_logger.info(
                f"Processed {percent}% of batch updates ({count}/{total_updates})"
            )

    app_logger.info(f"Updated {total_updates} rows.")


def _log_update_mismatches(
    data: pd.DataFrame,
    existing_df: pd.DataFrame,
    reference_id: str,
    rows_to_update: list,
) -> None:
    """
    Log mismatches for updated rows by comparing new data with existing data.
    Each mismatch shows the reference ID, the column name, the existing value, and the new value.
    The output is indented for better readability.
    """
    import logging

    if not app_logger.isEnabledFor(logging.DEBUG):
        return
    diff_list = []
    for row in rows_to_update:
        ref = row.get(reference_id)
        new_rows = data.loc[data[reference_id] == ref]
        if new_rows.empty:
            continue
        new_row = new_rows.iloc[0]
        existing_rows = existing_df.loc[existing_df[reference_id] == ref]
        if existing_rows.empty:
            continue
        existing_row = existing_rows.iloc[0]
        for col, new_val in row.items():
            if col in [reference_id, "_synced_at"]:
                continue
            old_val = existing_row.get(col, None)
            # Compare values (using != for simplicity; adjust if needed)
            if not _equals(old_val, new_val):
                diff_list.append(
                    {
                        "reference_id": ref,
                        "column": col,
                        "existing_value": old_val,
                        "new_value": new_val,
                    }
                )
    if diff_list:
        import pandas as pd

        diff_df = pd.DataFrame(diff_list)
        import textwrap

        indented_str = textwrap.indent(diff_df.to_string(index=False), prefix="    ")
        print("  Mismatches for updated rows:\n" + indented_str)


@retry(
    stop=stop_after_attempt(7),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    before_sleep=_log_retry,
)
def sync_with_df(
    data: pd.DataFrame, reference_id: str, project: str, dataset: str, table: str
) -> None:
    """
    Synchronize the provided DataFrame with a BigQuery table.

    For each row in the DataFrame, the function:
      - Ensures the table exists (or creates/updates its schema as needed).
      - Queries the table for existing rows using the given reference ID.
      - Inserts new rows or updates rows where data has changed (ignoring _synced_at).

    :param data: DataFrame containing the data to sync.
    :param reference_id: Column name used as the unique identifier for rows.
    :param project: BigQuery project ID.
    :param dataset: BigQuery dataset name.
    :param table: BigQuery table name.
    :return: None
    """
    if not os.path.exists(config.SERVICE_ACCOUNT_FILE_PATH):
        app_logger.error(
            f"Service account file not found: {config.SERVICE_ACCOUNT_FILE_PATH}"
        )
        raise FileNotFoundError(
            f"Service account file not found: {config.SERVICE_ACCOUNT_FILE_PATH}"
        )

    credentials = service_account.Credentials.from_service_account_file(
        config.SERVICE_ACCOUNT_FILE_PATH
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    table_id = f"{project}.{dataset}.{table}"
    table_ref = f"`{table_id}`"
    column_types = {col: _get_bq_field_type(col, data[col]) for col in data.columns}

    # Ensure the table exists or create/update it with the appropriate schema
    _ensure_table_exists(client, table_id, table_ref, data)

    ref_ids = data[reference_id].tolist()
    if not ref_ids:
        app_logger.info("No reference IDs found in DataFrame; skipping update.")
        return

    # Retrieve existing rows from BigQuery
    existing_df = _get_existing_dataframe(client, table_ref, reference_id, ref_ids)

    # Prepare rows for batch insert and update
    rows_to_insert, rows_to_update = _prepare_rows(data, reference_id, existing_df)
    _log_update_mismatches(data, existing_df, reference_id, rows_to_update)

    # Execute batch insert and update
    _execute_batch_insert(client, table_ref, data, rows_to_insert, column_types)
    _execute_batch_update(client, table_ref, reference_id, rows_to_update, column_types)
    app_logger.info(
        f"Sync complete: {len(rows_to_insert)} rows added and {len(rows_to_update)} rows updated."
    )


# cspell: ignore db-dtypes iloc iterrows
