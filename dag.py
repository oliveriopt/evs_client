from __future__ import annotations

import pendulum
import json
import pandas as pd

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.exceptions import AirflowException # Import AirflowException for specific error handling

def extract_and_process_metadata(**context):
    """
    Connects to BigQuery, extracts metadata, parses the source_config JSON,
    and stores relevant information in a Pandas DataFrame.
    """
    # Using 'google_cloud_default' gcp_conn_id is standard in Composer.
    # Ensure your Composer service account has permissions to read the BigQuery table.
    bq_hook = BigQueryHook(
        project_id="rxo-dataeng-datalake-np",  # Your GCP Project ID
        gcp_conn_id="google_cloud_default",     # Your Airflow GCP Connection ID
        location="us-central1"
    )

    # Define the BigQuery table and filter conditions
    # The table is 'rxo-dataeng-datalake-uat.dataops_admin.extraction_metadata'
    table_id = 'rxo-dataeng-datalake-np.dataops_admin.extraction_metadata'
    source_type_filter = 'sqlserver'
    table_type_filter = 'extraction'

    # Construct the SQL query. We use JSON_EXTRACT_SCALAR to extract values from JSON.
    # This is more robust and handles cases where keys might not exist gracefully.
    sql_query = f"""
    SELECT
        source_type,
        table_type,
        JSON_EXTRACT_SCALAR(source_config, '$.database_name') AS database_name,
        JSON_EXTRACT_SCALAR(source_config, '$.schema_name') AS schema_name,
        JSON_EXTRACT_SCALAR(source_config, '$.table_name') AS table_name
    FROM
        `{table_id}`
    WHERE
        source_type = '{source_type_filter}' AND table_type = '{table_type_filter}'
    """

    # Execute the query and fetch results as a list of dictionaries.
    # Each dictionary represents a row, and keys are column names.
    print(f"Executing BigQuery SQL Query:\n{sql_query}")
    try:
        # REMOVED: 'location' parameter from get_records() call.
        # Your current BigQueryHook version does not support a 'location' argument here.
        # BigQuery will attempt to infer the location based on the dataset/project.
        results = bq_hook.get_records(sql=sql_query)
    except AirflowException as e:
        print(f"Airflow Exception while executing BigQuery query: {e}")
        raise # Re-raise the AirflowException to mark the task as failed.
    except Exception as e:
        print(f"An unexpected error occurred while executing BigQuery query: {e}")
        raise # Re-raise any other exception for Airflow to catch.

    # Process results directly into a Pandas DataFrame.
    # Columns are already separated thanks to JSON_EXTRACT_SCALAR in the SQL query.
    df = pd.DataFrame(results)

    # If the DataFrame is empty, no matching records were found.
    if df.empty:
        print("No records found matching the criteria.")
    else:
        print("Metadata extracted and processed into DataFrame:")
        print(df.head()) # Show the first few rows of the DataFrame
        print(f"Total rows: {len(df)}")

    # You can now add further steps to work with the DataFrame:
    # - Save it to GCS (e.g., df.to_csv('gs://your-bucket/metadata.csv'), df.to_parquet(...))
    # - Load it into another BigQuery table
    # - Perform additional analysis or transformations
    # - Pass it to a downstream task via XComs (be cautious with large DataFrames!)

    # Example of passing a small part of the DataFrame via XCom (not recommended for large DataFrames)
    # context['ti'].xcom_push(key='extracted_metadata_df_head', value=df.head().to_json())


with DAG(
    dag_id="bq_metadata_sqlserver_extraction_v1",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,         # Do not run caught-up DAG runs if activated after start_date
    schedule=None,         # IMPORTANT: This makes the DAG run only when triggered manually
    tags=["bigquery", "metadata", "sqlserver", "extraction"],
    doc_md="""
    ### DAG for SQL Server Metadata Extraction from BigQuery
    This DAG connects to BigQuery, queries the `extraction_metadata` table
    for SQL Server records, extracts key information from `source_config`
    (database_name, schema_name, table_name), and organizes it into a Pandas DataFrame.
    Designed to be triggered manually.
    """,
) as dag:
    extract_metadata_task = PythonOperator(
        task_id="extract_and_process_sqlserver_metadata_v3",
        python_callable=extract_and_process_metadata,
        # The python_callable function receives Airflow arguments (ti, dag_run, etc.)
        # You can add op_kwargs if your python_callable function needs specific arguments.
    )

