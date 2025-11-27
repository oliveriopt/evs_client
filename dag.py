from __future__ import annotations

import pendulum
import pandas as pd

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.exceptions import AirflowException


def extract_and_process_metadata(**context):
    """
    Connects to BigQuery, extracts metadata, parses the source_config JSON,
    and stores relevant information in a Pandas DataFrame.
    """

    # BigQuery connection
    bq_hook = BigQueryHook(
        project_id="rxo-dataeng-datalake-np",   # GCP Project ID
        gcp_conn_id="google_cloud_default",     # Airflow GCP Connection ID
        location="us-central1",                 # BigQuery dataset location
    )

    # BigQuery dataset.table (sin el project, el hook ya lo conoce)
    table_id = "dataops_admin.extraction_metadata"
    source_type_filter = "sqlserver"
    table_type_filter = "extraction"

    # SQL: extraemos campos desde el JSON source_config
    sql_query = f"""
    SELECT
        source_type,
        table_type,
        JSON_EXTRACT_SCALAR(source_config, '$.database_name') AS database_name,
        JSON_EXTRACT_SCALAR(source_config, '$.schema_name')   AS schema_name,
        JSON_EXTRACT_SCALAR(source_config, '$.table_name')    AS table_name
    FROM `{table_id}`
    WHERE
        source_type = '{source_type_filter}'
        AND table_type = '{table_type_filter}'
    """

    print(f"Executing BigQuery SQL Query:\n{sql_query}")

    try:
        results = bq_hook.get_records(sql=sql_query)
    except AirflowException as e:
        print(f"Airflow Exception while executing BigQuery query: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred while executing BigQuery query: {e}")
        raise

    # get_records devuelve una lista de tuplas; definimos columnas explícitamente
    df = pd.DataFrame(
        results,
        columns=[
            "source_type",
            "table_type",
            "database_name",
            "schema_name",
            "table_name",
        ],
    )

    if df.empty:
        print("No records found matching the criteria.")
    else:
        print("Metadata extracted and processed into DataFrame:")
        print(df.head())
        print(f"Total rows: {len(df)}")

    # Ejemplo si quisieras empujar algo a XCom (cuidado con el tamaño)
    # context["ti"].xcom_push(
    #     key="extracted_metadata_df_head",
    #     value=df.head().to_json(orient="records"),
    # )


with DAG(
    dag_id="bq_metadata_sqlserver_extraction_v1",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,  # Solo se ejecuta manualmente
    tags=["bigquery", "metadata", "sqlserver", "extraction"],
    doc_md="""
    ### DAG for SQL Server Metadata Extraction from BigQuery

    This DAG connects to BigQuery, queries the `extraction_metadata` table
    for SQL Server records, extracts key information from `source_config`
    (`database_name`, `schema_name`, `table_name`), and organizes it into
    a Pandas DataFrame. Designed to be triggered manually.
    """,
) as dag:

    extract_metadata_task = PythonOperator(
        task_id="extract_and_process_sqlserver_metadata_v3",
        python_callable=extract_and_process_metadata,
        provide_context=True,
    )
