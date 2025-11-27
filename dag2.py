from __future__ import annotations

import pendulum
import pandas as pd
import pyodbc

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.exceptions import AirflowException


# === TASK 1: Extraer metadata desde BigQuery y guardarla en XCom ===
def extract_and_process_metadata(**context):
    """
    Connects to BigQuery, extracts metadata (database_name, schema_name, table_name)
    from extraction_metadata, and stores it in a Pandas DataFrame.
    Pushes the rows as list[dict] to XCom.
    """

    bq_hook = BigQueryHook(
        project_id="rxo-dataeng-datalake-np",
        gcp_conn_id="google_cloud_default",
        location="us-central1",
    )

    table_id = "rxo-dataeng-datalake-np.dataops_admin.extraction_metadata"
    source_type_filter = "sqlserver"
    table_type_filter = "extraction"

    sql_query = f"""
    SELECT
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
        client = bq_hook.get_client()
        job = client.query(sql_query, location="us-central1")
        rows = list(job.result())
    except AirflowException as e:
        print(f"Airflow Exception while executing BigQuery query: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred while executing BigQuery query: {e}")
        raise

    rows_dicts = [dict(r) for r in rows]
    df = pd.DataFrame(rows_dicts)

    if df.empty:
        print("No records found matching the criteria.")
    else:
        print("Metadata extracted and processed into DataFrame:")
        print(df.head())
        print(f"Total rows: {len(df)}")

    # Push list[dict] to XCom for the next task
    context["ti"].xcom_push(key="metadata_rows", value=rows_dicts)


# === TASK 2: Construir query tipo query1 desde el DataFrame y ejecutarla en SQL Server ===
def build_and_run_sqlserver_query(**context):
    """
    Pulls metadata from XCom, builds a T-SQL query (based on query1)
    using database_name, schema_name, table_name from BigQuery,
    executes it on SQL Server, and loads the result into a DataFrame.
    """

    ti = context["ti"]
    metadata_rows = ti.xcom_pull(
        task_ids="extract_and_process_sqlserver_metadata",
        key="metadata_rows",
    )

    if not metadata_rows:
        print("No metadata_rows received from XCom. Nothing to do.")
        return

    df_meta = pd.DataFrame(metadata_rows)
    print("Metadata from BigQuery:")
    print(df_meta.head())

    # 1) Construir VALUES (...) para input_rows a partir del DataFrame
    values_rows = []
    for _, row in df_meta.iterrows():
        db  = (row.get("database_name") or "").replace("'", "''")
        sch = (row.get("schema_name")   or "").replace("'", "''")
        tbl = (row.get("table_name")    or "").replace("'", "''")
        # Column y is_pk_flag: placeholders por ahora
        values_rows.append(
            f"(N'{db}', N'{sch}', N'{tbl}', N'', N'0')"
        )

    if not values_rows:
        print("No valid rows to build VALUES clause.")
        return

    values_clause = ",\n        ".join(values_rows)

    # 2) Construir pk_catalog a partir de databases únicas
    db_names = sorted(
        {row["database_name"] for row in metadata_rows if row.get("database_name")}
    )
    if not db_names:
        print("No database_name values found in metadata.")
        return

    select_fragments = []
    for db in db_names:
        db_escaped = db.replace("'", "''")
        fragment = f"""
SELECT
    DBName   = N'{db_escaped}',
    SchemaPk = s.name,
    TablePk  = t.name,
    ColumnPk = c.name
FROM [{db_escaped}].sys.tables t
JOIN [{db_escaped}].sys.schemas s
  ON s.schema_id = t.schema_id
JOIN [{db_escaped}].sys.key_constraints kc
  ON kc.parent_object_id = t.object_id
 AND kc.type = 'PK'
JOIN [{db_escaped}].sys.index_columns ic
  ON ic.object_id = t.object_id
 AND ic.index_id = kc.unique_index_id
JOIN [{db_escaped}].sys.columns c
  ON c.object_id = ic.object_id
 AND c.column_id = ic.column_id
"""
        select_fragments.append(fragment.strip())

    pk_catalog_part = "\nUNION ALL\n".join(select_fragments)

    # 3) Query final estilo query1, pero generada desde el DataFrame
    final_tsql = f"""
WITH input_rows AS (
    SELECT *
    FROM (VALUES
        {values_clause}
    ) v([Database],[Schema],[Table],[Column],[is_pk_flag])
),
rows_0 AS (
    SELECT [Database],[Schema],[Table],[Column]
    FROM input_rows
    WHERE is_pk_flag = N'0'
),
pk_catalog AS (
{pk_catalog_part}
)
SELECT * FROM pk_catalog;
"""

    print("Final T-SQL to be executed on SQL Server:")
    print(final_tsql)

    # 4) Ejecutar en SQL Server y cargar a DataFrame
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=fbtdw2090.qaamer.qacorp.xpo.com;"
        "DATABASE=master;"  # DB inicial neutra; la query usa [db].sys.XXX
        "UID=svcGCPDataEngg;"
        "PWD=OXZ6q67wr77k"
    )

    conn = None
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute(final_tsql)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        df_pk = pd.DataFrame.from_records(rows, columns=columns)

        print("Result from SQL Server PK catalog:")
        print(df_pk.head())
        print(f"Total rows from SQL Server: {len(df_pk)}")

        # Opcional: mandar el resultado a XCom
        # ti.xcom_push(key="pk_catalog", value=df_pk.to_dict(orient="records"))

    finally:
        if conn is not None:
            conn.close()


# === DAG definition ===
with DAG(
    dag_id="bq_to_sqlserver_pk_catalog_v1",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,  # solo manual
    tags=["bigquery", "metadata", "sqlserver", "pk_catalog"],
    doc_md="""
    ### DAG: BigQuery metadata → dynamic PK catalog in SQL Server

    1) Extracts `database_name`, `schema_name`, `table_name` from
       `dataops_admin.extraction_metadata` in BigQuery.
    2) Builds a dynamic T-SQL query (based on the original query1)
       using that metadata to inspect PKs across all listed databases.
    3) Executes the query on SQL Server via pyodbc.
    4) Loads the result into a Pandas DataFrame (printed in logs).
    """,
) as dag:

    extract_metadata_task = PythonOperator(
        task_id="extract_and_process_sqlserver_metadata",
        python_callable=extract_and_process_metadata,
        provide_context=True,
    )

    run_sqlserver_pk_task = PythonOperator(
        task_id="build_and_run_sqlserver_pk_query",
        python_callable=build_and_run_sqlserver_query,
        provide_context=True,
    )

    extract_metadata_task >> run_sqlserver_pk_task
