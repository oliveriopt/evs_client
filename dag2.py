from __future__ import annotations

import pendulum
import pandas as pd
import pymssql

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

    context["ti"].xcom_push(key="metadata_rows", value=rows_dicts)


# === TASK 2: Ejecutar en SQL Server en batches y hacer join (PK + rowcount) ===
def build_and_run_sqlserver_query(**context):
    """
    1) Pulls metadata from XCom.
    2) Deduplica (database_name, schema_name, table_name) y lo procesa en batches de 10.
    3) Para cada batch y para cada database_name del batch:
       - Construye un CTE todo(...) desde VALUES.
       - Ejecuta una query con solo CTEs para PKs.
       - Ejecuta otra query con solo CTEs para rowcounts.
    4) Concatena resultados y hace merge en Pandas.
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
    print("Full metadata from BigQuery:")
    print(df_meta.head())

    # Limpiamos filas sin database/schema/table
    df_meta = df_meta.dropna(subset=["database_name", "schema_name", "table_name"]).copy()

    if df_meta.empty:
        print("No valid rows with non-null database/schema/table. Nothing to do.")
        return

    # Normalizamos strings y deduplicamos
    df_unique = (
        df_meta.assign(
            database_name=lambda d: d["database_name"].astype(str).str.strip(),
            schema_name=lambda d: d["schema_name"].astype(str).str.strip(),
            table_name=lambda d: d["table_name"].astype(str).str.strip(),
        )[["database_name", "schema_name", "table_name"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    print(f"Unique triplets (db, schema, table): {len(df_unique)}")

    # Lista blanca de DBs que sabes que existen en este servidor
    allowed_dbs = {"XpoMaster", "brkLTL", "XPOCustomer"}
    df_unique = df_unique[df_unique["database_name"].isin(allowed_dbs)].reset_index(drop=True)
    print(f"Unique triplets after allowed_dbs filter: {len(df_unique)}")

    if df_unique.empty:
        print("No rows after filtering by allowed_dbs.")
        return

    batch_size = 10

    pk_dfs = []
    cnt_dfs = []

    conn = None
    try:
        conn = pymssql.connect(
            server="fbtdw2090.qaamer.qacorp.xpo.com",
            user="svcGCPDataEngg",
            password="OXZ6q67wr77k",
            database="XpoMaster",  # DB inicial; luego usamos [DatabaseName].sys.*
        )
        cursor = conn.cursor()

        # Iteramos por batches de 10 tablas
        for start in range(0, len(df_unique), batch_size):
            df_batch = df_unique.iloc[start:start + batch_size].copy()
            print(f"\n=== Processing batch rows {start} to {start + len(df_batch) - 1} ===")
            print(df_batch)

            # Databases únicas en este batch
            dbs_in_batch = sorted(df_batch["database_name"].unique())
            print(f"Databases in this batch: {dbs_in_batch}")

            for db in dbs_in_batch:
                df_db = df_batch[df_batch["database_name"] == db].copy()
                if df_db.empty:
                    continue

                print(f"\n--- Processing DB: {db} in this batch ---")

                # VALUES(...) solo con las tablas de esta DB
                values_rows = []
                for _, row in df_db.iterrows():
                    sch = row["schema_name"]
                    tbl = row["table_name"]

                    db_esc = db.replace("'", "''")
                    sch_esc = sch.replace("'", "''")
                    tbl_esc = tbl.replace("'", "''")

                    values_rows.append(
                        f"(N'{db_esc}', N'{sch_esc}', N'{tbl_esc}')"
                    )

                values_clause = ",\n        ".join(values_rows)

                # === PK query para esta DB (solo CTEs) ===
                pk_tsql = f"""
WITH todo AS (
    SELECT DISTINCT
        LTRIM(RTRIM(DatabaseName)) AS DatabaseName,
        LTRIM(RTRIM(SchemaName))   AS SchemaName,
        LTRIM(RTRIM(TableName))    AS TableName
    FROM (VALUES
        {values_clause}
    ) v(DatabaseName, SchemaName, TableName)
),
cols AS (
    SELECT 
        DB      = N'{db_esc}',
        s.name  AS [Schema],
        t.name  AS [Table],
        c.name  AS column_id
    FROM [{db_esc}].sys.tables t
    JOIN [{db_esc}].sys.schemas s ON s.schema_id = t.schema_id
    JOIN [{db_esc}].sys.columns c ON c.object_id = t.object_id
    JOIN todo td
      ON td.SchemaName = s.name
     AND td.TableName  = t.name
    WHERE c.name LIKE N'%Id'
),
pk_cols AS (
    SELECT 
        s.name AS [Schema],
        t.name AS [Table],
        c.name AS column_id
    FROM [{db_esc}].sys.tables t
    JOIN [{db_esc}].sys.schemas s ON s.schema_id = t.schema_id
    JOIN [{db_esc}].sys.key_constraints kc
      ON kc.parent_object_id = t.object_id AND kc.type = 'PK'
    JOIN [{db_esc}].sys.index_columns ic
      ON ic.object_id = t.object_id AND ic.index_id = kc.unique_index_id
    JOIN [{db_esc}].sys.columns c
      ON c.object_id = ic.object_id AND c.column_id = ic.column_id
),
pick AS (
    SELECT 
        cols.DB      AS [Database],
        cols.[Schema],
        cols.[Table],
        cols.column_id,
        is_pk = CASE WHEN pk_cols.column_id IS NOT NULL THEN 1 ELSE 0 END
    FROM cols
    LEFT JOIN pk_cols
      ON pk_cols.[Schema]   = cols.[Schema]
     AND pk_cols.[Table]    = cols.[Table]
     AND pk_cols.column_id  = cols.column_id
)
SELECT [Database], [Schema], [Table], column_id, is_pk
FROM pick
ORDER BY [Database], [Schema], [Table], column_id;
"""

                print("PK T-SQL for this DB:")
                print(pk_tsql)

                # Ejecutar PK
                cursor.execute(pk_tsql)
                rows_pk = cursor.fetchall()
                if rows_pk:
                    cols_pk = [col[0] for col in cursor.description]
                    df_pk_batch_db = pd.DataFrame.from_records(rows_pk, columns=cols_pk)
                    pk_dfs.append(df_pk_batch_db)
                    print(f"PK rows for DB {db}: {len(df_pk_batch_db)}")
                else:
                    print(f"PK query returned no rows for DB {db}.")

                # === COUNTS query para esta DB (solo CTEs) ===
                counts_tsql = f"""
WITH todo AS (
    SELECT DISTINCT
        LTRIM(RTRIM(DatabaseName)) AS DatabaseName,
        LTRIM(RTRIM(SchemaName))   AS SchemaName,
        LTRIM(RTRIM(TableName))    AS TableName
    FROM (VALUES
        {values_clause}
    ) v(DatabaseName, SchemaName, TableName)
)
SELECT
    [Database]  = N'{db_esc}',
    [Schema]    = s.name,
    [Table]     = t.name,
    total_rows  = SUM(p.[rows])
FROM [{db_esc}].sys.tables t
JOIN [{db_esc}].sys.schemas s ON s.schema_id = t.schema_id
JOIN [{db_esc}].sys.partitions p ON p.object_id = t.object_id
JOIN todo td
  ON td.SchemaName = s.name
 AND td.TableName  = t.name
WHERE p.index_id IN (0, 1)
GROUP BY s.name, t.name
ORDER BY [Database], [Schema], [Table];
"""

                print("COUNTS T-SQL for this DB:")
                print(counts_tsql)

                cursor.execute(counts_tsql)
                rows_cnt = cursor.fetchall()
                if rows_cnt:
                    cols_cnt = [col[0] for col in cursor.description]
                    df_cnt_batch_db = pd.DataFrame.from_records(rows_cnt, columns=cols_cnt)
                    cnt_dfs.append(df_cnt_batch_db)
                    print(f"Rowcount rows for DB {db}: {len(df_cnt_batch_db)}")
                else:
                    print(f"Rowcount query returned no rows for DB {db}.")

        # Concatenar todos los resultados
        if pk_dfs:
            df_pk_all = pd.concat(pk_dfs, ignore_index=True)
        else:
            df_pk_all = pd.DataFrame(columns=["Database", "Schema", "Table", "column_id", "is_pk"])

        if cnt_dfs:
            df_cnt_all = pd.concat(cnt_dfs, ignore_index=True)
        else:
            df_cnt_all = pd.DataFrame(columns=["Database", "Schema", "Table", "total_rows"])

        print("\nGlobal PK DF:")
        print(df_pk_all.head())
        print(f"Global PK rows: {len(df_pk_all)}")

        print("\nGlobal COUNTS DF:")
        print(df_cnt_all.head())
        print(f"Global rowcount rows: {len(df_cnt_all)}")

        # Join final PK + total_rows
        if not df_pk_all.empty and not df_cnt_all.empty:
            df_final = df_pk_all.merge(
                df_cnt_all,
                on=["Database", "Schema", "Table"],
                how="left",
            )
        else:
            df_final = df_pk_all.copy()
            if "total_rows" not in df_final.columns:
                df_final["total_rows"] = pd.NA

        print("\nFinal merged result (PK + total_rows):")
        print(df_final.head())
        print(f"Total merged rows: {len(df_final)}")

        # Opcional: XCom
        # ti.xcom_push(key="pk_with_rowcounts", value=df_final.to_dict(orient="records"))

    finally:
        if conn is not None:
            conn.close()


# === Definición del DAG ===
with DAG(
    dag_id="bq_to_sqlserver_pk_catalog_pymssql_batches_cte_only_v1",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["bigquery", "metadata", "sqlserver", "pk_catalog", "rowcount", "dynamic", "batches"],
) as dag:

    extract_metadata_task = PythonOperator(
        task_id="extract_and_process_sqlserver_metadata",
        python_callable=extract_and_process_metadata,
        provide_context=True,
    )

    run_sqlserver_pk_task = PythonOperator(
        task_id="build_and_run_sqlserver_pk_query_dynamic_batches",
        python_callable=build_and_run_sqlserver_query,
        provide_context=True,
    )

    extract_metadata_task >> run_sqlserver_pk_task
