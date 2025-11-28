from __future__ import annotations

import pendulum
import pandas as pd
import pymssql

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.exceptions import AirflowException


PROJECT_ID = "rxo-dataeng-datalake-np"
BQ_CONN_ID = "google_cloud_default"
BQ_LOCATION = "us-central1"
BQ_DATASET = "dataops_admin"
BQ_FINAL_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.lookuptable_fk_pk_analysis"
BQ_STAGE_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.lookuptable_fk_pk_analysis_staging"


# === TASK 1: Extraer metadata desde BigQuery y guardarla en XCom ===
def extract_and_process_metadata(**context):
    """
    Connects to BigQuery, extracts metadata (database_name, schema_name, table_name)
    from extraction_metadata, and stores it in a Pandas DataFrame.
    Pushes the rows as list[dict] to XCom.
    """

    bq_hook = BigQueryHook(
        project_id=PROJECT_ID,
        gcp_conn_id=BQ_CONN_ID,
        location=BQ_LOCATION,
    )

    table_id = f"{PROJECT_ID}.{BQ_DATASET}.extraction_metadata"
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
        job = client.query(sql_query, location=BQ_LOCATION)
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


# === TASK 2: Ejecutar en SQL Server en batches y hacer join (PK + rowcount + DISTINCT) ===
def build_and_run_sqlserver_query(**context):
    """
    1) Pulls metadata from XCom.
    2) Deduplica (database_name, schema_name, table_name) y lo procesa en batches de 10.
    3) Para cada batch y para cada database_name:
       - Query PKs (cols ...Id + is_pk).
       - Query rowcount total de la tabla.
    4) Hace merge PK + total_rows.
    5) Para cada fila (Database, Schema, Table, column_id) ejecuta:
         SELECT COUNT(DISTINCT [column_id]) ...
       y agrega la columna distinct_values.
    6) Empuja df_final a XCom.
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

    # Lista blanca de DBs
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
            database="XpoMaster",
        )
        cursor = conn.cursor()

        # Iteramos por batches de 10 tablas
        for start in range(0, len(df_unique), batch_size):
            df_batch = df_unique.iloc[start:start + batch_size].copy()
            print(f"\n=== Processing batch rows {start} to {start + len(df_batch) - 1} ===")
            print(df_batch)

            dbs_in_batch = sorted(df_batch["database_name"].unique())
            print(f"Databases in this batch: {dbs_in_batch}")

            for db in dbs_in_batch:
                df_db = df_batch[df_batch["database_name"] == db].copy()
                if df_db.empty:
                    continue

                print(f"\n--- Processing DB: {db} in this batch ---")

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

                # === PK query (solo CTEs) ===
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

                cursor.execute(pk_tsql)
                rows_pk = cursor.fetchall()
                if rows_pk:
                    cols_pk = [col[0] for col in cursor.description]
                    df_pk_batch_db = pd.DataFrame.from_records(rows_pk, columns=cols_pk)
                    pk_dfs.append(df_pk_batch_db)
                    print(f"PK rows for DB {db}: {len(df_pk_batch_db)}")
                else:
                    print(f"PK query returned no rows for DB {db}.")

                # === COUNTS por tabla (solo CTEs) ===
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
    [Database]        = N'{db_esc}',
    [Schema]          = s.name,
    [Table]           = t.name,
    total_rows        = SUM(p.[rows])
FROM [{db_esc}].sys.tables t
JOIN [{db_esc}].sys.schemas s    ON s.schema_id = t.schema_id
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

        # Concatenar resultados PK y COUNTS
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

        # Join por tabla: PK + total_rows
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

        # === COUNT(DISTINCT) por columna_id ===
        distinct_rows = []

        for _, r in (
            df_final[["Database", "Schema", "Table", "column_id"]]
            .dropna()
            .drop_duplicates()
            .iterrows()
        ):
            db = r["Database"]
            sch = r["Schema"]
            tbl = r["Table"]
            col = r["column_id"]

            db_esc = str(db).replace("'", "''")
            sch_esc = str(sch).replace("'", "''")
            tbl_esc = str(tbl).replace("'", "''")
            col_esc = str(col).replace("'", "''")

            distinct_sql = f"""
SELECT
    DISTINCT_COUNT = COUNT(DISTINCT [{col_esc}])
FROM [{db_esc}].[{sch_esc}].[{tbl_esc}];
"""
            print("DISTINCT T-SQL:")
            print(distinct_sql)

            try:
                cursor.execute(distinct_sql)
                row = cursor.fetchone()
                distinct_count = row[0] if row else None
            except Exception as e:
                print(
                    f"Error computing DISTINCT for "
                    f"{db}.{sch}.{tbl}.{col}: {e}"
                )
                distinct_count = None

            distinct_rows.append(
                {
                    "Database": db,
                    "Schema": sch,
                    "Table": tbl,
                    "column_id": col,
                    "distinct_values": distinct_count,
                }
            )

        if distinct_rows:
            df_distinct = pd.DataFrame(distinct_rows)
        else:
            df_distinct = pd.DataFrame(
                columns=["Database", "Schema", "Table", "column_id", "distinct_values"]
            )

        print("\nDistinct counts DF:")
        print(df_distinct.head())
        print(f"Distinct rows: {len(df_distinct)}")

        # Merge final: PK + total_rows + distinct_values
        if not df_distinct.empty:
            df_final = df_final.merge(
                df_distinct,
                on=["Database", "Schema", "Table", "column_id"],
                how="left",
            )
        else:
            df_final["distinct_values"] = pd.NA

        print("\nFinal merged result (PK + total_rows + distinct_values):")
        print(df_final.head())
        print(f"Total merged rows: {len(df_final)}")

        # Push a XCom para la siguiente tarea
        ti.xcom_push(
            key="pk_with_rowcounts_and_distinct",
            value=df_final.to_dict(orient="records"),
        )

    finally:
        if conn is not None:
            conn.close()


# === TASK 3: Escribir resultado final en BigQuery con upsert ===
def write_results_to_bigquery(**context):
    """
    Lee el resultado final desde XCom, lo carga a una tabla staging en BQ,
    crea la tabla final si no existe y hace MERGE (upsert) basado en
    (database_name, schema_name, table_name, column_name).
    """

    ti = context["ti"]
    records = ti.xcom_pull(
        task_ids="build_and_run_sqlserver_pk_query_dynamic_batches",
        key="pk_with_rowcounts_and_distinct",
    )

    if not records:
        print("No final records found in XCom. Nothing to write to BigQuery.")
        return

    df = pd.DataFrame(records)
    if df.empty:
        print("Final DataFrame is empty. Nothing to write to BigQuery.")
        return

    # Renombrar columnas para BQ
    df_bq = df.rename(
        columns={
            "Database": "database_name",
            "Schema": "schema_name",
            "Table": "table_name",
            "column_id": "column_name",
        }
    )[["database_name", "schema_name", "table_name", "column_name", "is_pk", "total_rows", "distinct_values"]]

    # Forzar tipos numéricos donde tiene sentido
    df_bq["is_pk"] = pd.to_numeric(df_bq["is_pk"], errors="coerce").astype("Int64")
    df_bq["total_rows"] = pd.to_numeric(df_bq["total_rows"], errors="coerce").astype("Int64")
    df_bq["distinct_values"] = pd.to_numeric(df_bq["distinct_values"], errors="coerce").astype("Int64")

    # Timestamp de carga
    df_bq["load_ts"] = pd.Timestamp.utcnow()

    print("DataFrame to load into BigQuery:")
    print(df_bq.head())
    print(f"Rows to write: {len(df_bq)}")

    bq_hook = BigQueryHook(
        project_id=PROJECT_ID,
        gcp_conn_id=BQ_CONN_ID,
        location=BQ_LOCATION,
    )
    client = bq_hook.get_client()

    # 1) Cargar a tabla staging (se crea si no existe)
    load_job = client.load_table_from_dataframe(
        df_bq,
        BQ_STAGE_TABLE,
    )
    load_job.result()
    print(f"Loaded {len(df_bq)} rows into staging table {BQ_STAGE_TABLE}.")

    # 2) Crear tabla final si no existe
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{BQ_FINAL_TABLE}` (
      database_name   STRING,
      schema_name     STRING,
      table_name      STRING,
      column_name     STRING,
      is_pk           INT64,
      total_rows      INT64,
      distinct_values INT64,
      load_ts         TIMESTAMP
    )
    """
    client.query(ddl, location=BQ_LOCATION).result()
    print(f"Ensured final table {BQ_FINAL_TABLE} exists.")

    # 3) MERGE (upsert) desde staging a final
    merge_sql = f"""
    MERGE `{BQ_FINAL_TABLE}` T
    USING `{BQ_STAGE_TABLE}` S
    ON T.database_name = S.database_name
       AND T.schema_name = S.schema_name
       AND T.table_name = S.table_name
       AND T.column_name = S.column_name
    WHEN MATCHED AND (
        T.is_pk           IS DISTINCT FROM S.is_pk OR
        T.total_rows      IS DISTINCT FROM S.total_rows OR
        T.distinct_values IS DISTINCT FROM S.distinct_values
    ) THEN
      UPDATE SET
        T.is_pk           = S.is_pk,
        T.total_rows      = S.total_rows,
        T.distinct_values = S.distinct_values,
        T.load_ts         = S.load_ts
    WHEN NOT MATCHED THEN
      INSERT (database_name, schema_name, table_name, column_name,
              is_pk, total_rows, distinct_values, load_ts)
      VALUES (S.database_name, S.schema_name, S.table_name, S.column_name,
              S.is_pk, S.total_rows, S.distinct_values, S.load_ts);
    """
    client.query(merge_sql, location=BQ_LOCATION).result()
    print(f"Merge completed into {BQ_FINAL_TABLE}.")

    # 4) Limpiar tabla staging
    try:
        client.delete_table(BQ_STAGE_TABLE, not_found_ok=True)
        print(f"Staging table {BQ_STAGE_TABLE} dropped.")
    except Exception as e:
        print(f"Error dropping staging table {BQ_STAGE_TABLE}: {e}")


# === Definición del DAG ===
with DAG(
    dag_id="bq_to_sqlserver_pk_catalog_pymssql_batches_cte_only_distinct_to_bq_v1",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=[
        "bigquery",
        "metadata",
        "sqlserver",
        "pk_catalog",
        "rowcount",
        "distinct_values",
        "dynamic",
        "batches",
        "bq_upsert",
    ],
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

    write_bq_task = PythonOperator(
        task_id="write_pk_analysis_to_bigquery",
        python_callable=write_results_to_bigquery,
        provide_context=True,
    )

    extract_metadata_task >> run_sqlserver_pk_task >> write_bq_task
