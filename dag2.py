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
    3) Para cada batch:
       - Ejecuta Query PK: [Database, Schema, Table, column_id, is_pk].
       - Ejecuta Query ROWCOUNT: [Database, Schema, Table, total_rows].
    4) Concatena resultados de todos los batches y hace merge en Pandas.
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

    # Nos quedamos con combinaciones únicas (Database, Schema, Table)
    df_unique = (
        df_meta[["database_name", "schema_name", "table_name"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    print(f"Unique triplets (db, schema, table): {len(df_unique)}")

    batch_size = 10

    # Acumuladores globales
    pk_dfs = []
    cnt_dfs = []

    # Conexión única a SQL Server
    conn = None
    try:
        conn = pymssql.connect(
            server="fbtdw2090.qaamer.qacorp.xpo.com",
            user="svcGCPDataEngg",
            password="OXZ6q67wr77k",
            database="XpoMaster",  # DB inicial; las queries usan [DatabaseName].sys.*
        )
        cursor = conn.cursor()

        # Procesar en batches
        for start in range(0, len(df_unique), batch_size):
            df_batch = df_unique.iloc[start:start + batch_size].copy()
            print(f"Processing batch rows {start} to {start + len(df_batch) - 1}")

            # Construimos VALUES(...) para este batch
            values_rows = []
            for _, row in df_batch.iterrows():
                db  = str(row["database_name"]).strip()
                sch = str(row["schema_name"]).strip()
                tbl = str(row["table_name"]).strip()

                db_escaped  = db.replace("'", "''")
                sch_escaped = sch.replace("'", "''")
                tbl_escaped = tbl.replace("'", "''")

                values_rows.append(
                    f"(N'{db_escaped}', N'{sch_escaped}', N'{tbl_escaped}')"
                )

            if not values_rows:
                print("No rows in this batch, skipping.")
                continue

            values_clause = ",\n        ".join(values_rows)

            # === Query 1 para el batch: PK catalog ===
            pk_tsql = f"""
/* === Batch PK: Lista objetivo (Database, Schema, Table) === */
WITH todo_raw (DatabaseName, SchemaName, TableName) AS (
    SELECT *
    FROM (VALUES
        {values_clause}
    ) v(DatabaseName, SchemaName, TableName)
)
SELECT DISTINCT
    DatabaseName = LTRIM(RTRIM(DatabaseName)),
    SchemaName   = LTRIM(RTRIM(SchemaName)),
    TableName    = LTRIM(RTRIM(TableName))
INTO #todo
FROM todo_raw;

DECLARE @sql NVARCHAR(MAX) = N'';

SELECT
    @sql = STRING_AGG(CAST(BlockSql AS NVARCHAR(MAX)), CHAR(10) + 'UNION ALL' + CHAR(10))
FROM (
    SELECT
        d.DatabaseName,
        BlockSql =
N';WITH cols AS (
    SELECT 
        DB      = N''' + d.DatabaseName + ''',
        s.name  AS [Schema],
        t.name  AS [Table],
        c.name  AS column_id
    FROM [' + d.DatabaseName + '].sys.tables t
    JOIN [' + d.DatabaseName + '].sys.schemas s ON s.schema_id = t.schema_id
    JOIN [' + d.DatabaseName + '].sys.columns c ON c.object_id = t.object_id
    WHERE c.name LIKE N''%Id''
),
pk_cols AS (
    SELECT 
        s.name AS [Schema],
        t.name AS [Table],
        c.name AS column_id
    FROM [' + d.DatabaseName + '].sys.tables t
    JOIN [' + d.DatabaseName + '].sys.schemas s ON s.schema_id = t.schema_id
    JOIN [' + d.DatabaseName + '].sys.key_constraints kc
      ON kc.parent_object_id = t.object_id AND kc.type = ''PK''
    JOIN [' + d.DatabaseName + '].sys.index_columns ic
      ON ic.object_id = t.object_id AND ic.index_id = kc.unique_index_id
    JOIN [' + d.DatabaseName + '].sys.columns c
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
    JOIN #todo td
      ON td.DatabaseName = cols.DB
     AND td.SchemaName   = cols.[Schema]
     AND td.TableName    = cols.[Table]
    LEFT JOIN pk_cols
      ON pk_cols.[Schema]   = cols.[Schema]
     AND pk_cols.[Table]    = cols.[Table]
     AND pk_cols.column_id  = cols.column_id
)
SELECT [Database], [Schema], [Table], column_id, is_pk
FROM pick'
    FROM (
        SELECT DISTINCT DatabaseName
        FROM #todo
    ) d
) AS q;

IF @sql IS NULL OR LEN(@sql) = 0
BEGIN
    DROP TABLE #todo;
    RAISERROR('No databases found in #todo for PK query (batch).', 16, 1);
    RETURN;
END;

SET @sql = @sql + CHAR(10) + 'ORDER BY [Database], [Schema], [Table], column_id;';

EXEC sys.sp_executesql @sql;

DROP TABLE #todo;
"""

            # === Query 2 para el batch: Rowcount por tabla ===
            counts_tsql = f"""
/* === Batch COUNTS: Lista objetivo (Database, Schema, Table) === */
WITH todo_raw (DatabaseName, SchemaName, TableName) AS (
    SELECT *
    FROM (VALUES
        {values_clause}
    ) v(DatabaseName, SchemaName, TableName)
)
SELECT DISTINCT
    DatabaseName = LTRIM(RTRIM(DatabaseName)),
    SchemaName   = LTRIM(RTRIM(SchemaName)),
    TableName    = LTRIM(RTRIM(TableName))
INTO #todo
FROM todo_raw;

DECLARE @sql2 NVARCHAR(MAX) = N'';

SELECT
    @sql2 = STRING_AGG(CAST(BlockSql AS NVARCHAR(MAX)), CHAR(10) + 'UNION ALL' + CHAR(10))
FROM (
    SELECT
        d.DatabaseName,
        BlockSql =
N'SELECT
    [Database]  = N''' + d.DatabaseName + ''',
    [Schema]    = s.name,
    [Table]     = t.name,
    total_rows  = SUM(p.[rows])
FROM [' + d.DatabaseName + '].sys.tables t
JOIN [' + d.DatabaseName + '].sys.schemas s ON s.schema_id = t.schema_id
JOIN [' + d.DatabaseName + '].sys.partitions p ON p.object_id = t.object_id
JOIN #todo td
  ON td.DatabaseName = N''' + d.DatabaseName + '''
 AND td.SchemaName   = s.name
 AND td.TableName    = t.name
WHERE p.index_id IN (0, 1)
GROUP BY s.name, t.name'
    FROM (
        SELECT DISTINCT DatabaseName
        FROM #todo
    ) d
) AS q;

IF @sql2 IS NULL OR LEN(@sql2) = 0
BEGIN
    DROP TABLE #todo;
    RAISERROR('No databases found in #todo for rowcount query (batch).', 16, 1);
    RETURN;
END;

SET @sql2 = @sql2 + CHAR(10) + 'ORDER BY [Database], [Schema], [Table];';

EXEC sys.sp_executesql @sql2;

DROP TABLE #todo;
"""

            # Ejecutar PK para el batch
            print("Executing PK batch T-SQL:")
            print(pk_tsql)
            cursor.execute(pk_tsql)
            try:
                rows_pk = cursor.fetchall()
                if rows_pk:
                    cols_pk = [col[0] for col in cursor.description]
                    df_pk_batch = pd.DataFrame.from_records(rows_pk, columns=cols_pk)
                    pk_dfs.append(df_pk_batch)
                    print(f"PK rows in this batch: {len(df_pk_batch)}")
                else:
                    print("PK query (batch) returned no rows.")
            except pymssql.ProgrammingError:
                print("PK query (batch) did not return a resultset.")

            # Ejecutar COUNTS para el batch
            print("Executing COUNTS batch T-SQL:")
            print(counts_tsql)
            cursor.execute(counts_tsql)
            try:
                rows_cnt = cursor.fetchall()
                if rows_cnt:
                    cols_cnt = [col[0] for col in cursor.description]
                    df_cnt_batch = pd.DataFrame.from_records(rows_cnt, columns=cols_cnt)
                    cnt_dfs.append(df_cnt_batch)
                    print(f"Rowcount rows in this batch: {len(df_cnt_batch)}")
                else:
                    print("Rowcount query (batch) returned no rows.")
            except pymssql.ProgrammingError:
                print("Rowcount query (batch) did not return a resultset.")

        # Una vez recorridos todos los batches, concatenamos
        if pk_dfs:
            df_pk_all = pd.concat(pk_dfs, ignore_index=True)
        else:
            df_pk_all = pd.DataFrame(columns=["Database", "Schema", "Table", "column_id", "is_pk"])

        if cnt_dfs:
            df_cnt_all = pd.concat(cnt_dfs, ignore_index=True)
        else:
            df_cnt_all = pd.DataFrame(columns=["Database", "Schema", "Table", "total_rows"])

        print("Global PK DF:")
        print(df_pk_all.head())
        print(f"Global PK rows: {len(df_pk_all)}")

        print("Global COUNTS DF:")
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

        print("Final merged result (PK + total_rows):")
        print(df_final.head())
        print(f"Total merged rows: {len(df_final)}")

        # Opcional: XCom
        # ti.xcom_push(key="pk_with_rowcounts", value=df_final.to_dict(orient="records"))

    finally:
        if conn is not None:
            conn.close()


# === Definición del DAG ===
with DAG(
    dag_id="bq_to_sqlserver_pk_catalog_pymssql_batches_v1",
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
