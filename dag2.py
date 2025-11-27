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


# === TASK 2: Ejecutar dos queries en SQL Server (PKs + rowcounts) y hacer join ===
def build_and_run_sqlserver_query(**context):
    """
    1) Pulls metadata from XCom.
    2) Construye dinámicamente el VALUES(...) de todo_raw
       a partir de (database_name, schema_name, table_name).
    3) Ejecuta:
       - Query PK: devuelve [Database, Schema, Table, column_id, is_pk].
       - Query ROWCOUNT: devuelve [Database, Schema, Table, total_rows].
    4) Hace un merge en Pandas y muestra el resultado combinado.
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

    # Construimos VALUES (...) para todo_raw en base al DataFrame
    values_rows = []
    for _, row in df_meta.iterrows():
        db  = str(row["database_name"]).strip()
        sch = str(row["schema_name"]).strip()
        tbl = str(row["table_name"]).strip()

        # Escapar comillas simples para T-SQL Unicode N'...'
        db_escaped  = db.replace("'", "''")
        sch_escaped = sch.replace("'", "''")
        tbl_escaped = tbl.replace("'", "''")

        values_rows.append(
            f"(N'{db_escaped}', N'{sch_escaped}', N'{tbl_escaped}')"
        )

    if not values_rows:
        print("No rows to build VALUES clause from metadata.")
        return

    values_clause = ",\n        ".join(values_rows)

    # === Query 1: PK catalog (Database, Schema, Table, column_id, is_pk) ===
    pk_tsql = f"""
/* === 1) Lista objetivo (Database, Schema, Table) desde BigQuery === */
WITH todo_raw (DatabaseName, SchemaName, TableName) AS (
    SELECT *
    FROM (VALUES
        {values_clause}
    ) v(DatabaseName, SchemaName, TableName)
)
SELECT
    DatabaseName = LTRIM(RTRIM(DatabaseName)),
    SchemaName   = LTRIM(RTRIM(SchemaName)),
    TableName    = LTRIM(RTRIM(TableName))
INTO #todo
FROM todo_raw;

WITH t AS (
    SELECT DISTINCT DatabaseName, SchemaName, TableName
    FROM #todo
)
DELETE FROM #todo;

INSERT INTO #todo (DatabaseName, SchemaName, TableName)
SELECT DatabaseName, SchemaName, TableName
FROM t;

DECLARE @sql NVARCHAR(MAX) = N'';

SELECT
    @sql = STRING_AGG(BlockSql, CHAR(10) + 'UNION ALL' + CHAR(10))
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
    RAISERROR('No databases found in #todo for PK query.', 16, 1);
    RETURN;
END;

SET @sql = @sql + CHAR(10) + 'ORDER BY [Database], [Schema], [Table], column_id;';

EXEC sys.sp_executesql @sql;

DROP TABLE #todo;
"""

    # === Query 2: Rowcount por tabla (Database, Schema, Table, total_rows) ===
    counts_tsql = f"""
/* === 1) Lista objetivo (Database, Schema, Table) desde BigQuery === */
WITH todo_raw (DatabaseName, SchemaName, TableName) AS (
    SELECT *
    FROM (VALUES
        {values_clause}
    ) v(DatabaseName, SchemaName, TableName)
)
SELECT
    DatabaseName = LTRIM(RTRIM(DatabaseName)),
    SchemaName   = LTRIM(RTRIM(SchemaName)),
    TableName    = LTRIM(RTRIM(TableName))
INTO #todo
FROM todo_raw;

WITH t AS (
    SELECT DISTINCT DatabaseName, SchemaName, TableName
    FROM #todo
)
DELETE FROM #todo;

INSERT INTO #todo (DatabaseName, SchemaName, TableName)
SELECT DatabaseName, SchemaName, TableName
FROM t;

DECLARE @sql2 NVARCHAR(MAX) = N'';

SELECT
    @sql2 = STRING_AGG(BlockSql, CHAR(10) + 'UNION ALL' + CHAR(10))
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
    RAISERROR('No databases found in #todo for rowcount query.', 16, 1);
    RETURN;
END;

SET @sql2 = @sql2 + CHAR(10) + 'ORDER BY [Database], [Schema], [Table];';

EXEC sys.sp_executesql @sql2;

DROP TABLE #todo;
"""

    print("PK T-SQL to be executed on SQL Server:")
    print(pk_tsql)
    print("COUNTS T-SQL to be executed on SQL Server:")
    print(counts_tsql)

    # 4) Ejecutar en SQL Server con pymssql
    conn = None
    try:
        conn = pymssql.connect(
            server="fbtdw2090.qaamer.qacorp.xpo.com",
            user="svcGCPDataEngg",
            password="OXZ6q67wr77k",
            database="XpoMaster",  # DB inicial; las queries usan [DatabaseName].sys.*
        )
        cursor = conn.cursor()

        # --- Query 1: PKs ---
        cursor.execute(pk_tsql)
        try:
            rows_pk = cursor.fetchall()
            if rows_pk:
                cols_pk = [col[0] for col in cursor.description]
                df_pk = pd.DataFrame.from_records(rows_pk, columns=cols_pk)
                print("PK catalog result:")
                print(df_pk.head())
                print(f"Total PK rows: {len(df_pk)}")
            else:
                print("PK query returned no rows.")
                df_pk = pd.DataFrame(columns=["Database", "Schema", "Table", "column_id", "is_pk"])
        except pymssql.ProgrammingError:
            print("PK query did not return a resultset.")
            df_pk = pd.DataFrame(columns=["Database", "Schema", "Table", "column_id", "is_pk"])

        # --- Query 2: Rowcounts ---
        cursor.execute(counts_tsql)
        try:
            rows_cnt = cursor.fetchall()
            if rows_cnt:
                cols_cnt = [col[0] for col in cursor.description]
                df_cnt = pd.DataFrame.from_records(rows_cnt, columns=cols_cnt)
                print("Rowcount result:")
                print(df_cnt.head())
                print(f"Total rowcount rows: {len(df_cnt)}")
            else:
                print("Rowcount query returned no rows.")
                df_cnt = pd.DataFrame(columns=["Database", "Schema", "Table", "total_rows"])
        except pymssql.ProgrammingError:
            print("Rowcount query did not return a resultset.")
            df_cnt = pd.DataFrame(columns=["Database", "Schema", "Table", "total_rows"])

        # --- Join en Pandas: PK + total_rows ---
        if not df_pk.empty and not df_cnt.empty:
            df_final = df_pk.merge(
                df_cnt,
                on=["Database", "Schema", "Table"],
                how="left",
            )
        else:
            df_final = df_pk.copy()
            if "total_rows" not in df_final.columns:
                df_final["total_rows"] = pd.NA

        print("Final merged result (PK + total_rows):")
        print(df_final.head())
        print(f"Total merged rows: {len(df_final)}")

        # Si quieres, puedes empujar este df_final a XCom:
        # ti.xcom_push(key="pk_with_rowcounts", value=df_final.to_dict(orient="records"))

    finally:
        if conn is not None:
            conn.close()


# === Definición del DAG ===
with DAG(
    dag_id="bq_to_sqlserver_pk_catalog_pymssql_two_queries_with_join_v1",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["bigquery", "metadata", "sqlserver", "pk_catalog", "rowcount", "dynamic"],
) as dag:

    extract_metadata_task = PythonOperator(
        task_id="extract_and_process_sqlserver_metadata",
        python_callable=extract_and_process_metadata,
        provide_context=True,
    )

    run_sqlserver_pk_task = PythonOperator(
        task_id="build_and_run_sqlserver_pk_query_dynamic",
        python_callable=build_and_run_sqlserver_query,
        provide_context=True,
    )

    extract_metadata_task >> run_sqlserver_pk_task
