# dags/sql_to_bq_extraction_xpomaster_dbo_carrier_ct.py
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator, get_current_context, ShortCircuitOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import bigquery
import datetime, logging, yaml, re, io, json
import pendulum
import csv
import io
from functools import lru_cache


# === CONFIG LOADER ===
from include.utils.pipe_config_loader import PipeConfigLoader

# === Add all the details below in lowercase ===
source_type = "sqlserver"
database_name = "xpomaster"
schema_name = ["dbo", "carrier"]
type_of_data = "extraction"
table_type = f"{source_type}_objects"
enabled_flag = True
extraction_type = "change_tracking"
pool="pool1"

config_loader = PipeConfigLoader()
env_config = config_loader.load_configurations(table_type)

# === Common env / params ===
project_id = env_config["project_id"]
dataops_dataset = env_config["dataops_dataset"]
object_extraction_metadata_table = env_config["object_extraction_metadata_table_name"]
region = env_config["region"]
composer_gcs_bucket = env_config["composer_gcs_bucket"]

# Templates
template_sql_to_gcs = env_config["template_gcs_to_bq_refactored_ct"]
template_worker_sdk_container_image = env_config["template_gcs_to_bq_refactored_ct_image"]
template_sql_to_bq_metadata_comparison = env_config["template_sql_to_bq_metadata_comparison"]
template_sql_to_bq_metadata_comparison_image = env_config["template_sql_to_bq_metadata_comparison_image"]


# Dataflow env
dataflow_service_account = env_config["dataflow_service_account"]
dataflow_subnetwork = env_config["subnetwork"]
dataflow_temp_location = env_config["dataflow_temp_location"]
dataflow_staging_location = env_config["dataflow_staging_location"]
ip_configuration = env_config["ip_configuration"]

# YAML location (strip existing extension then add our own suffix + .yaml)
_yaml_base = re.sub(r"(?i)\.ya?ml$", "", (env_config["analytical_tables_metadata_yaml_output_path"] or "").lstrip("/"))
if schema_name:
    if len(schema_name) == 1:
        schema_name_stripped = str(schema_name[0]).lower()
        yaml_uri = f"gs://{composer_gcs_bucket}/{_yaml_base}_{database_name.lower()}_{schema_name_stripped}_{extraction_type}.yaml".lower()
    else:
        _schema_suffix = "_".join(str(s).lower() for s in schema_name) if isinstance(schema_name, list) else str(schema_name).lower()
        yaml_uri = f"gs://{composer_gcs_bucket}/{_yaml_base}_{database_name.lower()}_{_schema_suffix}_{extraction_type}.yaml".lower()
else:
    yaml_uri = f"gs://{composer_gcs_bucket}/{_yaml_base}_{database_name.lower()}_{extraction_type}.yaml".lower()

# Silver/Bronze defaults
silver_layer_file_format = env_config.get("silver_layer_file_format", "parquet")
silver_layer_table_format = env_config.get("silver_layer_table_format", "iceberg")
silver_layer_iceberg_bq_connection = env_config.get("silver_layer_iceberg_bq_connection", "us-central1.iceberg-connection")
silver_layer_iceberg_bucket_path = env_config.get("silver_layer_iceberg_bucket_path")
silver_layer_bq_dataset = env_config.get("silver_layer_bq_dataset", "sqlserver_to_bq_silver")
bronze_layer_bq_dataset = env_config.get("bronze_layer_bq_dataset", "sqlserver_to_bq_bronze")
bq_information_schema_tables = env_config.get("bq_information_schema_tables", "INFORMATION_SCHEMA.TABLES")

# === Lookup relationships (FK → PK) ===
# FQN estable usando el mismo proyecto y dataset de dataops_admin que ya trae env_config
LOOKUP_TABLE_RELATIONSHIPS_FQN = f"{project_id}.{dataops_dataset}.lookup_table_relationships"

# ===== Helpers to compute stable IDs (identical in Python + templated operators) =====
def _compute_run_id_from_ctx(ctx) -> str:
    ds = ctx["ds_nodash"]
    ts = ctx["ts_nodash"][9:15]  # HHMMSS
    return f"sqlserver_to_bigquery_ingestion_{ds}_t{ts}"

def _compute_pipeline_id_from_ctx(ctx) -> str:
    ds = ctx["ds_nodash"]
    ts = ctx["ts_nodash"][9:15]
    base = "sql_to_bq_extraction"
    pid = f"{base}-{ds}-t{ts}"
    return pid[:63]

def _env_from_project(pid: str) -> str:
    return (pid.rsplit('-', 1)[-1] or '').lower()

# Per-table suffix and child-ID helpers
def _table_suffix(tbl: dict) -> str:
    return f"{tbl['database']}_{tbl['schema']}_{tbl['table']}".lower()

def _child_ids_from_ctx(ctx, tbl: dict) -> tuple[str, str]:
    """Returns (pipeline_id, child_run_id)"""
    return _compute_pipeline_id_from_ctx(ctx), f"{_compute_run_id_from_ctx(ctx)}-{_table_suffix(tbl)}"


pipeline_run_id_template = "{{ 'sqlserver_to_bigquery_ingestion_' ~ ds_nodash ~ '_t' ~ ts_nodash[9:15] }}"
pipeline_id_template     = "{{ ('sql_to_bq_extraction' ~ '-' ~ ds_nodash ~ '-t' ~ ts_nodash[9:15])[:63] }}"

# Audit tables
audit_run_table_name = env_config["audit_run_table"]
ct_version_sync_table_name = env_config["ct_version_sync_table"]
audit_table = f"{project_id}.{dataops_dataset}.{audit_run_table_name}"
ct_sync_table = f"{project_id}.{dataops_dataset}.{ct_version_sync_table_name}"


FINAL_SUCCESS_STATUS = "success_ct"

DEFAULT_ARGS = {
    "owner": "dataeng-datalake",
    "start_date": pendulum.now("America/Chicago").subtract(days=1),
}



CST = pendulum.timezone("America/Chicago")

def _should_run_now() -> bool:
    return True

# def _should_run_now() -> bool:
#     now_local = pendulum.now(CST)
#     is_weekday = now_local.day_of_week in (0, 1, 2, 3, 4)
#     t = now_local.time()  # pendulum.Time
#     in_blackout = (pendulum.time(6, 30) <= t < pendulum.time(11, 0))
#     return not (is_weekday and in_blackout)

logging.basicConfig(level=logging.INFO)

POOL = "default_pool"
WEIGHTS = {
    "extraction": 100, "gcs_copy": 90, "external_table": 80, "iceberg": 70,
    "metadata_compare": 60, "yaml": 50, "gate": 10, "misc": 5
}
REQUIRED_KEYS = {
    "database", "schema", "table", "output_gcs_path", "gcs_external_table_path",
    "output_bigquery_bronze_table", "secret_id"
}

def _shorten(name: str, keep: int = 63) -> str:
    return name if len(name) <= keep else name[:keep]

def _to_snake_case_preserve_id(name: str) -> str:
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    s2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1)
    return s2.replace("__", "_").lower()

def _split_and_clean_csv(value: str) -> list[str]:
    """
    Robust CSV splitter using Python's built-in csv module.

    - Correctly handles quoted fields, embedded commas, and escaped quotes.
    - Trims surrounding whitespace from each field.
    - Drops empty fields.
    - Preserves the original casing (important for SELECT clauses).
    - Deduplicates while preserving the original order (via dict.fromkeys).

    This is both cleaner and safer than manual regex splitting.
    """
    if not value or not str(value).strip():
        return []

    # Parse a single CSV row from the input string
    row = next(csv.reader(io.StringIO(str(value)), skipinitialspace=True), [])

    # Strip whitespace and drop empties
    cleaned = [f.strip() for f in row if f is not None and f.strip()]

    # Deduplicate while preserving order
    return list(dict.fromkeys(cleaned))


@lru_cache(maxsize=4096)
def _alias_from_parent(table_pk: str, parent_col_name: str) -> str:
    """
    Builds a standardized alias for flattened lookup columns.

    Alias pattern: <table_pk>__<col_in_snake_case> (all lowercase)

    Parameters:
    - table_pk: Parent table name as it appears in the lookup table (e.g., 'AccessControlType').
    - parent_col_name: Actual column name in the parent table (used in SELECT).

    Returns:
    - Lowercase alias combining the parent table prefix and the snake_case column name.
    """
    # Convert parent table and column names to snake_case
    tbl_prefix = _to_snake_case_preserve_id(table_pk).lower()
    col_snake = _to_snake_case_preserve_id(parent_col_name).lower()

    # Example: 'access_control_type__access_control_name'
    return f"{tbl_prefix}_{col_snake}"



def _sanitize_job_name(*parts: str) -> str:
    raw = "-".join(filter(None, [str(p) for p in parts]))
    s = re.sub(r"[^a-z0-9-]+", "-", raw.lower())
    s = re.sub(r"-+", "-", s).strip("-")
    return s[:63] if len(s) > 63 else s

def _gcs_split(uri: str):
    assert uri.startswith("gs://"), f"Not a GCS URI: {uri}"
    path = uri[5:]
    bucket, _, object_name = path.partition("/")
    assert bucket and object_name, f"Incomplete GCS URI: {uri}"
    return bucket, object_name

def _resolve_datasets(tbl: dict):
    bronze_ds = tbl.get("bronze_layer_bq_dataset", bronze_layer_bq_dataset)
    silver_ds = tbl.get("silver_layer_bq_dataset", silver_layer_bq_dataset)
    return bronze_ds, silver_ds

def _read_text_from_gcs_safe(gcs_uri: str, gcp_conn_id: str = "google_cloud_default") -> str | None:
    try:
        if not gcs_uri or not gcs_uri.startswith("gs://"):
            logging.warning("YAML URI not gs:// — skipping: %s", gcs_uri)
            return None
        bucket, object_name = _gcs_split(gcs_uri)
        hook = GCSHook(gcp_conn_id=gcp_conn_id)
        return hook.download(bucket_name=bucket, object_name=object_name).decode("utf-8")
    except Exception as e:
        logging.warning("YAML not available at %s (%s). Proceeding with no tables.", gcs_uri, e)
        return None

def _should_process_table(
    reload_flag: bool,
    has_change_tracking: bool,
    sqlserver_row_count: int,
    bigquery_row_count: int,
    sqlserver_ct_version: int,
    bigquery_ct_version: int,
) -> bool:
    if reload_flag:
        return True
    if has_change_tracking:
        return sqlserver_ct_version > bigquery_ct_version
    return False

def _load_tables_from_yaml_safe(yaml_uri: str) -> list[dict]:
    raw = _read_text_from_gcs_safe(yaml_uri)
    if not raw:
        return []
    try:
        doc = yaml.safe_load(io.StringIO(raw)) or {}
        tables = doc.get("tables", doc if isinstance(doc, list) else [])
        if not isinstance(tables, list):
            logging.warning("YAML root not a list or 'tables' array. Using empty list.")
            return []
        normalized: list[dict] = []
        for t in tables:
            if not isinstance(t, dict):
                continue
            missing = REQUIRED_KEYS - set(t.keys())
            if missing:
                logging.warning("Skipping table entry missing keys %s: %s", sorted(missing), t)
                continue
            tbl = dict(t)
            for k in ("database","schema","table"):
                tbl[k] = str(tbl[k]).strip().lower()
            tbl["primary_keys"] = list(tbl.get("primary_keys", []))
            tbl["type_of_extraction"] = (tbl.get("type_of_extraction") or "full").lower()
            tbl["reload_flag"] = bool(tbl.get("reload_flag", False))
            tbl["allow_no_pk_full_reload"] = bool(tbl.get("allow_no_pk_full_reload", True))
            tbl["no_pk_row_limit"] = int(tbl.get("no_pk_row_limit", 20_000_000))
            for k in ("incremental_column","query","olap_database","archive_database",
                      "qa_output_gcs_base","qa_table","autoscaling_algorithm","machine_type"):
                v = tbl.get(k, "")
                tbl[k] = v.strip() if isinstance(v, str) else (v or "")
            tbl["qa_enabled"] = bool(tbl.get("qa_enabled", False))
            tbl["qa_allow_count_diff"] = int(tbl.get("qa_allow_count_diff", 0))
            tbl["bronze_layer_bq_dataset"] = tbl.get("bronze_layer_bq_dataset", bronze_layer_bq_dataset)
            tbl["silver_layer_bq_dataset"] = tbl.get("silver_layer_bq_dataset", silver_layer_bq_dataset)
            normalized.append(tbl)
        return normalized
    except Exception:
        logging.exception("Failed to parse YAML at %s; proceeding with no tables.", yaml_uri)
        return []
    
def _fetch_lookup_relationships(server_fk: str, database_fk: str, schema_fk: str, table_fk: str) -> list[dict]:
    """

    Shows the exact query sent to BigQuery. Do NOT use this in production.
    """

    bq = BigQueryHook().get_client(project_id=project_id, location=region)

    server_val = (server_fk or "").lower()
    db_val = (database_fk or "").lower()
    sch_val = (schema_fk or "").lower()
    tbl_val = (table_fk or "").lower()

    # Build WHERE conditions without parameters
    if server_val == "":
        server_filter = "TRUE"
    else:
        server_filter = f"""
            server_fk IS NULL
            OR server_fk = ''
            OR LOWER(server_fk) = '{server_val}'
        """

    sql = f"""
      SELECT
        column_fk,
        LOWER(database_pk)  AS database_pk,
        LOWER(schema_pk)    AS schema_pk,
        LOWER(table_pk)     AS table_pk,
        pk_name_id,
        data_to_add_into_pipeline
      FROM `{LOOKUP_TABLE_RELATIONSHIPS_FQN}`
      WHERE LOWER(database_fk) = '{db_val}'
        AND LOWER(schema_fk)   = '{sch_val}'
        AND LOWER(table_fk)    = '{tbl_val}'
        AND (
              {server_filter}
        )
    """
    rows = bq.query(sql).result()

    out: list[dict] = []
    for r in rows:
        out.append({
            "column_fk": (r.get("column_fk") or "").strip(),
            "database_pk": (r.get("database_pk") or "").strip(),
            "schema_pk": (r.get("schema_pk") or "").strip(),
            "table_pk": (r.get("table_pk") or "").strip(),
            "pk_name_id": (r.get("pk_name_id") or "").strip(),
            "data_to_add_into_pipeline": (r.get("data_to_add_into_pipeline") or "").strip(),
        })

    return out



def _get_lookup_relationships_for_child(
    server: str,
    database: str,
    schema: str,
    table: str,
    project_id: str | None = None,      # kept for interface compatibility
    dataops_dataset: str | None = None, # not used (we rely on global FQN)
    location: str | None = None,        # not used (we use global region)
) -> list[dict]:
    """
    Thin wrapper used by _prepare_generate_execute_iceberg_sql() to obtain
    FK→PK relationships for a given child table.

    It simply normalizes inputs and delegates to _fetch_lookup_relationships.
    Extra parameters are accepted for future flexibility but are not used.
    """
    return _fetch_lookup_relationships(
        server_fk=(server or "").lower(),
        database_fk=(database or "").lower(),
        schema_fk=(schema or "").lower(),
        table_fk=(table or "").lower(),
    )


def _generate_and_run_external_table_sql(table, project_id, region, dataops_dataset):
    bq_hook = BigQueryHook()
    client = bq_hook.get_client(project_id=project_id, location=region)
    source_information_schema_columns = f"{project_id}.{dataops_dataset}.{table['database']}_information_schema_columns".lower()
    query = f"""
        SELECT column_name
        FROM `{source_information_schema_columns}`
        WHERE LOWER(table_name) = LOWER('{table['table']}')
          AND LOWER(table_schema) = LOWER('{table['schema']}')
          AND LOWER(table_catalog) = LOWER('{table['database']}')
        ORDER BY ordinal_position
    """
    results = client.query(query).result()
    schema_fields = [f"{_to_snake_case_preserve_id(r['column_name'])} STRING" for r in results]

    manual_columns = [
        "sys_change_version STRING",
        "sys_change_operation STRING",
        "ingestion_date_utc STRING",
        "business_status STRING",
        "row_hash STRING",
        "run_id STRING",
        "rn STRING",
        "dag_id STRING",
        "job_id STRING",
    ]
    schema_fields.extend(manual_columns)
    schema_sql = ",\n  ".join(schema_fields)

    sql = f"""
    CREATE OR REPLACE EXTERNAL TABLE `{table['output_bigquery_bronze_table']}` (
      {schema_sql}
    )
    OPTIONS (
      format = 'PARQUET',
      uris = ['{table["gcs_external_table_path"]}']
    );
    """
    logging.info(sql)
    client.query(sql, location=region).result()

def _build_iceberg_create_sql(project_id, dataset, database_name, schema_name, table_name,
                              primary_keys, column_cast, *, file_format: str, table_format: str,
                              connection: str, storage_bucket_path: str):
    column_definitions = []
    for col in column_cast:
        line = f"  {_to_snake_case_preserve_id(col['name'])} {col['bq_datatype']}"
        if col["is_nullable"] == "NO":
            line += " NOT NULL"
        column_definitions.append(line)
    column_def_str = ",\n".join(column_definitions)
    cluster_clause = ""
    if primary_keys:
        cluster_cols = ", ".join([_to_snake_case_preserve_id(pk) for pk in primary_keys])
        cluster_clause = f"\n    CLUSTER BY {cluster_cols}"
    return f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset}.{database_name}_{schema_name}_{table_name}` (
    {column_def_str}
    ){cluster_clause}
    WITH CONNECTION `{connection}`
    OPTIONS (
      file_format = '{file_format}',
      table_format = '{table_format}',
      storage_uri = '{storage_bucket_path}/{database_name}/{schema_name}/{table_name}/'
    );
    """

def _dedup_source_sql(project_id: str, bronze_dataset: str,
                      database_name: str, schema_name: str, table_name: str,
                      primary_keys: list[str]) -> str:
    if not primary_keys:
        raise RuntimeError(
            f"No primary keys configured for {database_name}.{schema_name}.{table_name}; MERGE would be invalid."
        )
    pk_cols = [_to_snake_case_preserve_id(pk) for pk in primary_keys]
    where_nonempty = " AND ".join([f"NULLIF(TRIM(S.{c}), '') IS NOT NULL" for c in pk_cols])
    partition_by   = ", ".join([f"S.{c}" for c in pk_cols])
    biz_rank = (
        "CASE LOWER(CAST(NULLIF(TRIM(S.business_status), '') AS STRING)) "
        "WHEN 'active' THEN 3 WHEN 'completed' THEN 2 WHEN 'archived' THEN 1 ELSE 0 END"
    )
    order_by = (
        "SAFE_CAST(NULLIF(TRIM(S.sys_change_version), '') AS INT64) DESC, "
        f"{biz_rank} DESC, "
        "S.run_id DESC"
    )
    source_fqn = f"`{project_id}.{bronze_dataset}.{database_name}_{schema_name}_{table_name}`"
    return f"""
      SELECT S.*
      FROM {source_fqn} AS S
      WHERE {where_nonempty}
      QUALIFY ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY {order_by}) = 1
    """

def _build_iceberg_merge_sql(project_id, dataset, database_name, schema_name, table_name,
                             primary_keys, column_cast, cast_expressions, *, bronze_dataset: str):
    if not primary_keys:
        raise RuntimeError(
            f"No primary keys configured for {database_name}.{schema_name}.{table_name}; MERGE would be invalid."
        )
    match_condition = " AND ".join([
        f"CAST(T.{_to_snake_case_preserve_id(pk)} AS STRING) = S.{_to_snake_case_preserve_id(pk)}"
        for pk in primary_keys
    ])

    insert_columns_list = [_to_snake_case_preserve_id(col["name"]) for col in column_cast]
    insert_values = ", ".join(cast_expressions)
    update_assignments = [
        f"T.{col} = {expr}"
        for col, expr in zip(
            [_to_snake_case_preserve_id(col["name"]) for col in column_cast],
            cast_expressions
        )
    ]
    update_clause = ",\n        ".join(update_assignments)

    dedup_source_sql = _dedup_source_sql(
        project_id=project_id,
        bronze_dataset=bronze_dataset,
        database_name=database_name,
        schema_name=schema_name,
        table_name=table_name,
        primary_keys=primary_keys,
    )

    return f"""
    MERGE INTO `{project_id}.{dataset}.{database_name}_{schema_name}_{table_name}` T
    USING ({dedup_source_sql}) S
    ON {match_condition}

    WHEN MATCHED AND S.sys_change_operation = 'D' THEN
      DELETE

    WHEN MATCHED AND S.sys_change_operation in ('I','U') THEN
      UPDATE SET
        {update_clause}

    WHEN NOT MATCHED AND S.sys_change_operation in ('I','U') THEN
      INSERT ({", ".join(insert_columns_list)})
      VALUES ({insert_values})
    """

def _prepare_generate_execute_iceberg_sql(
    table: dict,
    dataflow_job_name: str,
    run_id: str,
    dag_id: str,
) -> None:
    """
    Build and execute the CREATE (if needed) and MERGE statements for the Silver Iceberg table.

    Key points (minimal change philosophy):
    - Keep the original child-column typing/casting logic.
    - Add lookup-based enrichment via a SINGLE LEFT JOIN per parent table
      inside the MERGE's USING subquery (no per-row scalar subqueries).
    - Enriched columns are STRING, nullable, and aliased as
      "<parent_table>__<parent_col_snake>" in lowercase to avoid collisions.
    - Do NOT modify the existing deduplication logic; we wrap it and join parents.

    Parameters
    ----------
    table : dict
        Current table configuration (child table).
    dataflow_job_name : str
        Dataflow job name (for audit columns).
    run_id : str
        Child run_id used for stamping.
    dag_id : str
        Airflow DAG id (for audit columns).
    """
    client = bigquery.Client(project=project_id)

    # --- 1) Read child's information_schema to build native column metadata and casts (as before) ---
    source_info_schema = (
        f"{project_id}.{dataops_dataset}."
        f"{table['database']}_information_schema_columns"
    ).lower()

    meta_sql = f"""
      SELECT
        column_name,
        data_type,
        character_maximum_length,
        numeric_precision,
        numeric_scale,
        is_nullable
      FROM `{source_info_schema}`
      WHERE LOWER(table_name)   = LOWER(@tbl)
        AND LOWER(table_schema) = LOWER(@sch)
        AND LOWER(table_catalog)= LOWER(@db)
    """

    job_cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("tbl", "STRING", table["table"]),
            bigquery.ScalarQueryParameter("sch", "STRING", table["schema"]),
            bigquery.ScalarQueryParameter("db", "STRING", table["database"]),
        ]
    )
    rows = client.query(meta_sql, job_config=job_cfg).result()

    column_metadata: list[dict] = []
    cast_expressions: list[str] = []

    # Default values for NOT NULL columns when parsing fails.
    not_null_defaults = {
        "STRING": "''",
        "INT64": "0",
        "NUMERIC": "0",
        "FLOAT64": "0.0",
        "BOOL": "FALSE",
        "DATE": "DATE '1970-01-01'",
        "TIMESTAMP": "TIMESTAMP '1970-01-01 00:00:00+00'",
    }

    # Build typing/casting for native child columns (unchanged behavior).
    for r in rows:
        column_name = r["column_name"]
        data_type = (r["data_type"] or "").lower()
        is_nullable = r["is_nullable"] or "YES"

        snake_col = _to_snake_case_preserve_id(column_name).lower()

        if data_type in {"varchar", "nvarchar", "char", "nchar", "text"}:
            bq_type = "STRING"
        elif data_type in {"int", "bigint", "smallint", "tinyint"}:
            bq_type = "INT64"
        elif data_type in {"decimal", "numeric"}:
            bq_type = "NUMERIC"
        elif data_type in {"float", "real"}:
            bq_type = "FLOAT64"
        elif data_type == "bit":
            bq_type = "BOOL"
        elif data_type in {"datetime", "datetime2", "smalldatetime", "datetimeoffset"}:
            bq_type = "TIMESTAMP"
        elif data_type == "date":
            bq_type = "DATE"
        else:
            bq_type = "STRING"

        column_metadata.append(
            {"name": snake_col, "bq_datatype": bq_type, "is_nullable": is_nullable}
        )

        # Safe parse expression from the source alias S.<col>.
        base = f"NULLIF(TRIM(S.{snake_col}), '')"
        if bq_type == "DATE":
            inner = f"SAFE.PARSE_DATE('%F', {base})"
        elif bq_type == "TIMESTAMP":
            inner = f"SAFE.PARSE_TIMESTAMP('%F %H:%M:%E*S', {base})"
        elif bq_type == "BOOL":
            inner = (
                "CASE WHEN LOWER(CAST({0} AS STRING)) IN ('1','true') THEN TRUE "
                "WHEN LOWER(CAST({0} AS STRING)) IN ('0','false') THEN FALSE "
                "ELSE NULL END"
            ).format(base)
        elif bq_type in {"INT64", "NUMERIC", "FLOAT64"}:
            inner = f"SAFE_CAST({base} AS {bq_type})"
        else:
            inner = f"CAST({base} AS STRING)"

        expr = (
            f"COALESCE({inner}, {not_null_defaults[bq_type]})"
            if (is_nullable or "").upper() == "NO"
            else inner
        )
        cast_expressions.append(expr)

    # Stamp + technical columns for Silver (unchanged).
    column_metadata.extend(
        [
            {"name": "sys_change_version", "bq_datatype": "INT64", "is_nullable": "YES"},
            {"name": "bq_insert_datetime", "bq_datatype": "TIMESTAMP", "is_nullable": "NO"},
            {"name": "bq_update_datetime", "bq_datatype": "TIMESTAMP", "is_nullable": "YES"},
            {"name": "dag_id", "bq_datatype": "STRING", "is_nullable": "NO"},
            {"name": "job_id", "bq_datatype": "STRING", "is_nullable": "NO"},
            {"name": "business_status", "bq_datatype": "STRING", "is_nullable": "YES"},
            {"name": "row_hash", "bq_datatype": "STRING", "is_nullable": "YES"},
            {"name": "run_id", "bq_datatype": "STRING", "is_nullable": "NO"},
        ]
    )
    cast_expressions.extend(
        [
            "CAST(S.sys_change_version AS INT64)",
            "CURRENT_TIMESTAMP()",
            "CURRENT_TIMESTAMP()",
            f"'{dag_id}'",
            f"'{dataflow_job_name}'",
            "CAST(S.business_status AS STRING)",
            "CAST(S.row_hash AS STRING)",
            f"COALESCE(S.run_id, '{run_id}')",
        ]
    )
    # --- 2) Build deduplicated SOURCE for the child (unchanged) ---
    bronze_ds, silver_ds = _resolve_datasets(table)
    dedup_sql = _dedup_source_sql(
        project_id=project_id,
        bronze_dataset=bronze_ds,
        database_name=table["database"],
        schema_name=table["schema"],
        table_name=table["table"],
        primary_keys=table["primary_keys"],
    )

    # === lookup_relationships: usar lo que venga del YAML;
    #     if it comes back empty, query BigQuery again ===
    relationships = table.get("lookup_relationships") or []

    if not relationships:
        logging.info(
            "lookup_relationships vacío en YAML para %s.%s.%s, "
            "recuperando directamente desde lookup_table_relationships",
            table["database"], table["schema"], table["table"],
        )
        relationships = _fetch_lookup_relationships(
            server_fk=(table.get("server") or "").lower(),
            database_fk=(table["database"] or "").lower(),
            schema_fk=(table["schema"] or "").lower(),
            table_fk=(table["table"] or "").lower(),
        )

    logging.info(
        "lookup_relationships usado en merge para %s.%s.%s: %s",
        table["database"], table["schema"], table["table"], relationships,
    )

    # Group relationships by the same parent to avoid multiple JOINs per parent.
    # Key: (db_pk, sch_pk, tb_pk, pk_name_id, child_fk)
    from collections import defaultdict

    parents: dict = defaultdict(list)
    for rel in relationships:
        key = (
            (rel.get("database_pk") or "").lower(),
            (rel.get("schema_pk") or "").lower(),
            (rel.get("table_pk") or "").lower(),
            (rel.get("pk_name_id") or "").lower(),   # solo para agrupar
            (rel.get("column_fk") or "").lower(),    # solo para agrupar
        )
        parents[key].append(rel)

    # Build JOIN clauses and SELECT projections for enriched columns.
    join_clauses: list[str] = []
    select_projections: list[str] = []

    # Track used aliases to avoid collisions; append __2, __3... if needed.
    used_enriched_aliases: set[str] = set()

    def _unique_alias(base_alias: str) -> str:
        if base_alias not in used_enriched_aliases:
            used_enriched_aliases.add(base_alias)
            return base_alias
        i = 2
        while True:
            cand = f"{base_alias}__{i}"
            if cand not in used_enriched_aliases:
                used_enriched_aliases.add(cand)
                return cand
            i += 1

    parent_idx = 0
    # NOTE: here the names of the key variables change
    for (db_pk, sch_pk, tb_pk, pk_name_id_key, child_fk_key), rels in parents.items():
        parent_idx += 1
        parent_alias = f"P{parent_idx}"

        # Retrieve the ORIGINAL names of pk_name_id and column_fk
        # (for example 'AccessControlTypeId'), not the lower/flattened version.
        first_rel = rels[0]
        pk_name_original = (first_rel.get("pk_name_id") or "").strip()
        child_fk_original = (first_rel.get("column_fk") or "").strip()

        # FQN for the parent's Bronze table.
        parent_fqn = f"`{project_id}.{bronze_ds}.{db_pk}_{sch_pk}_{tb_pk}`"

        # Join ON <parent_pk> = S.<child_fk> usando snake_case de los nombres originales
        parent_pk_snake = _to_snake_case_preserve_id(pk_name_original).lower()
        child_fk_snake = _to_snake_case_preserve_id(child_fk_original).lower()

        join_clauses.append(
            f"LEFT JOIN {parent_fqn} {parent_alias} "
            f"ON {parent_alias}.{parent_pk_snake} = S.{child_fk_snake}"
        )

        # Collect the set of parent columns to project (union across rels).
        parent_cols_raw: list[str] = []
        for rel in rels:
            cols = _split_and_clean_csv(rel.get("data_to_add_into_pipeline", ""))
            parent_cols_raw.extend(cols)

        # Deduplicate while preserving order.
        seen = set()
        parent_cols_ordered = []
        for c in parent_cols_raw:
            if c not in seen:
                seen.add(c)
                parent_cols_ordered.append(c)

        # Build select projections with stable, collision-free aliases.
        for col_real in parent_cols_ordered:
            col_snake = _to_snake_case_preserve_id(col_real).lower()
            base_alias = _alias_from_parent(tb_pk, col_real)  # already snake_case + lowercase
            alias_final = _unique_alias(base_alias)
            select_projections.append(
                f"{parent_alias}.{col_snake} AS `{alias_final}`"
            )
            # Register the enriched column into Silver schema as STRING, nullable.
            column_metadata.append(
                {"name": alias_final, "bq_datatype": "STRING", "is_nullable": "YES"}
            )
            # Its expression in MERGE is a simple pass-through from the USING subquery.
            cast_expressions.append(f"CAST(S.{alias_final} AS STRING)")


    # --- 4) Build the USING subquery for MERGE: wrap dedup and add LEFT JOINs per parent ---
    # If there are no relationships, the USING subquery is just the dedup SELECT.
    if select_projections:
        select_list = ",\n          ".join(["S.*"] + select_projections)
        joined_sql = f"""
        WITH S AS (
          {dedup_sql}
        )
        SELECT
          {select_list}
        FROM S
        {' '.join(join_clauses)}
        """
    else:
        # No enrichment: minimal path.
        joined_sql = f"""
        WITH S AS (
          {dedup_sql}
        )
        SELECT
          S.*
        FROM S
        """

    # --- 5) CREATE TABLE IF NOT EXISTS (Iceberg) using the complete column metadata ---
    iceberg_table_name = (
        f"{table['database']}_{table['schema']}_{table['table']}"
    ).lower()

    exists_sql = f"""
      SELECT 1
      FROM `{project_id}.{table['silver_layer_bq_dataset']}.{bq_information_schema_tables}`
      WHERE table_name = @tn
      LIMIT 1
    """
    exists_cfg = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("tn", "STRING", iceberg_table_name)]
    )
    exists_rows = list(client.query(exists_sql, job_config=exists_cfg, location=region).result())

    if not exists_rows:
        create_stmt = _build_iceberg_create_sql(
            project_id=project_id,
            dataset=table["silver_layer_bq_dataset"],
            database_name=table["database"],
            schema_name=table["schema"],
            table_name=table["table"],
            primary_keys=table["primary_keys"],
            column_cast=column_metadata,
            file_format=silver_layer_file_format,
            table_format=silver_layer_table_format,
            connection=table.get(
                "silver_layer_iceberg_bq_connection",
                silver_layer_iceberg_bq_connection,
            ),
            storage_bucket_path=table.get(
                "silver_layer_iceberg_bucket_path",
                silver_layer_iceberg_bucket_path,
            ),
        )
        # Execute CREATE first (idempotent replace).
        logging.info(create_stmt)
        client.query(create_stmt, location=region).result()

    # --- 6) Build and execute MERGE using the enriched USING subquery ---
    # Match condition: CAST(T.pk AS STRING) = S.pk (string) for each PK.
    if not table["primary_keys"]:
        raise RuntimeError(
            f"No primary keys configured for "
            f"{table['database']}.{table['schema']}.{table['table']}; MERGE would be invalid."
        )

    match_condition = " AND ".join(
        [
            f"CAST(T.{_to_snake_case_preserve_id(pk).lower()} AS STRING) = "
            f"S.{_to_snake_case_preserve_id(pk).lower()}"
            for pk in table["primary_keys"]
        ]
    )

    # Build column lists for INSERT/UPDATE from the final metadata.
    insert_cols = ", ".join([_to_snake_case_preserve_id(c["name"]).lower() for c in column_metadata])
    insert_vals = ", ".join(cast_expressions)

    update_set = ",\n        ".join(
        [
            f"T.{_to_snake_case_preserve_id(c['name']).lower()} = {expr}"
            for c, expr in zip(column_metadata, cast_expressions)
        ]
    )

    target_fqn = (
        f"`{project_id}.{table['silver_layer_bq_dataset']}.{iceberg_table_name}`"
    )

    merge_sql = f"""
    MERGE INTO {target_fqn} AS T
    USING ({joined_sql}) AS S
    ON {match_condition}

    WHEN MATCHED AND S.sys_change_operation = 'D' THEN
      DELETE

    WHEN MATCHED AND S.sys_change_operation IN ('I','U') THEN
      UPDATE SET
        {update_set}

    WHEN NOT MATCHED AND S.sys_change_operation IN ('I','U') THEN
      INSERT ({insert_cols})
      VALUES ({insert_vals})
    """

    logging.info(merge_sql)
    client.query(merge_sql, location=region).result()

# ===========================
# AUDIT failure callback (insert-only)
# ===========================
from google.cloud import bigquery as _bq_mod
import datetime as _dt_mod
import json as _json_mod
import logging as _logging

def audit_task_failure_callback(context):
    """Airflow task failure callback: append rows into audit table (insert-only)."""
    try:
        ti = context["ti"]
        task = context["task"]
        log_url = getattr(ti, "log_url", "")
        exc = context.get("exception")
        ctx_for_ids = context  # both have ds_nodash/ts_nodash

        base_run_id = _compute_run_id_from_ctx(ctx_for_ids)
        pipeline_id = _compute_pipeline_id_from_ctx(ctx_for_ids)

        # Try to get table suffix from task group id (preferred)
        tg = getattr(task, "task_group", None)
        tbl_suffix = None
        if tg and getattr(tg, "group_id", "").endswith("_pipeline"):
            g = tg.group_id
            tbl_suffix = g[: -len("_pipeline")] if g and len(g) > len("_pipeline") else None

        # Fallback to params (present on Dataflow operator)
        if not tbl_suffix:
            params = (context.get("params") or {})
            tbl_suffix = params.get("tbl_suffix") or tbl_suffix

        # Compute target run_id
        run_id = f"{base_run_id}-{tbl_suffix}" if tbl_suffix else base_run_id

        # Build error payload
        err_obj = {
            "when": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "dag_id": ti.dag_id,
            "task_id": task.task_id,
            "log_url": log_url,
            "try_number": ti.try_number,
            "max_tries": getattr(ti, "max_tries", 0) or 0,
            "exception": f"{type(exc).__name__}: {exc!s}" if exc else "Unknown"
        }

        client = _bq_mod.Client(project=project_id)

        sql1 = f"""
        INSERT INTO `{audit_table}` (run_id, pipeline_id, gcp_project, comments, inserted_at)
        VALUES (@rid, @pid, '{project_id}', @cmt, CURRENT_TIMESTAMP())
        """
        cfg1 = _bq_mod.QueryJobConfig(query_parameters=[
            _bq_mod.ScalarQueryParameter("rid", "STRING", run_id),
            _bq_mod.ScalarQueryParameter("pid", "STRING", pipeline_id),
            _bq_mod.ScalarQueryParameter("cmt", "STRING", f"[DAG] Task FAILED after retries: {task.task_id} | log={log_url}"),
        ])
        client.query(sql1, job_config=cfg1, location=region).result()

        sql2 = f"""
        INSERT INTO `{audit_table}` (run_id, pipeline_id, gcp_project, status, error, run_completed_at, inserted_at)
        VALUES (@rid, @pid, '{project_id}', 'failed_ct', SAFE.PARSE_JSON(@err), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
        """
        cfg2 = _bq_mod.QueryJobConfig(query_parameters=[
            _bq_mod.ScalarQueryParameter("rid","STRING", run_id),
            _bq_mod.ScalarQueryParameter("pid","STRING", pipeline_id),
            _bq_mod.ScalarQueryParameter("err","STRING", _json_mod.dumps(err_obj or {})),
        ])
        client.query(sql2, job_config=cfg2, location=region).result()

    except Exception as e:
        _logging.exception("audit failure callback crashed: %s", e)

# ===========================
# DAG
# ===========================
@dag(
    dag_id="sql_to_bq_extraction_xpomaster_dbo_carrier_ct_v15",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    tags=["change-tracking", "sql-to-bq", "extraction", "ct","xpomaster","orders"],
    max_active_runs=1,
    concurrency=20,
)
def sql_to_bq_extraction_xpomaster_dbo_carrier_ct():

    # ====== AUDIT HELPERS (insert-only) ======
    def _json_or_none(d: dict | None) -> str | None:
        return None if not d else json.dumps(d, ensure_ascii=False)

    def _merge_upsert_start(run_id: str, pipeline_id: str, yaml_uri_val: str,
                            parameters: dict | None, orchestration: dict | None):
        """Create a RUNNING row (append-only)."""
        bq = BigQueryHook().get_client(project_id=project_id, location=region)
        sql = f"""
        INSERT INTO `{audit_table}` (
          run_id, pipeline_id, gcp_project, region, env, yaml_uri,
          parameters, orchestration_info, run_started_at, status, inserted_at
        )
        SELECT
          @run_id, @pipeline_id, @project_id, @region, @env, @yaml_uri,
          SAFE.PARSE_JSON(@parameters_json), SAFE.PARSE_JSON(@orchestration_json),
          CURRENT_TIMESTAMP(), 'running', CURRENT_TIMESTAMP()
        """
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            bigquery.ScalarQueryParameter("pipeline_id", "STRING", pipeline_id),
            bigquery.ScalarQueryParameter("project_id", "STRING", project_id),
            bigquery.ScalarQueryParameter("region", "STRING", region),
            bigquery.ScalarQueryParameter("env", "STRING", _env_from_project(project_id)),
            bigquery.ScalarQueryParameter("yaml_uri", "STRING", yaml_uri_val or ""),
            bigquery.ScalarQueryParameter("parameters_json", "STRING", _json_or_none(parameters)),
            bigquery.ScalarQueryParameter("orchestration_json", "STRING", _json_or_none(orchestration)),
        ])
        bq.query(sql, job_config=cfg).result()

    def _append_comment(run_id: str, pipeline_id: str, comment: str):
        bq = BigQueryHook().get_client(project_id=project_id, location=region)
        sql = f"""
        INSERT INTO `{audit_table}` (run_id, pipeline_id, gcp_project, comments, inserted_at)
        VALUES (@rid, @pid, '{project_id}', @cmt, CURRENT_TIMESTAMP())
        """
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("rid", "STRING", run_id),
            bigquery.ScalarQueryParameter("pid", "STRING", pipeline_id),
            bigquery.ScalarQueryParameter("cmt", "STRING", comment or ""),
        ])
        bq.query(sql, job_config=cfg).result()

    def _stamp_stage(run_id: str, pipeline_id: str, stage_col: str, new_status: str | None = None):
        assert stage_col in {
            "copy_and_cleanup_started_at",
            "copy_and_cleanup_completed_at",
            "bronze_layer_creation_started_at",
            "bronze_layer_creation_completed_at",
            "silver_layer_creation_started_at",
            "silver_layer_creation_completed_at",
        }, f"invalid stage column: {stage_col}"
        bq = BigQueryHook().get_client(project_id=project_id, location=region)

        if new_status:
            sql = f"""
            INSERT INTO `{audit_table}` (run_id, pipeline_id, gcp_project, {stage_col}, status, inserted_at)
            VALUES (@rid, @pid, '{project_id}', CURRENT_TIMESTAMP(), @status, CURRENT_TIMESTAMP())
            """
            params = [
                bigquery.ScalarQueryParameter("rid", "STRING", run_id),
                bigquery.ScalarQueryParameter("pid", "STRING", pipeline_id),
                bigquery.ScalarQueryParameter("status", "STRING", new_status),
            ]
        else:
            sql = f"""
            INSERT INTO `{audit_table}` (run_id, pipeline_id, gcp_project, {stage_col}, inserted_at)
            VALUES (@rid, @pid, '{project_id}', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
            """
            params = [
                bigquery.ScalarQueryParameter("rid", "STRING", run_id),
                bigquery.ScalarQueryParameter("pid", "STRING", pipeline_id),
            ]
        bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()

    def _update_jsons_and_metrics(run_id: str, pipeline_id: str,
                                  source_metrics: dict | None = None,
                                  target_metrics: dict | None = None,
                                  record_counts: dict | None = None,
                                  runtime_metrics: dict | None = None,
                                  files_written: int | None = None,
                                  bytes_written: int | None = None):
        bq = BigQueryHook().get_client(project_id=project_id, location=region)
        sql = f"""
        INSERT INTO `{audit_table}` (
            run_id, pipeline_id, gcp_project,
            source_metrics, target_metrics, record_counts, runtime_metrics,
            total_files_written, total_bytes_written, inserted_at
        )
        VALUES (
            @rid, @pid, '{project_id}',
            SAFE.PARSE_JSON(@sm), SAFE.PARSE_JSON(@tm), SAFE.PARSE_JSON(@rc), SAFE.PARSE_JSON(@rm),
            @fw, @bw, CURRENT_TIMESTAMP()
        )
        """
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("rid", "STRING", run_id),
            bigquery.ScalarQueryParameter("pid", "STRING", pipeline_id),
            bigquery.ScalarQueryParameter("sm", "STRING", json.dumps(source_metrics) if source_metrics else None),
            bigquery.ScalarQueryParameter("tm", "STRING", json.dumps(target_metrics) if target_metrics else None),
            bigquery.ScalarQueryParameter("rc", "STRING", json.dumps(record_counts) if record_counts else None),
            bigquery.ScalarQueryParameter("rm", "STRING", json.dumps(runtime_metrics) if runtime_metrics else None),
            bigquery.ScalarQueryParameter("fw", "INT64", int(files_written) if files_written is not None else None),
            bigquery.ScalarQueryParameter("bw", "INT64", int(bytes_written) if bytes_written is not None else None),
        ])
        bq.query(sql, job_config=cfg).result()

    def _finalize_success(run_id: str, pipeline_id: str, success_status: str):
        bq = BigQueryHook().get_client(project_id=project_id, location=region)
        sql = f"""
        INSERT INTO `{audit_table}`
          (run_id, pipeline_id, gcp_project, status, run_completed_at, total_process_ms, inserted_at)
        VALUES (
          @rid,
          @pid,
          COALESCE(CAST('{project_id}' AS STRING), 'unknown'),         -- never NULL
          @status,
          CURRENT_TIMESTAMP(),
          -- safe duration (NULL if we can't find a start)
          (
            SELECT
              CASE
                WHEN rs.run_started_at IS NULL THEN NULL
                ELSE TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), rs.run_started_at, MILLISECOND)
              END
            FROM (
              SELECT run_started_at
              FROM `{audit_table}`
              WHERE run_id = @rid AND pipeline_id = @pid AND run_started_at IS NOT NULL
              ORDER BY inserted_at DESC
              LIMIT 1
            ) AS rs
          ),
          CURRENT_TIMESTAMP()
        )
        """
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("rid", "STRING", run_id),
            bigquery.ScalarQueryParameter("pid", "STRING", pipeline_id),
            bigquery.ScalarQueryParameter("status", "STRING", success_status),
        ])
        bq.query(sql, job_config=cfg).result()
        _append_comment(run_id, pipeline_id, f"[{success_status}] run completed")

    def _finalize_failure(run_id: str, pipeline_id: str, error_obj: dict):
        bq = BigQueryHook().get_client(project_id=project_id, location=region)
        sql = f"""
        INSERT INTO `{audit_table}` (run_id, pipeline_id, gcp_project, status, error, run_completed_at, total_process_ms, inserted_at)
        SELECT
          @rid, @pid, '{project_id}', 'failed', SAFE.PARSE_JSON(@err), CURRENT_TIMESTAMP(),
          TIMESTAMP_DIFF(
            CURRENT_TIMESTAMP(),
            (SELECT run_started_at
               FROM `{audit_table}`
              WHERE run_id=@rid AND pipeline_id=@pid
                AND run_started_at IS NOT NULL
              ORDER BY inserted_at DESC
              LIMIT 1),
            MILLISECOND
          ),
          CURRENT_TIMESTAMP()
        """
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("rid","STRING", run_id),
            bigquery.ScalarQueryParameter("pid","STRING", pipeline_id),
            bigquery.ScalarQueryParameter("err","STRING", json.dumps(error_obj or {})),
        ])
        bq.query(sql, job_config=cfg).result()
        _append_comment(run_id, pipeline_id, f"[FAILED] {str((error_obj or {}).get('message','')).strip()[:500]}")

    # === NEW: finalize ct_version_sync for the table (SUCCESS_CT) ===
    def finalize_ct_sync_run(tbl: dict):
        bq = BigQueryHook().get_client(project_id=project_id, location=region)
        ctx = get_current_context()
        _, child_run_id = _child_ids_from_ctx(ctx, tbl)

        sql = f"""
        UPDATE `{ct_sync_table}`
           SET status               = 'success_ct',
               last_success_at      = CURRENT_TIMESTAMP(),
               last_success_job_id  = @rid,
               extract_completed_at = COALESCE(extract_completed_at, CURRENT_TIMESTAMP()),
               last_applied_version = COALESCE(to_version, last_applied_version),
               updated_at           = CURRENT_TIMESTAMP()
         WHERE lower(source_server_name)   = @srv
           AND lower(source_database_name) = @db
           AND lower(source_schema_name)   = @sch
           AND lower(source_table_name)    = @tbl
           AND updated_by                  = @upd_by
           AND LOWER(COALESCE(status,''))  = 'running'
        """
        params = [
            bigquery.ScalarQueryParameter("srv", "STRING", (tbl.get("server") or "").lower()),
            bigquery.ScalarQueryParameter("db",  "STRING", (tbl.get("database") or "").lower()),
            bigquery.ScalarQueryParameter("sch", "STRING", (tbl.get("schema") or "").lower()),
            bigquery.ScalarQueryParameter("tbl", "STRING", (tbl.get("table") or "").lower()),
            bigquery.ScalarQueryParameter("rid", "STRING", child_run_id),
            bigquery.ScalarQueryParameter("upd_by", "STRING", child_run_id),
        ]
        bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
        logging.info("CT watermark finalized for %s.%s.%s (updated_by=%s)",
                     tbl.get("database"), tbl.get("schema"), tbl.get("table"), child_run_id)

    # === NEW: count parquet files + total bytes under processed path and insert into audit ===
    def _capture_gcs_file_metrics(tbl: dict):
        gcs = GCSHook()
        dst_uri_root = tbl["gcs_external_table_path"]
        assert dst_uri_root.startswith("gs://"), f"Unexpected GCS URI: {dst_uri_root}"
        bucket = dst_uri_root[5:].split("/", 1)[0]
        prefix = dst_uri_root[5+len(bucket):].lstrip("/")
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        client = gcs.get_conn()
        blobs_iter = client.list_blobs(bucket, prefix=prefix)

        parquet_files = 0
        parquet_bytes = 0
        for b in blobs_iter:
            name = getattr(b, "name", "") or ""
            if not name.lower().endswith(".parquet"):
                continue
            parquet_files += 1
            parquet_bytes += int(getattr(b, "size", 0) or 0)

        ctx = get_current_context()
        pipeline_id, run_id = _child_ids_from_ctx(ctx, tbl)
        _update_jsons_and_metrics(
            run_id=run_id,
            pipeline_id=pipeline_id,
            files_written=parquet_files,
            bytes_written=parquet_bytes,
        )

    # === NEW: helpers to capture silver row counts before/after merge ===
    def _get_silver_row_count(tbl: dict) -> tuple[int, str]:
        bq = BigQueryHook().get_client(project_id=project_id, location=region)
        db = tbl["database"].lower()
        sch = tbl["schema"].lower()
        tb = tbl["table"].lower()
        silver_ds = tbl.get("silver_layer_bq_dataset", silver_layer_bq_dataset)
        iceberg_table = f"{db}_{sch}_{tb}"
        fq = f"{project_id}.{silver_ds}.{iceberg_table}"

        row_count = None
        try:
            isql = f"""
              SELECT row_count
              FROM `{project_id}.{silver_ds}.INFORMATION_SCHEMA.TABLES`
              WHERE table_name = '{iceberg_table}'
              LIMIT 1
            """
            rows = list(bq.query(isql, location=region).result())
            if rows and rows[0]["row_count"] is not None:
                row_count = int(rows[0]["row_count"])
        except Exception:
            pass

        if row_count is None:
            try:
                csql = f"SELECT COUNT(*) AS c FROM `{fq}`"
                row_count = int(list(bq.query(csql, location=region).result())[0]["c"])
            except Exception:
                row_count = 0

        return row_count, fq

    def _capture_silver_count_before(tbl: dict):
        count, fq = _get_silver_row_count(tbl)
        ctx = get_current_context()
        pipeline_id, run_id = _child_ids_from_ctx(ctx, tbl)
        db = tbl["database"].lower(); sch = tbl["schema"].lower(); tb = tbl["table"].lower()
        key = f"{db}.{sch}.{tb}"
        payload = {key: {"bq_table": fq, "row_count_before_merge": count}}
        _update_jsons_and_metrics(run_id, pipeline_id, target_metrics=payload)

    def _capture_silver_count_after(tbl: dict):
        count, fq = _get_silver_row_count(tbl)
        ctx = get_current_context()
        pipeline_id, run_id = _child_ids_from_ctx(ctx, tbl)
        db = tbl["database"].lower(); sch = tbl["schema"].lower(); tb = tbl["table"].lower()
        key = f"{db}.{sch}.{tb}"
        payload = {key: {"bq_table": fq, "row_count_after_merge": count}}
        _update_jsons_and_metrics(run_id, pipeline_id, target_metrics=payload)

    @task(pool=POOL, priority_weight=WEIGHTS["yaml"], weight_rule="absolute")
    def build_and_write_yaml():
        gcs_hook = GCSHook()
        client = bigquery.Client(project=project_id)
        ctx = get_current_context()
        ld = ctx["dag_run"].logical_date
        yyyy, mm, dd = ld.strftime("%Y"), ld.strftime("%m"), ld.strftime("%d")
        hh, mn, ss = ld.strftime("%H"), ld.strftime("%M"), ld.strftime("%S")

        def _clean_gs_uri(uri: str) -> str:
            if not uri.startswith("gs://"):
                return uri
            bucket_and_rest = uri[5:]
            if "/" not in bucket_and_rest:
                return uri
            bucket, rest = bucket_and_rest.split("/", 1)
            rest = re.sub(r"/{2,}", "/", rest.strip("/"))
            return f"gs://{bucket}/{rest}"

        def _fmt_ds(template: str) -> str:
            x = (template or "").strip().replace("{source_type}", "sqlserver").replace("{target_type}", "bigquery")
            return re.sub(r"(?i)bigquery", "bq", x)

        # Build metadata query (filtered)
        if schema_name:
            schema_list = [str(s).lower() for s in schema_name]
            q = f"""
            SELECT source_type, table_type, target_type, source_config, target_config, runtime_config,
                   metadata_config, extraction_flags, audit_config, qa_config, enabled
            FROM `{project_id}.{dataops_dataset}.{object_extraction_metadata_table}`
            WHERE enabled=@enabled_flag
              AND source_type=@source_type
              AND table_type=@type_of_data
              AND LOWER(JSON_VALUE(source_config,'$.database_name'))=@database_name
              AND LOWER(JSON_VALUE(source_config,'$.schema_name')) IN UNNEST(@schema_name)
              AND LOWER(JSON_VALUE(source_config,'$.type_of_extraction'))=@type_of_extraction
              AND pool=@pool
            """
            params = [
                bigquery.ScalarQueryParameter("enabled_flag", "BOOL", enabled_flag),
                bigquery.ScalarQueryParameter("source_type", "STRING", source_type),
                bigquery.ScalarQueryParameter("type_of_data", "STRING", type_of_data),
                bigquery.ScalarQueryParameter("database_name", "STRING", database_name.lower()),
                bigquery.ArrayQueryParameter("schema_name", "STRING", schema_list),
                bigquery.ScalarQueryParameter("type_of_extraction", "STRING", extraction_type),
                bigquery.ScalarQueryParameter("pool", "STRING", pool),
            ]
        else:
            q = f"""
            SELECT source_type, table_type, target_type, source_config, target_config, runtime_config,
                   metadata_config, extraction_flags, audit_config, qa_config, enabled
            FROM `{project_id}.{dataops_dataset}.{object_extraction_metadata_table}`
            WHERE enabled=@enabled_flag
              AND source_type=@source_type
              AND table_type=@type_of_data
              AND LOWER(JSON_VALUE(source_config,'$.database_name'))=@database_name
              AND LOWER(JSON_VALUE(source_config,'$.type_of_extraction'))=@type_of_extraction
              AND pool=@pool              
            """
            params = [
                bigquery.ScalarQueryParameter("enabled_flag", "BOOL", enabled_flag),
                bigquery.ScalarQueryParameter("source_type", "STRING", source_type),
                bigquery.ScalarQueryParameter("type_of_data", "STRING", type_of_data),
                bigquery.ScalarQueryParameter("database_name", "STRING", database_name.lower()),
                bigquery.ScalarQueryParameter("type_of_extraction", "STRING", extraction_type),
                bigquery.ScalarQueryParameter("pool", "STRING", pool),
            ]

        job_config = bigquery.QueryJobConfig(query_parameters=params)
        rows = [dict(r) for r in client.query(q, job_config=job_config).result()]

        # Build table list
        tables = []
        for record in rows:
            source_config = record.get("source_config") or {}
            target_config = record.get("target_config") or {}
            runtime_config = record.get("runtime_config") or {}
            metadata_config = record.get("metadata_config") or {}
            extraction_flags = record.get("extraction_flags") or {}
            qa_cfg = record.get("qa_config") or {}

            server = (source_config.get("server_address") or "").lower()
            database = (source_config.get("database_name") or "").lower()
            schema = (source_config.get("schema_name") or "").lower()
            table = (source_config.get("table_name") or "").lower()
            secret_id = source_config.get("secret_id") or ""
            olap_database = (source_config.get("olap_database") or "").lower()
            archive_database = (source_config.get("archive_database") or "").lower()

            bronze_dataset = _fmt_ds(target_config.get("bronze_layer_bq_dataset", bronze_layer_bq_dataset))
            silver_dataset = _fmt_ds(target_config.get("silver_layer_bq_dataset", silver_layer_bq_dataset))

            ctx2 = get_current_context()
            ld2 = ctx2["dag_run"].logical_date
            yyyy2, mm2, dd2 = ld2.strftime("%Y"), ld2.strftime("%m"), ld2.strftime("%d")
            hh2, mn2, ss2 = ld2.strftime("%H"), ld2.strftime("%M"), ld2.strftime("%S")

            raw_path = _clean_gs_uri(f"gs://{env_config['raw_bucket']}/{server}/{database}/{schema}/{table}/{yyyy2}-{mm2}-{dd2}/{hh2}_{mn2}_{ss2}/")
            processed_path = _clean_gs_uri(f"gs://{env_config['processed_bucket']}/{server}/{database}/{schema}/{table}/")

            bronze_table = f"{project_id}.{bronze_dataset}.{database}_{schema}_{table}"
            silver_table = f"{project_id}.{silver_dataset}.{database}_{schema}_{table}"

            qa_table_name = (qa_cfg.get("qa_table") or env_config.get("qa_results_table") or "").lower()
            qa_output_gcs_base = _clean_gs_uri(f"gs://{env_config['raw_bucket']}/{server}/{database}/{schema}/{table}/{yyyy2}-{mm2}-{dd2}/{hh2}_{mn2}_{ss2}/{qa_table_name}/")

            # === NEW: lookup relationships for this table ===
            try:
                relations = _fetch_lookup_relationships(
                    server_fk=server,
                    database_fk=database,
                    schema_fk=schema,
                    table_fk=table,
                )
            except Exception as e:
                logging.error(f"Failed to fetch lookup relationships for {database}.{schema}.{table}: {e}")
                relations = []

            tables.append({
                "server": server, "database": database, "schema": schema, "table": table,
                "primary_keys": metadata_config.get("primary_key", []),
                            # NEW FIELD (FK→PK metadata persisted into YAML)
                "lookup_relationships": relations,
                "type_of_extraction": source_config.get("type_of_extraction"),
                "query": "",
                "output_gcs_path": raw_path,
                "gcs_external_table_path": processed_path,
                "output_bigquery_bronze_table": bronze_table,
                "output_bigquery_silver_table": silver_table,
                "write_disposition": extraction_flags.get("bq_write_disposition", "write_append"),
                "secret_id": secret_id,
                "reload_flag": bool(extraction_flags.get("reload_flag", False)),
                "olap_database": olap_database,
                "archive_database": archive_database,
                "batch_size": runtime_config.get("batch_size"),
                "num_workers": runtime_config.get("num_workers"),
                "max_num_workers": runtime_config.get("max_num_workers"),
                "autoscaling_algorithm": runtime_config.get("autoscaling_algorithm"),
                "machine_type": runtime_config.get("machine_type"),
                "chunk_size": runtime_config.get("chunk_size"),
                "silver_layer_iceberg_bq_connection": target_config.get("silver_layer_iceberg_bq_connection", silver_layer_iceberg_bq_connection),
                "silver_layer_iceberg_bucket_path": target_config.get("silver_layer_iceberg_bucket_path", silver_layer_iceberg_bucket_path),
                "silver_layer_bq_dataset": silver_dataset,
                "bronze_layer_bq_dataset": bronze_dataset,
                "bq_information_schema_tables": target_config.get("bq_information_schema_tables", bq_information_schema_tables),
                "full_load_override_flag": runtime_config.get("full_load_override_flag"),
                "incremental_column": source_config.get("incremental_column"),
                # QA
                "qa_enabled": bool(qa_cfg.get("qa_enabled", False)),
                "qa_output_gcs_base": qa_output_gcs_base,
                "qa_table": qa_table_name,
                "qa_threshold_pct": qa_cfg.get("qa_threshold_pct", 100.0),
                "qa_allow_count_diff": qa_cfg.get("qa_allow_count_diff", 0),
                # stamp + types
                "execution_date": f"{yyyy}_{mm}_{dd}_{hh}_{mn}_{ss}",
                "source_type": "sqlserver",
                "target_type": "bigquery",
                # image for downstream DF param usage
                "template_worker_sdk_container_image": template_worker_sdk_container_image,
            })

        pipeline = dict(env_config)
        pipeline["run_id"] = pipeline_run_id_template  # OK: task is templated
        yaml_doc = {"pipeline": pipeline, "tables": tables}
        yaml_content = yaml.safe_dump(yaml_doc, sort_keys=False, default_flow_style=False)

        bucket, object_name = _gcs_split(yaml_uri)
        gcs_hook.upload(bucket_name=bucket, object_name=object_name,
                        data=yaml_content.encode("utf-8"), mime_type="application/x-yaml")

    def _yaml_with_suffix(yaml_uri: str, suffix: str = "ctc_config") -> str:
        base = re.sub(r"(?i)\.ya?ml$", "", (yaml_uri or "").strip())
        return f"{base}_{suffix}.yaml".lower()

    def _stable_job_base(db: str, sch: str, tb: str) -> str:
        return _sanitize_job_name("sql-gcs", db, sch, tb)

    start = PythonOperator(
        task_id="log_start",
        python_callable=lambda **_: logging.info("Master DAG started"),
        pool=POOL, priority_weight=WEIGHTS["misc"], weight_rule="absolute"
    )

    should_run_blackout_guard = ShortCircuitOperator(

        task_id="should_run_blackout_guard",

        python_callable=_should_run_now,

    )
    end = PythonOperator(
        task_id="log_end",
        python_callable=lambda **_: logging.info("Master DAG finished"),
        trigger_rule="all_done",
        pool=POOL, priority_weight=WEIGHTS["misc"], weight_rule="absolute"
    )
    all_tables_done = EmptyOperator(
        task_id="all_tables_done", trigger_rule="all_done",
        pool=POOL, priority_weight=WEIGHTS["misc"], weight_rule="absolute"
    )

    build_yaml = build_and_write_yaml()
    meta_base = _sanitize_job_name("sql-bq-metadata-comparison")
    change_tracking_yaml_output_path = _yaml_with_suffix(yaml_uri, "ctc_config")
    tables_cfg = _load_tables_from_yaml_safe(change_tracking_yaml_output_path)

    # === NEW: merge FK relationships from original YAML into the ctc_config YAML ===

    # Load original YAML (contains lookup_relationships)
    orig_tables_cfg = _load_tables_from_yaml_safe(yaml_uri)

    # Index original tables by unique key (db, schema, table)
    orig_index = {
        (t["database"].lower(), t["schema"].lower(), t["table"].lower()): t
        for t in orig_tables_cfg
    }

    # Inject lookup_relationships into the ctc_config tables
    for t in tables_cfg:
        key = (t["database"].lower(), t["schema"].lower(), t["table"].lower())
        if key in orig_index:
            t["lookup_relationships"] = orig_index[key].get("lookup_relationships", [])
        else:
            t["lookup_relationships"] = []


    for _t in tables_cfg:
        _t.setdefault("template_worker_sdk_container_image", template_worker_sdk_container_image)

    run_metadata_comparison = DataflowStartFlexTemplateOperator(
        task_id="sql_to_bq_metadata_comparison",
        project_id=project_id,
        location=region,
        body={
            "launchParameter": {
                "jobName": f"{meta_base}-{{{{ ds_nodash }}}}-t{{{{ ts_nodash[9:15] }}}}",
                "containerSpecGcsPath": template_sql_to_bq_metadata_comparison,
                "environment": {
                    "serviceAccountEmail": dataflow_service_account,
                    "subnetwork": dataflow_subnetwork,
                    "tempLocation": dataflow_temp_location,
                    "stagingLocation": dataflow_staging_location,
                    "ipConfiguration": ip_configuration,
                    "additionalUserLabels": {"triggered_by": "airflow"},
                },
                "parameters": {
                    "project_id": project_id,
                    "metadata_yaml_path": yaml_uri,
                    "change_tracking_metadata_yaml_output_path": change_tracking_yaml_output_path,
                    "extraction_metadata_bq_table": f"{project_id}.{dataops_dataset}.{object_extraction_metadata_table}",
                    "etl_state_table_fqn": ct_sync_table,
                    "sdk_container_image": template_sql_to_bq_metadata_comparison_image,
                    
                },
            }
        },
        pool=POOL,
        priority_weight=WEIGHTS["metadata_compare"],
        weight_rule="absolute",
        wait_until_finished=True,
    )

    # === Master audit (append-only)
    def _audit_dag_start_callable():
        ctx = get_current_context()
        run_id = _compute_run_id_from_ctx(ctx)
        pipeline_id = _compute_pipeline_id_from_ctx(ctx)
        orch = {
            "orchestrator": "airflow",
            "dag_id": ctx["ti"].dag_id,
            "task_id": ctx["ti"].task_id,
            "attempt": ctx["ti"].try_number,
            "trigger": "manual" if ctx["dag_run"].external_trigger else "schedule",
        }
        params = {"database": database_name, "schemas": schema_name, "tables_count": len(tables_cfg),
                  "type_of_extraction": "change_tracking"}
        _merge_upsert_start(run_id, pipeline_id, yaml_uri, params, orch)
        _append_comment(run_id, pipeline_id, "DAG started")
        _stamp_stage(run_id, pipeline_id, "copy_and_cleanup_started_at")

    audit_dag_start = PythonOperator(
        task_id="audit_dag_start",
        python_callable=_audit_dag_start_callable,
        pool=POOL, priority_weight=WEIGHTS["misc"], weight_rule="absolute",
    )

    # >>> Per-table counts + source metrics (append as their own rows)
    def _capture_counts_and_source_metrics(tbl: dict):
        bq = BigQueryHook().get_client(project_id=project_id, location=region)
        ctx = get_current_context()
        pipeline_id, run_id = _child_ids_from_ctx(ctx, tbl)

        db = (tbl["database"] or "").lower()
        sch = (tbl["schema"] or "").lower()
        tb = (tbl["table"] or "").lower()
        table_key = f"{db}.{sch}.{tb}"

        bronze_table_fq = tbl.get("output_bigquery_bronze_table")

        sql_counts = f"""
          WITH base AS (
            SELECT UPPER(COALESCE(sys_change_operation, 'I')) AS op
            FROM `{bronze_table_fq}`
          )
          SELECT
            SUM(CASE WHEN op='I' THEN 1 ELSE 0 END) AS inserts,
            SUM(CASE WHEN op='U' THEN 1 ELSE 0 END) AS updates,
            SUM(CASE WHEN op='D' THEN 1 ELSE 0 END) AS deletes,
            COUNT(*) AS total
          FROM base
        """
        res = list(bq.query(sql_counts).result())
        if res:
            row = res[0]
            inserts = int(row["inserts"] or 0)
            updates = int(row["updates"] or 0)
            deletes = int(row["deletes"] or 0)
            total   = int(row["total"]   or 0)
        else:
            inserts = updates = deletes = total = 0

        record_counts = {table_key: {"inserts": inserts, "updates": updates, "deletes": deletes, "total": total}}
        source_metrics = {
            table_key: {
                "source_type": "sqlserver",
                "server": tbl.get("server", ""),
                "database": db,
                "schema": sch,
                "table": tb,
                "type_of_extraction": (tbl.get("type_of_extraction") or "").lower()
            }
        }
        _update_jsons_and_metrics(run_id, pipeline_id, source_metrics=source_metrics, record_counts=record_counts)

    # Build per-table groups
    table_groups = []
    for tbl in tables_cfg:
        tg_id = f"{tbl['database']}_{tbl['schema']}_{tbl['table']}_pipeline"
        with TaskGroup(group_id=tg_id) as tg:
            job_base = _stable_job_base(tbl["database"], tbl["schema"], tbl["table"])
            tbl_suffix = _table_suffix(tbl)
            has_ct = ((tbl.get("type_of_extraction", "") or "").lower() == "change_tracking")

            # ---- Create per-table audit start row
            def _audit_table_start(tbl=tbl, tbl_suffix=tbl_suffix):
                ctx = get_current_context()
                pipeline_id, run_id = _child_ids_from_ctx(ctx, tbl)
                orch = {
                    "orchestrator": "airflow",
                    "dag_id": ctx["ti"].dag_id,
                    "task_id": ctx["ti"].task_id,
                    "attempt": ctx["ti"].try_number,
                    "table": tbl_suffix,
                    "trigger": "manual" if ctx["dag_run"].external_trigger else "schedule",
                }
                params = {
                    "database": tbl["database"], "schema": tbl["schema"], "table": tbl["table"],
                    "type_of_extraction": (tbl.get("type_of_extraction") or "").lower()
                }
                _merge_upsert_start(run_id, pipeline_id, yaml_uri, params, orch)
                _append_comment(run_id, pipeline_id, f"Table run started for {tbl_suffix}")

            audit_table_start = PythonOperator(
                task_id=f"audit_start_{tbl_suffix}",
                python_callable=_audit_table_start,
                pool=POOL, priority_weight=WEIGHTS["misc"], weight_rule="absolute",
            )

            # ---- Branch: run extraction or skip with success
            def _branch_on_changes(
                reload_flag: bool,
                has_change_tracking: bool,
                sqlserver_row_count: int,
                bigquery_row_count: int,
                sqlserver_ct_version: int,
                bigquery_ct_version: int,
                *, tbl=tbl, tbl_suffix=tbl_suffix
             ) -> str:
            #     should_run = _should_process_table(
            #         reload_flag=reload_flag,
            #         has_change_tracking=has_change_tracking,
            #         sqlserver_row_count=sqlserver_row_count,
            #         bigquery_row_count=bigquery_row_count,
            #         sqlserver_ct_version=sqlserver_ct_version,
            #         bigquery_ct_version=bigquery_ct_version,
            #     )
                should_run = True
                # Build fully-qualified ids using the current task's group prefix
                ctx = get_current_context()
                full_tid = ctx["ti"].task_id  # e.g. "<group_id>.decide_changes_<suffix>"
                group_prefix = full_tid.rsplit(".", 1)[0] if "." in full_tid else ""  # "<group_id>"
                if should_run:
                    return f"{group_prefix}.run_sql_to_gcs_{tbl_suffix}_batched"
                else:
                    return f"{group_prefix}.no_changes_success_{tbl_suffix}"

            decide_changes = BranchPythonOperator(
                task_id=f"decide_changes_{tbl_suffix}",
                python_callable=_branch_on_changes,
                op_kwargs={
                    "reload_flag": tbl.get("reload_flag", False),
                    "has_change_tracking": has_ct,
                    "sqlserver_row_count": tbl.get("sqlserver_row_count", 0),
                    "bigquery_row_count": tbl.get("bq_row_count", 0),
                    "sqlserver_ct_version": tbl.get("sqlserver_current_version", 0),
                    "bigquery_ct_version": tbl.get("bq_max_sys_change_version", 0),
                },
                pool=POOL,
                priority_weight=WEIGHTS["gate"],
                weight_rule="absolute",
            )

            def _on_no_changes_success(tbl=tbl):
                ctx = get_current_context()
                pipeline_id, run_id = _child_ids_from_ctx(ctx, tbl)
                _append_comment(run_id, pipeline_id, f"No changes detected for {_table_suffix(tbl)} — skipping extraction.")
                _finalize_success(run_id, pipeline_id, FINAL_SUCCESS_STATUS)

            no_changes_success = PythonOperator(
                task_id=f"no_changes_success_{tbl_suffix}",
                python_callable=_on_no_changes_success,
                pool=POOL, priority_weight=WEIGHTS["misc"], weight_rule="absolute",
            )

            table_done = EmptyOperator(
                task_id=f"table_done_{tbl_suffix}",
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                pool=POOL, priority_weight=WEIGHTS["misc"], weight_rule="absolute",
            )

            # ---- Dataflow extraction (child run_id in params + labels)
            run_sql_to_gcs = DataflowStartFlexTemplateOperator(
                task_id=f"run_sql_to_gcs_{tbl_suffix}_batched",
                project_id=project_id,
                location=region,
                body={
                    "launchParameter": {
                        "jobName": "{{ (params.job_base ~ '-' ~ ds_nodash ~ '-t' ~ ts_nodash[9:15])[:63] }}",
                        "containerSpecGcsPath": template_sql_to_gcs,
                        "environment": {
                            "serviceAccountEmail": dataflow_service_account,
                            "subnetwork": dataflow_subnetwork,
                            "tempLocation": dataflow_temp_location,
                            "stagingLocation": dataflow_staging_location,
                            "ipConfiguration": ip_configuration,
                            "additionalUserLabels": {
                                "triggered_by": "airflow",
                                "pipeline_id": pipeline_id_template,
                                "run_id": "{{ ('sqlserver_to_bigquery_ingestion_' ~ ds_nodash ~ '_t' ~ ts_nodash[9:15] ~ '-' ~ params.tbl_suffix)[:63] }}",
                            },
                        },
                        "parameters": {
                            "gcp_project": project_id,
                            "dataops_dataset": dataops_dataset,
                            "database": tbl["database"],
                            "schema": tbl["schema"],
                            "table": tbl["table"],
                            "primary_key": ",".join(tbl.get("primary_keys", [])),
                            "type_of_extraction": tbl["type_of_extraction"],
                            "reload_flag": str(tbl.get("reload_flag", False)).lower(),
                            "allow_no_pk_full_reload": str(tbl.get("allow_no_pk_full_reload", True)).lower(),
                            "no_pk_row_limit": str(tbl.get("no_pk_row_limit", 20000000)),
                            "batch_size": str(tbl.get("batch_size", "")),
                            "chunk_size": str(tbl.get("chunk_size", "")),
                            "num_workers": str(tbl.get("num_workers", "")),
                            "max_num_workers": str(tbl.get("max_num_workers", "")),
                            "autoscaling_algorithm": str(tbl.get("autoscaling_algorithm", "")),
                            "machine_type": str(tbl.get("machine_type", "")),
                            "output_gcs_path": tbl["output_gcs_path"],
                            "incremental_column": tbl.get("incremental_column", "") or "",
                            "query": tbl.get("query", "") or "",
                            "secret_id": tbl["secret_id"],
                            "audit_project": project_id,
                            "audit_run_table": audit_run_table_name,
                            "ct_version_sync_table": ct_version_sync_table_name,
                            "olap_database": tbl.get("olap_database", ""),
                            "archive_database": tbl.get("archive_database", ""),
                            "qa_enabled": str(tbl.get("qa_enabled", False)).lower(),
                            "qa_output_gcs_base": tbl.get("qa_output_gcs_base", ""),
                            "qa_table": tbl.get("qa_table", ""),
                            "qa_threshold_pct": str(tbl.get("qa_threshold_pct", "")),
                            "qa_allow_count_diff": str(tbl.get("qa_allow_count_diff", 0)),
                            "pipeline_id": pipeline_id_template,
                            "run_id": "{{ " +
                                      "'sqlserver_to_bigquery_ingestion_' ~ ds_nodash ~ '_t' ~ ts_nodash[9:15] ~ '-' ~ params.tbl_suffix" +
                                      " }}",
                            "sdk_container_image": template_worker_sdk_container_image,
                        },
                    }
                },
                params={"job_base": job_base, "tbl_suffix": tbl_suffix},
                pool=POOL, priority_weight=WEIGHTS["extraction"], weight_rule="absolute",
                retries=0, wait_until_finished=True,
            )

            def _after_df_launch(tbl=tbl, tbl_suffix=tbl_suffix):
                ctx = get_current_context()
                pipeline_id, run_id = _child_ids_from_ctx(ctx, tbl)
                _append_comment(run_id, pipeline_id, f"Dataflow job launched for {tbl_suffix}")

            mark_df_launched = PythonOperator(
                task_id=f"audit_mark_df_launched_{tbl_suffix}",
                python_callable=_after_df_launch,
                pool=POOL, priority_weight=WEIGHTS["misc"], weight_rule="absolute",
            )

            def _mark_copy_started(tbl=tbl, tbl_suffix=tbl_suffix):
                ctx = get_current_context()
                pipeline_id, run_id = _child_ids_from_ctx(ctx, tbl)
                _append_comment(run_id, pipeline_id, f"Copy & cleanup started for {tbl_suffix}")
                _stamp_stage(run_id, pipeline_id, "copy_and_cleanup_started_at")

            mark_copy_started = PythonOperator(
                task_id=f"audit_mark_copy_started_{tbl_suffix}",
                python_callable=_mark_copy_started,
                pool=POOL, priority_weight=WEIGHTS["gcs_copy"], weight_rule="absolute",
            )

            # ---- Copy raw → processed (external path) ----
            src_bucket = tbl["output_gcs_path"].replace("gs://", "").split("/")[0]
            src_path = "/".join(tbl["output_gcs_path"].replace("gs://", "").split("/")[1:])
            if not src_path.endswith("/"): src_path += "/"
            dst_uri_root = tbl["gcs_external_table_path"]
            dst_bucket = dst_uri_root.replace("gs://", "").split("/")[0]
            dst_path = "/".join(dst_uri_root.replace("gs://", "").split("/")[1:])
            if not dst_path.endswith("/"): dst_path += "/"

            copy_cmd = f"""
                set -euo pipefail
                echo 'Cleaning destination: gs://{dst_bucket}/{dst_path}'
                gsutil -m rm -r 'gs://{dst_bucket}/{dst_path}**' || true
                echo 'Copying from gs://{src_bucket}/{src_path} to gs://{dst_bucket}/{dst_path}'
                gsutil -m cp -r 'gs://{src_bucket}/{src_path}*' 'gs://{dst_bucket}/{dst_path}'
            """
            cleanup_and_copy = BashOperator(
                task_id=f"cleanup_and_copy_{tbl_suffix}",
                bash_command=copy_cmd,
                pool=POOL, priority_weight=WEIGHTS["gcs_copy"], weight_rule="absolute",
            )

            def _mark_gcs_ready(tbl=tbl, tbl_suffix=tbl_suffix):
                ctx = get_current_context()
                pipeline_id, run_id = _child_ids_from_ctx(ctx, tbl)
                _append_comment(run_id, pipeline_id, f"GCS ready for {tbl_suffix}")
                _stamp_stage(run_id, pipeline_id, "copy_and_cleanup_completed_at")

            mark_gcs_ready = PythonOperator(
                task_id=f"audit_mark_gcs_ready_{tbl_suffix}",
                python_callable=_mark_gcs_ready,
                pool=POOL, priority_weight=WEIGHTS["misc"], weight_rule="absolute",
            )

            # === NEW: capture parquet file metrics into total_files_written / total_bytes_written
            capture_file_metrics = PythonOperator(
                task_id=f"capture_file_metrics_{tbl_suffix}",
                python_callable=_capture_gcs_file_metrics,
                op_kwargs={"tbl": tbl},
                pool=POOL, priority_weight=WEIGHTS["gcs_copy"], weight_rule="absolute",
            )

            # ---- Bronze (external table) ----
            def _mark_bronze_started(tbl=tbl, tbl_suffix=tbl_suffix):
                ctx = get_current_context()
                pipeline_id, run_id = _child_ids_from_ctx(ctx, tbl)
                _append_comment(run_id, pipeline_id, f"Bronze external creation started for {tbl_suffix}")
                _stamp_stage(run_id, pipeline_id, "bronze_layer_creation_started_at")

            bronze_started = PythonOperator(
                task_id=f"audit_mark_bronze_started_{tbl_suffix}",
                python_callable=_mark_bronze_started,
                pool=POOL, priority_weight=WEIGHTS["misc"], weight_rule="absolute",
            )

            def _create_external(tbl=tbl, tbl_suffix=tbl_suffix):
                _generate_and_run_external_table_sql(
                    table={
                        "database": tbl["database"].lower(),
                        "schema": tbl["schema"].lower(),
                        "table": tbl["table"].lower(),
                        "gcs_external_table_path": f"{tbl['gcs_external_table_path']}/*.parquet",
                        "output_bigquery_bronze_table": tbl["output_bigquery_bronze_table"],
                    },
                    project_id=project_id, region=region, dataops_dataset=dataops_dataset
                )

            create_external = PythonOperator(
                task_id=f"create_external_table_{tbl_suffix}",
                python_callable=_create_external,
                pool=POOL, priority_weight=WEIGHTS["external_table"], weight_rule="absolute",
            )

            def _mark_external_created(tbl=tbl, tbl_suffix=tbl_suffix):
                ctx = get_current_context()
                pipeline_id, run_id = _child_ids_from_ctx(ctx, tbl)
                _append_comment(run_id, pipeline_id, f"Bronze external created for {tbl_suffix}")
                _stamp_stage(run_id, pipeline_id, "bronze_layer_creation_completed_at")

            mark_external_created = PythonOperator(
                task_id=f"audit_mark_external_created_{tbl_suffix}",
                python_callable=_mark_external_created,
                pool=POOL, priority_weight=WEIGHTS["misc"], weight_rule="absolute",
            )

            capture_counts_and_source = PythonOperator(
                task_id=f"capture_counts_and_source_{tbl_suffix}",
                python_callable=_capture_counts_and_source_metrics,
                op_kwargs={"tbl": tbl},
                pool=POOL, priority_weight=WEIGHTS["misc"], weight_rule="absolute",
            )

            # ---- Silver (Iceberg create/merge) ----
            def _mark_silver_started(tbl=tbl, tbl_suffix=tbl_suffix):
                ctx = get_current_context()
                pipeline_id, run_id = _child_ids_from_ctx(ctx, tbl)
                _append_comment(run_id, pipeline_id, f"Silver merge started for {tbl_suffix}")
                _stamp_stage(run_id, pipeline_id, "silver_layer_creation_started_at")

            silver_started = PythonOperator(
                task_id=f"audit_mark_silver_started_{tbl_suffix}",
                python_callable=_mark_silver_started,
                pool=POOL, priority_weight=WEIGHTS["misc"], weight_rule="absolute",
            )

            # === NEW: capture silver row count BEFORE merge
            capture_silver_count_before = PythonOperator(
                task_id=f"capture_silver_count_before_{tbl_suffix}",
                python_callable=_capture_silver_count_before,
                op_kwargs={"tbl": tbl},
                pool=POOL, priority_weight=WEIGHTS["iceberg"], weight_rule="absolute",
            )

            def _build_iceberg(tbl=tbl, tbl_suffix=tbl_suffix):
                ctx = get_current_context()
                pipeline_id, run_id = _child_ids_from_ctx(ctx, tbl)
                dag_id = ctx["ti"].dag_id
                job_name = f"{_sanitize_job_name('sql-gcs', tbl['database'], tbl['schema'], tbl['table'])}-{ctx['ds_nodash']}-t{ctx['ts_nodash'][9:15]}"
                _prepare_generate_execute_iceberg_sql(table=tbl, dataflow_job_name=job_name, run_id=run_id, dag_id=dag_id)

            build_iceberg = PythonOperator(
                task_id=f"{tbl_suffix}_prepare_generate_execute_iceberg",
                python_callable=_build_iceberg,
                pool=POOL, priority_weight=WEIGHTS["iceberg"], weight_rule="absolute",
            )

            def _mark_silver_completed(tbl=tbl, tbl_suffix=tbl_suffix):
                ctx = get_current_context()
                pipeline_id, run_id = _child_ids_from_ctx(ctx, tbl)
                _append_comment(run_id, pipeline_id, f"Silver merge completed for {tbl_suffix}")
                _stamp_stage(run_id, pipeline_id, "silver_layer_creation_completed_at")

            mark_silver_completed = PythonOperator(
                task_id=f"audit_mark_silver_completed_{tbl_suffix}",
                python_callable=_mark_silver_completed,
                pool=POOL, priority_weight=WEIGHTS["iceberg"], weight_rule="absolute",
            )

            # === NEW: capture silver row count AFTER merge
            capture_silver_count_after = PythonOperator(
                task_id=f"capture_silver_count_after_{tbl_suffix}",
                python_callable=_capture_silver_count_after,
                op_kwargs={"tbl": tbl},
                pool=POOL, priority_weight=WEIGHTS["iceberg"], weight_rule="absolute",
            )

            def _finalize_table(tbl=tbl, tbl_suffix=tbl_suffix):
                ctx = get_current_context()
                pipeline_id, run_id = _child_ids_from_ctx(ctx, tbl)
                _append_comment(run_id, pipeline_id, f"Silver merge completed for {tbl_suffix}")
                _finalize_success(run_id, pipeline_id, FINAL_SUCCESS_STATUS)

            finalize_table = PythonOperator(
                task_id=f"audit_finalize_table_{tbl_suffix}",
                python_callable=_finalize_table,
                pool=POOL, priority_weight=WEIGHTS["misc"], weight_rule="absolute",
            )

            ct_sync_success = PythonOperator(
                task_id=f"ct_sync_success_{tbl_suffix}",
                python_callable=finalize_ct_sync_run,
                op_kwargs={"tbl": tbl},
                pool=POOL, priority_weight=WEIGHTS["misc"], weight_rule="absolute",
            )

            # Branching wiring
            audit_table_start >> decide_changes
            # No changes path
            decide_changes >> no_changes_success >> table_done
            # Changes path
            decide_changes >> run_sql_to_gcs
            (
                run_sql_to_gcs
                >> mark_df_launched
                >> mark_copy_started
                >> cleanup_and_copy
                >> mark_gcs_ready
                >> capture_file_metrics
                >> bronze_started
                >> create_external
                >> mark_external_created
                >> capture_counts_and_source
                >> silver_started
                >> capture_silver_count_before
                >> build_iceberg
                >> mark_silver_completed
                >> capture_silver_count_after
                >> finalize_table
                >> ct_sync_success
                >> table_done
            )

        table_groups.append(tg)

    def _audit_dag_finalize_callable():
        ctx = get_current_context()
        run_id = _compute_run_id_from_ctx(ctx)
        pipeline_id = _compute_pipeline_id_from_ctx(ctx)
        _append_comment(run_id, pipeline_id, "All table groups finished")
        _stamp_stage(run_id, pipeline_id, "silver_layer_creation_completed_at")
        _finalize_success(run_id, pipeline_id, FINAL_SUCCESS_STATUS)

    audit_dag_finalize = PythonOperator(
        task_id="audit_dag_finalize",
        python_callable=_audit_dag_finalize_callable,
        pool=POOL, priority_weight=WEIGHTS["misc"], weight_rule="absolute",
    )

    start >> should_run_blackout_guard >> build_yaml >> audit_dag_start >> run_metadata_comparison >> table_groups >> audit_dag_finalize >> all_tables_done >> end

sql_to_bq_extraction_xpomaster_dbo_carrier_ct = sql_to_bq_extraction_xpomaster_dbo_carrier_ct()

