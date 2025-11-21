import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import json
import logging
from datetime import datetime, timezone
import re
import pyodbc
from google.cloud import bigquery
from google.cloud import secretmanager
from google.api_core.exceptions import NotFound, BadRequest
import yaml
from typing import Dict


class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--project_id', required=True)
        parser.add_argument('--metadata_yaml_path', required=True)  # input YAML (GCS)
        parser.add_argument('--change_tracking_metadata_yaml_output_path', required=True)  # output YAML (GCS)
        parser.add_argument('--extraction_metadata_bq_table', required=False, default="")  # kept for compatibility
        parser.add_argument('--etl_state_table_fqn', required=True)  # e.g. project.dataset.etl_state


def camel_to_snake_case(name: str) -> str:
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.replace("__", "_").lower()


# =====================
# Secret + connection helpers (user-preferred)
# =====================

def get_sql_config(secret_id: str, project_id: str) -> dict:
    logging.info("Accessing secret '%s' in project '%s'", secret_id, project_id)
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    resp = client.access_secret_version(request={'name': name})
    payload = resp.payload.data.decode('utf-8')
    logging.info("Secret retrieved successfully.")
    return json.loads(payload)


def build_connection_string(cfg: dict) -> str:
    # Default to ODBC 17 as requested
    driver = cfg.get('driver', 'ODBC Driver 17 for SQL Server')
    # 1) prefer privateIPaddress → 2) publicIPaddress → 3) server
    server = cfg.get('privateIPaddress') or cfg.get('publicIPaddress') or cfg.get('server')
    parts = [f"DRIVER={{{{}}}};SERVER={server},1433;".format(driver)]
    # Use database only if present in secret JSON (do not override from YAML)
    if cfg.get('database'):
        parts.append(f"DATABASE={cfg['database']};")
    if cfg.get('username'):
        parts.append(f"UID={cfg['username']};")
    if cfg.get('password'):
        parts.append(f"PWD={cfg['password']};")
    parts.append("Encrypt=yes;TrustServerCertificate=yes;Packet Size=512;")
    return ''.join(parts)


# =====================
# BigQuery watermark helpers (no ensure/create)
# =====================

def get_last_sync_version_bq(
    client: bigquery.Client,
    table_fqn: str,
    server_name: str,
    source_db: str,
    schema: str,
    table: str,
) -> int:
    sql = f"""
    SELECT COALESCE(MAX(SAFE_CAST(last_applied_version AS INT64)), 0) AS last_sync_version
    FROM `{table_fqn}`
    WHERE lower(source_server_name)   = @srv
      AND lower(source_database_name) = @db
      AND lower(source_schema_name)   = @sch
      AND lower(source_table_name)    = @tbl
      AND REGEXP_CONTAINS(LOWER(COALESCE(status, '')), r'success')
    """
    try:
        job = client.query(sql, job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("srv", "STRING", (server_name or '').lower()),
                bigquery.ScalarQueryParameter("db",  "STRING", (source_db or '').lower()),
                bigquery.ScalarQueryParameter("sch", "STRING", (schema or '').lower()),
                bigquery.ScalarQueryParameter("tbl", "STRING", (table or '').lower()),
            ]
        ))
        rows = list(job.result())
        return int(rows[0]["last_sync_version"]) if rows else 0
    except NotFound:
        # Watermark table not found → treat as first run
        logging.warning("Watermark table not found: %s; returning 0", table_fqn)
        return 0
    except BadRequest as e:
        logging.warning("BQ query failed on %s: %s; returning 0", table_fqn, e)
        return 0
    except Exception as e:
        logging.warning("BQ watermark lookup error on %s: %s; returning 0", table_fqn, e)
        return 0


# =====================
# SQL Server helpers
# =====================

def fetch_sqlserver_version(database: str, schema: str, table: str, conn_str: str) -> int:
    """Return MAX(SYS_CHANGE_VERSION) for schema.table, or 0 on error."""
    conn = None
    try:
        conn = pyodbc.connect(conn_str, timeout=30)
        cur = conn.cursor()
        sql = f"SELECT MAX(SYS_CHANGE_VERSION) FROM CHANGETABLE(CHANGES [{schema}].[{table}], 0) AS CT"  # nosec B608
        cur.execute(sql)
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except Exception as e:
        logging.warning("SQL Server CT lookup failed for %s.%s.%s: %s", database, schema, table, e)
        return 0
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


# =====================
# DoFns
# =====================
class ReadYamlFromGCS(beam.DoFn):
    def __init__(self, yaml_path: str):
        self.yaml_path = yaml_path

    def process(self, _):
        from apache_beam.io.filesystems import FileSystems
        logging.info("Reading metadata YAML: %s", self.yaml_path)
        with FileSystems.open(self.yaml_path) as f:
            data = yaml.safe_load(f.read().decode("utf-8")) or {}
        tables = data.get("tables", [])
        for table in tables:
            # normalize common casing variants from YAML
            norm = dict(table)
            for k in list(table.keys()):
                if re.search(r"[A-Z]", k):
                    norm[camel_to_snake_case(k)] = norm.pop(k)
            yield norm


class CompareVersionsAndEnrich(beam.DoFn):
    def __init__(self, project_id: str, etl_state_table_fqn: str, extraction_metadata_bq_table: str = ""):
        self.project_id = project_id
        self.extraction_metadata_bq_table = extraction_metadata_bq_table  # not used, kept for signature
        self.etl_state_table_fqn = etl_state_table_fqn
        self._secret_cache: Dict[str, dict] = {}
        self.bq_client = None

    def setup(self):
        self.bq_client = bigquery.Client(project=self.project_id)

    def _get_secret_cfg(self, secret_id: str) -> dict:
        if secret_id in self._secret_cache:
            return self._secret_cache[secret_id]
        cfg = get_sql_config(secret_id, self.project_id)
        self._secret_cache[secret_id] = cfg
        return cfg

    def process(self, table_config: dict):
        # Accept multiple possible key spellings from YAML
        def pick(d,k,alts):
            if k in d: return d[k]
            for a in alts:
                if a in d: return d[a]
            return ''
        db  = pick(table_config, 'database', ['database_name'])
        sch = pick(table_config, 'schema',   ['schema_name'])
        tbl = pick(table_config, 'table',    ['table_name'])
        srv = pick(table_config, 'server_name', ['server','server_host'])
        # Per-table secret id is **required** (no global fallback)
        tbl_secret_id = pick(table_config, 'secret_id', ['sql_secret_id','secret','secretName'])
        if not tbl_secret_id:
            raise RuntimeError(f"No secret_id provided for table {srv}|{db}.{sch}.{tbl}.")

        # Build connection string with user-preferred logic and ODBC 17
        secret_cfg = self._get_secret_cfg(tbl_secret_id)
        # Do NOT inject/override database from YAML; use only what secret provides
        conn_str = build_connection_string(secret_cfg)

        logging.info("Comparing CT versions for %s|%s.%s.%s using secret %s", srv, db, sch, tbl, tbl_secret_id)
        sqlserver_version = fetch_sqlserver_version(db, sch, tbl, conn_str)
        bq_version = get_last_sync_version_bq(
            client=self.bq_client,
            table_fqn=self.etl_state_table_fqn,
            server_name=srv or '',
            source_db=db or '',
            schema=sch or '',
            table=tbl or '',
        )
        has_changes = sqlserver_version > bq_version

        enriched = {
            **table_config,
            "sqlserver_current_version": sqlserver_version,
            # Keep field name but now sourced from watermark table
            "bq_max_sys_change_version": bq_version,
            "has_changes": has_changes,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }
        yield enriched


class WriteYaml(beam.DoFn):
    def __init__(self, output_path: str):
        self.output_path = output_path

    def process(self, enriched_tables: list):
        if not enriched_tables:
            logging.info("No tables to write; skipping YAML output")
            return
        yaml_data = {"tables": enriched_tables}
        yaml_str = yaml.dump(yaml_data, sort_keys=False, allow_unicode=True)
        from apache_beam.io.filesystems import FileSystems
        logging.info("Writing enriched YAML to: %s", self.output_path)
        with FileSystems.create(self.output_path) as f:
            f.write(yaml_str.encode("utf-8"))
        yield self.output_path


# =====================
# Entrypoint
# =====================

def run(argv=None):
    pipeline_options = PipelineOptions(
        save_main_session=True
    )
    pipeline_options.view_as(SetupOptions).save_main_session = True
    opts = pipeline_options.view_as(CustomOptions)

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Start" >> beam.Create([None])
            | "Read YAML" >> beam.ParDo(ReadYamlFromGCS(opts.metadata_yaml_path))
            | "Compare & Enrich" >> beam.ParDo(CompareVersionsAndEnrich(
                opts.project_id,
                opts.etl_state_table_fqn,
                opts.extraction_metadata_bq_table
            ))
            | "To list" >> beam.combiners.ToList()
            | "Write YAML" >> beam.ParDo(WriteYaml(opts.change_tracking_metadata_yaml_output_path))
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
