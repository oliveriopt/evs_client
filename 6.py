import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import logging
import pyodbc
import json
from google.cloud import secretmanager
from typing import Dict


class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--project_id', required=True)
        parser.add_argument('--sql_secret_id', required=True)
        parser.add_argument('--sql_query', required=True)
        parser.add_argument('--output_bq_table', required=False, default="")  # opcional


def get_sql_config(secret_id: str, project_id: str) -> dict:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    resp = client.access_secret_version(request={'name': name})
    return json.loads(resp.payload.data.decode('utf-8'))


def build_connection_string(cfg: dict) -> str:
    driver = cfg.get('driver', 'ODBC Driver 17 for SQL Server')
    server = cfg.get('privateIPaddress') or cfg.get('publicIPaddress') or cfg.get('server')
    parts = [f"DRIVER={{{{{}}}}};SERVER={server},1433;".format(driver)]
    if cfg.get('database'):
        parts.append(f"DATABASE={cfg['database']};")
    if cfg.get('username'):
        parts.append(f"UID={cfg['username']};")
    if cfg.get('password'):
        parts.append(f"PWD={cfg['password']};")
    parts.append("Encrypt=yes;TrustServerCertificate=yes;Packet Size=512;")
    print(''.join(parts))
    return ''.join(parts)


class RunSqlQuery(beam.DoFn):
    def __init__(self, project_id: str, secret_id: str, query: str):
        self.project_id = project_id
        self.secret_id = secret_id
        self.query = query
        self.conn_str = None

    def setup(self):
        cfg = get_sql_config(self.secret_id, self.project_id)
        print(cfg)
        self.conn_str = build_connection_string(cfg)

    def process(self, _):
        conn = None
        try:
            conn = pyodbc.connect(self.conn_str, timeout=60)
            cursor = conn.cursor()
            cursor.execute(self.query)

            columns = [col[0] for col in cursor.description] if cursor.description else []
            for row in cursor.fetchall():
                # Devuelvo cada fila como dict {col: valor}
                yield {col: val for col, val in zip(columns, row)}

        finally:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass


def run(argv=None):
    pipeline_options = PipelineOptions(
        argv,
        save_main_session=True
    )
    pipeline_options.view_as(SetupOptions).save_main_session = True
    opts = pipeline_options.view_as(CustomOptions)

    with beam.Pipeline(options=pipeline_options) as p:
        rows = (
            p
            | "Start" >> beam.Create([None])
            | "Run SQL" >> beam.ParDo(RunSqlQuery(
                project_id=opts.project_id,
                secret_id=opts.sql_secret_id,
                query=opts.sql_query
            ))
        )

        # Si quieres escribir a BigQuery:
        if opts.output_bq_table:
            _ = (
                rows
                | "Write to BQ" >> beam.io.WriteToBigQuery(
                    table=opts.output_bq_table,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                )
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
