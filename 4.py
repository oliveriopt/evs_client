Sí, puedes. No necesitas Spark ni Composer. Dataflow sólo exige que corras un pipeline Beam; dentro de ese pipeline puedes usar `pyodbc` como en tu código anterior.

La idea práctica es:

* Haces un pipeline Beam mínimo que:

  * Crea un solo elemento (`Create([None])`).
  * En un `DoFn` abre la conexión `pyodbc` a SQL Server.
  * Ejecuta la query.
  * Opcionalmente escribe el resultado en BigQuery o GCS.
* Ese pipeline lo empaquetas en un Flex Template (con una imagen Docker que tenga `pyodbc` y el driver ODBC) y lo lanzas con Dataflow.

Te dejo un ejemplo completo y directo.

---

### 1) Código Python del pipeline (sólo Dataflow + pyodbc)

```python
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
    return ''.join(parts)


class RunSqlQuery(beam.DoFn):
    def __init__(self, project_id: str, secret_id: str, query: str):
        self.project_id = project_id
        self.secret_id = secret_id
        self.query = query
        self.conn_str = None

    def setup(self):
        cfg = get_sql_config(self.secret_id, self.project_id)
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
```

Con esto:

* Si tu query es tipo `SELECT MAX(SYS_CHANGE_VERSION) AS max_ver FROM ...`, tendrás una fila con `{"max_ver": <valor>}` que puedes:

  * Mandar a BigQuery (`--output_bq_table`), o
  * Leer desde los logs / debug si no te importa persistirlo.

Si **sólo** quieres ejecutar la query (p.ej. un `EXEC sp_loquesea` sin resultado importante), basta con que el `DoFn` no haga `yield` o que haga un `yield {"status": "ok"}`; el resto del pipeline puede ser un `WriteToText` o nada.

---

### 2) Imagen Docker para Dataflow Flex (clave para `pyodbc`)

En Dataflow Python necesitas una imagen que tenga:

* `pyodbc`
* El driver de SQL Server (`msodbcsql17`)
* `unixodbc` / `unixodbc-dev`

Ejemplo mínimo de `Dockerfile`:

```dockerfile
FROM gcr.io/dataflow-templates-base/python39-template-launcher-base

# Instalar dependencias ODBC y driver de SQL Server
RUN apt-get update && \
    apt-get install -y curl gnupg apt-transport-https unixodbc unixodbc-dev && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list \
        > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Instalar dependencias Python
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copiar tu código
COPY main.py /template/main.py

# Entry point estándar de Dataflow Flex
ENTRYPOINT ["python", "/template/main.py"]
```

`requirements.txt` mínimo:

```text
apache-beam[gcp]==2.58.0
pyodbc
google-cloud-secret-manager
google-cloud-bigquery
PyYAML
```

---

### 3) Lanzar el job sin Composer

Opciones sin Composer:

* `gcloud dataflow flex-template run ...` (si usas Flex Template).
* `gcloud dataflow jobs run ...` con `--gcs-location` a tu template tradicional.
* HTTP call al endpoint de templates desde Cloud Scheduler / Cloud Functions, si quieres agendar.

Ejemplo con Flex Template:

1. Construyes y subes la imagen a `Artifact Registry` o `Container Registry`.
2. Creas el Flex Template:

```bash
gcloud dataflow flex-template build gs://TU_BUCKET/templates/sqlserver_simple_query.json \
  --image "us-central1-docker.pkg.dev/TU_PROYECTO/dataflow-templates/sqlserver-simple:latest" \
  --sdk-language "PYTHON" \
  --metadata-file "flex_metadata.json"
```

3. `flex_metadata.json` define los parámetros (`sql_query`, `sql_secret_id`, etc.).
4. Ejecutas:

```bash
gcloud dataflow flex-template run "sqlserver-simple-$(date +%Y%m%d-%H%M%S)" \
  --template-file-gcs-location "gs://TU_BUCKET/templates/sqlserver_simple_query.json" \
  --region "us-central1" \
  --parameters project_id=TU_PROYECTO \
               sql_secret_id=tu-secret-sql \
               sql_query="SELECT TOP 10 * FROM dbo.MiTabla" \
               output_bq_table=TU_PROYECTO.mi_dataset.mi_tabla_destino
```

---

Resumiendo: sí, puedes correr en Dataflow sólo con Beam + `pyodbc`, sin Spark y sin Composer. Lo único “extra” es la imagen Docker con el driver ODBC. Si me dices qué quieres hacer exactamente con el resultado (log, BQ, GCS, sólo ejecutar un `EXEC`), te lo ajusto a tu caso exacto.
