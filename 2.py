from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.utils.dates import days_ago
from airflow import DAG
import datetime

# Supongo que ya tienes algo así en tu DAG:
# from include.utils.pipe_config_loader import PipeConfigLoader
# config_loader = PipeConfigLoader()
# pipe_params, env_config = config_loader.load_configurations(TABLE_TYPE)

with DAG(
    dag_id="sqlserver_to_bq_via_dataflow",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    run_sqlserver_query = DataflowStartFlexTemplateOperator(
        task_id="run_sqlserver_query",
        project_id=env_config["gcp_project_id"],
        location=env_config["dataflow_region"],
        gcp_conn_id=env_config["gcp_conn_id"],  # si lo usas
        body={
            "launchParameter": {
                "jobName": "sqlserver-to-bq-"
                + datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S"),

                # Flex template que ya tienes en GCS
                "containerSpecGcsPath": env_config["dataflow_flex_template_sql"],

                # === AQUÍ VAN EL SERVICE ACCOUNT Y OTRAS VARIABLES DE ENTORNO ===
                "environment": {
                    "serviceAccountEmail": env_config["dataflow_service_account"],
                    "subnetwork": env_config["dataflow_subnetwork"],
                    "tempLocation": env_config["dataflow_temp_location"],
                    "stagingLocation": env_config["dataflow_staging_location"],
                    "ipConfiguration": "WORKER_IP_PRIVATE",
                    # opcional, si lo usas en otros DAGs:
                    # "additionalUserLabels": {"pipeline": "sqlserver_to_bq"},
                },

                # === PARÁMETROS QUE USA TU FLEX TEMPLATE PARA CONECTAR A SQL ===
                "parameters": {
                    # conexión a SQL Server (ajusta nombres a tu template real)
                    "server_name": "fbtdw2090.qaamer.qacorp.xpo.com",
                    "database_name": "XpoMaster",
                    "schema_name": "carrier",

                    # query a ejecutar en SQL Server
                    "sql_query": """
                        SELECT
                            AccessControlTypeId,
                            Code,
                            AccessControlName,
                            Description,
                            SortOrder,
                            IsActive,
                            CreatedBy,
                            CreatedOn,
                            UpdatedBy,
                            UpdatedOn
                        FROM carrier.AccessControlType
                    """,

                    # dónde dejar el resultado (puede ser GCS o BQ según tu template)
                    "output_table": (
                        "rxo-dataeng-datalake-np.sqlserver_to_bq_silver."
                        "xpomaster_carrier_accesscontroltype"
                    ),

                    # si tu template requiere estos:
                    "gcs_temp_location": env_config["dataflow_temp_location"],
                    "gcs_staging_location": env_config["dataflow_staging_location"],
                },
            }
        },
    )