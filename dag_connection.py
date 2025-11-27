
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pymssql

# Función que se ejecutará en el DAG
def query_sql_server():
    # Conexión usando pymssql
    conn = pymssql.connect(
        server='fbtdw2090.qaamer.qacorp.xpo.com',
        user='svcGCPDataEngg',
        password='OXZ6q67wr77k',
        database='XpoMaster'
    )
    cursor = conn.cursor()
    cursor.execute("SELECT TOP (10) [OrderId] FROM [XpoMaster].[accounting].[Order];")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    conn.close()

# Definición del DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 11, 18),
    'retries': 1
}

with DAG(
    dag_id='sqlserver_connection_dag_pymssql',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    task_query = PythonOperator(
        task_id='query_sql_server_task',
        python_callable=query_sql_server
    )

