 File "/opt/python3.11/lib/python3.11/site-packages/airflow/utils/operator_helpers.py", line 252, in run
    return self.func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/gcs/dags/00_1_copnnect_sql_server.py", line 8, in query_sql_server
    conn = pyodbc.connect(
           ^^^^^^^^^^^^^^^
pyodbc.Error: ('01000', "[01000] [unixODBC][Driver Manager]Can't open lib 'ODBC Driver 17 for SQL Server' : file not found (0) (SQLDriverConnect)")
