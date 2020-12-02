from airflow.hooks.base_hook import BaseHook
conn = BaseHook.get_connection('aws_credentials')
print(conn.get_extra())
