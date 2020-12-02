from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, 
                                PythonOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'sparkify',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past':False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup':False,
    'email_on_rety': False
}

dag = DAG('create_table_redshift',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
)

create_table=PostgresOperator(
    task_id='redshift_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql="""
        CREATE TABLE IF NOT EXIST employee(
        emp_name varchar,
        emp_dept varchar
    );
    """
)
