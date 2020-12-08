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

dag = DAG('test_create_table_redshift',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
)

create_table_employee=PostgresOperator(
    task_id='redshift_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql='test_sql.sql'
)

dag_2 = DAG('test_car_table_redshift',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
)

create_table_car=PostgresOperator(
    task_id='redshift_table',
    dag=dag_2,
    postgres_conn_id='redshift',
    sql="""
        CREATE TABLE IF NOT EXISTS car(
            car_id VARCHAR,
            car_name VARCHAR
        )
    """
)

dag_3=DAG(
    'insert_into_redshift',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)

start_operator=DummyOperator(task_id='Begin_execution',dag=dag_3)

create_table_all=PostgresOperator(
    task_id='redshift_table',
    dag=dag_3,
    postgres_conn_id='redshift',
    sql='sql/create_tables.sql'
)

load_songs_to_redshift=StageToRedshiftOperator(
    task_id='stage_songs',
    dag=dag_3,
    table='staging_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend', 
    copy_sql=SqlQueries.copy_staging_songs,
    s3_key='song_data/A/'
)
# s3://udacity-dend/log_data/2018/11/
# log_data/2018/11/2018-11-01-events.json
# log_data/{{year}}/{{month}}/{{yyyy-mm-dd}}-events.json
# LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
stage_events_to_redshift=StageToRedshiftOperator(
    task_id='staging_events',
    dag=dag_3,
    table='staging_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    copy_sql=SqlQueries.copy_staging_events,
    params={'log_path':'s3://udacity-dend/log_json_path.json'},
    s3_key='log_data/{{macros.ds_format(ds,"%Y-%m-%d","%Y")}}/{{macros.ds_format(ds,"%Y-%m-%d","%m")}}/{{ds}}-events.json'
)

end_operator=DummyOperator(task_id='End_execution', dag=dag_3)

start_operator>>create_table_all>>[load_songs_to_redshift,stage_events_to_redshift]>>end_operator