from datetime import datetime, timedelta
import os
from airflow import DAG
# from airflow.contrib.hooks.aws_hook import AwsHook
# from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, 
                                PythonOperator)
from helpers import SqlQueries
# http://michal.karzynski.pl/blog/2017/03/19/developing-workflows-with-apache-airflow/#:~:text=When%20a%20Task%20is%20executed,definition%20files%20and%20Airflow%20plugins

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
# able to share folder inside the container - check 2

'''
DAG default parameters:
    The DAG does not have dependencies on past runs
    On failure, the task are retried 3 times
    Retries happen every 5 minutes
    Catchup is turned off
    Do not email on retry
'''

default_args = {
    'owner': 'sparkify',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past':False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup':False,
    'email_on_rety': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

dag_test=DAG('udac_example_dag',
        default_args=default_args,
        description='create table in redshift',
        schedule_interval='0 * * * *'
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# create all tables in the database
# def create_table(*args,**kwargs):
#     aws_hook=AwsHook('aws_credentials')
#     credentials=aws_hook.get_credentials()
#     redshift=PostgresHook('redshift')
#     log.info('create table')
#     query_run_table="""
#         CREATE TABLE employee(
#         emp_name varchar(256),
#         emp_dept varchar(256)
#     );
#     """
#     redshift.run(query_run_table)

# create_table_redshift=PostgresOperato(
#     task_id='create_table',
#     dag=dag_test,
#     # redshift_conn_id = "redshift",
#     # aws_credentials_id="aws_credentials"
# )

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Relations between tasks
# see the bitshit operations in the documention https://airflow.apache.org/docs/stable/concepts.html?highlight=hook#additional-functionality
start_operator>>[stage_events_to_redshift,stage_songs_to_redshift]>>load_songplays_table
load_songplays_table>>[load_artist_dimension_table,load_song_dimension_table,load_time_dimension_table,load_user_dimension_table]>>run_quality_checks
run_quality_checks>>end_operator
