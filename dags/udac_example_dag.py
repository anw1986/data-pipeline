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
# http://michal.karzynski.pl/blog/2017/03/19/developing-workflows-with-apache-airflow/#:~:text=When%20a%20Task%20is%20executed,definition%20files%20and%20Airflow%20plugins

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')


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

create_table_all=PostgresOperator(
    task_id='redshift_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql='sql/create_tables.sql'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    dag=dag,
    table='staging_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend', 
    copy_sql=SqlQueries.copy_staging_songs,
    s3_key='log_data/2018/11/'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='staging_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    copy_sql=SqlQueries.copy_staging_songs,
    s3_key='song_data/A/'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='songplays',
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    sql_query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    sql_query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    sql_query=SqlQueries.artist_table_insert
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
start_operator>>create_table_all>>[stage_events_to_redshift,stage_songs_to_redshift]>>load_songplays_table
load_songplays_table>>[load_artist_dimension_table,load_song_dimension_table,load_time_dimension_table,load_user_dimension_table]>>run_quality_checks
run_quality_checks>>end_operator
