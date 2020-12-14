from airflow import DAG
from airflow.operators.sensors import S3KeySensor
from airflow.operators import (BashOperator,DummyOperator)
from datetime import datetime, timedelta

default_args = {
    'owner': 'sparkify',
    'start_date': datetime(2018, 11, 1),
    # 'start_date': datetime(2020, 12, 10),
    'depends_on_past':False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup':False,
    'email_on_rety': False
}

dag = DAG('using_s3_sensor',
        default_args=default_args,
        description='trying out missing s3 file',
        #   schedule_interval='0 * * * *'
        schedule_interval=timedelta(days=1)
)

start_operator=DummyOperator(task_id='Begin_execution',dag=dag)

create_table_all = DummyOperator(
    task_id='create_tables',
    # bash_command='creating tables in redshift',
    dag=dag,
)

load_songs_to_redshift = DummyOperator(
    task_id='stage_songs',
    # bash_command='load songs in the staging table',
    dag=dag,
)

s3_file = S3KeySensor(
    task_id='s3_file_check',
    bucket_key='s3://udacity-dend/log_data/{{macros.ds_format(ds,"%Y-%m-%d","%Y")}}/{{macros.ds_format(ds,"%Y-%m-%d","%m")}}/{{ds}}-events.json',
    bucket_name=None,
    aws_conn_id='aws_credentials',
    poke_interval=3,
    timeout=10,
    soft_fail=True,
    dag=dag)

stage_events_to_redshift = DummyOperator(
    task_id='staging_events',
    # bash_command='load events in the staging table {{ds}}',
    dag=dag,
)

load_songplays_table=DummyOperator(
    task_id='Load_songplays_fact_table',
    trigger_rule='none_failed',
    # bash_command='loading fact tables',
    dag=dag,
)
t1=DummyOperator(
    task_id='load_song_dim_table',
    trigger_rule='all_done',
    dag=dag
)
t2=DummyOperator(
    task_id='load_artist_dim_table',
    trigger_rule='all_done',
    dag=dag
)

end_operator=DummyOperator(task_id='End_execution', trigger_rule='all_done',dag=dag)

start_operator>>create_table_all
create_table_all>>load_songs_to_redshift>>load_songplays_table
create_table_all>>s3_file>>stage_events_to_redshift>>load_songplays_table
load_songplays_table>>[t1,t2]>>end_operator