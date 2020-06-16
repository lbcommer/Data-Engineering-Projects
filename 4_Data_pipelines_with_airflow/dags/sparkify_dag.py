from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'lbcommer',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'schedule_interval': '@hourly'
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='ETL to load Sparkify data into Redshift'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

s3_bucket = "udacity-dend"
s3_region = "us-west-2"
redshift_conn = "redshift"
aws_credentials= "aws_credentials"

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id=redshift_conn,
    aws_credentials_id=aws_credentials,
    table="staging_events",
    s3_bucket=s3_bucket,
    s3_key="log_data",
    s3_region=s3_region,
    file_format="s3://udacity-dend/log_json_path.json",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id=redshift_conn,
    aws_credentials_id=aws_credentials,
    table='staging_songs',
    s3_bucket=s3_bucket,
    s3_key="song_data",  
    s3_region=s3_region,
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id = redshift_conn,
    table = 'songplays',
    sql_query = SqlQueries.songplay_table_insert,    
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id = redshift_conn,
    table = 'users',
    sql_query = SqlQueries.user_table_insert,     
    truncate = True,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id = redshift_conn,
    table = 'songs',
    sql_query = SqlQueries.song_table_insert,     
    truncate = True,    
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id = redshift_conn,
    table = 'artists',
    sql_query = SqlQueries.artist_table_insert,     
    truncate = True,      
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id = redshift_conn,
    table = 'time',
    sql_query = SqlQueries.time_table_insert,     
    truncate = True,      
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id=redshift_conn,
    checks=[
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songplays WHERE playid is null", 'expected_result': 0},
    ],   
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



start_operator >> [stage_songs_to_redshift, stage_events_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
[load_user_dimension_table, load_song_dimension_table, 
 load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator

