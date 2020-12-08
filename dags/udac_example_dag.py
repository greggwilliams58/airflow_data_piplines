from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import ( StageToRedshiftOperator,LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,PostgresOperator)
from helpers import SqlQueries

"""
This is the central orchestration code for this data pipeline.  It defines the DAG, and calls the operators to perform the following tasks
1) Start Operator - a dummy operator is used to to log the start of the pipeline
2) Create Tables - a postgres operators is used to create the required tables in Redshift
3) stage_events and stage_songs - a custom StagetoRedshift Operator is used to load data from Udacity S3 buckets into staging_events and staging_songs tables
4) load_songplays_table - a custom LoadFactTable Operator is used to load data from staging table into fact table
5) load_x_ table - a custom LoadDimensionTable operator is used to load data from staging tables into 4 dimension tables
6) run_quality_checsk - a custom DataQualityCheck Operator is used to check that no NULL values are present in the id field of the songs_table
7) End Operator - a dummy operator is used to log the end of the pipeline
"""

#definition of default arguements for DAG
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past':False,
    'retries':3,
    'retry_delay':timedelta(minutes=5),
    'email_on_retry':False,
    'catchup':False
}

#Dag is defined here using the default arguments above
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id="create_postgres_tables",
    dag=dag,
    postgres_conn_id ="redshift",
    sql="create_tables.sql")


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    copy_json_option='s3://udacity-dend/log_json_path.json',
    region='us-west-2',
    dag=dag
    )

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    copy_json_option='auto',
    region='us-west-2',
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id = 'redshift',
    table='songplays',
    select_sql=SqlQueries.songplay_table_insert,
    dag=dag,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id = 'redshift',
    table = 'users',
    select_sql = SqlQueries.user_table_insert,
    dag=dag,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id = 'redshift',
    table='songs',
    select_sql=SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id = 'redshift',
    table='artists',
    select_sql=SqlQueries.artist_table_insert,
    append_insert=True,
    primary_key="artistid",
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id = 'redshift',
    table='time',
    select_sql =SqlQueries.time_table_insert,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id = 'redshift',
    test_query= 'SELECT COUNT(*) FROM songs where songid is NULL;',
    expected_result =0,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# Order of task operation is defined here
start_operator >> create_tables
create_tables >> stage_events_to_redshift
create_tables >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks 
load_song_dimension_table >> run_quality_checks 
load_artist_dimension_table >> run_quality_checks 
load_time_dimension_table >> run_quality_checks 

run_quality_checks  >> end_operator
