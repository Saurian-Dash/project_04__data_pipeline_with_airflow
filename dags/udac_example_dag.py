from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.udacity_plugin import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries


default_args = {
    'owner': 'sparkify',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 30),
    'depends_on_past': False,
    'catchup': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}

dag_name = 'sparkify_songplays_model'

with DAG(dag_name,
         default_args=default_args,
         description='Load and transform data in Redshift with Airflow',
         schedule_interval='@hourly',
         max_active_runs=1
         ) as dag:

    start_operator = DummyOperator(task_id='execution_started')
    schema_operator = DummyOperator(task_id='db_schema_created')
    staging_operator = DummyOperator(task_id='data_staging_completed')
    stop_operator = DummyOperator(task_id='stop_execution')

    create_artists_table = PostgresOperator(
        task_id='create_artists_table',
        postgres_conn_id='redshift',
        sql=SqlQueries.artists_table_create
    )

    create_songplays_table = PostgresOperator(
        task_id='create_songplays_table',
        postgres_conn_id='redshift',
        sql=SqlQueries.songplays_table_create
    )

    create_songs_table = PostgresOperator(
        task_id='create_songs_table',
        postgres_conn_id='redshift',
        sql=SqlQueries.songs_table_create
    )

    create_staging_events_table = PostgresOperator(
        task_id='create_staging_events_table',
        postgres_conn_id='redshift',
        sql=SqlQueries.staging_events_table_create
    )

    create_staging_songs_table = PostgresOperator(
        task_id='create_staging_songs_table',
        postgres_conn_id='redshift',
        sql=SqlQueries.staging_songs_table_create
    )

    create_time_table = PostgresOperator(
        task_id='create_time_table',
        postgres_conn_id='redshift',
        sql=SqlQueries.time_table_create
    )

    create_users_table = PostgresOperator(
        task_id='create_users_table',
        postgres_conn_id='redshift',
        sql=SqlQueries.users_table_create
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='stage_events_data',
        schema='public',
        table='staging_events',
        s3_bucket='udacity-dend',
        s3_key=(
            'log_data/{execution_date.year}/'
            '{execution_date.month}/{ds}-events.json'
        ),
        jsonpaths='s3://udacity-dend/log_json_path.json',
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='stage_songs_data',
        schema='public',
        table='staging_songs',
        s3_bucket='udacity-dend',
        s3_key=('song_data'),
    )

    load_songplays_table = LoadFactOperator(
        task_id='load_songplays_fact_table',
        schema='public',
        table='songplays',
        sql=SqlQueries.songplay_table_insert,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='load_user_dim_table',
        schema='public',
        table='users',
        sql=SqlQueries.user_table_insert,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='load_song_dim_table',
        schema='public',
        table='songs',
        sql=SqlQueries.song_table_insert,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='load_artist_dim_table',
        schema='public',
        table='artists',
        sql=SqlQueries.artist_table_insert,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        schema='public',
        table='time',
        sql=SqlQueries.time_table_insert,
    )

    run_quality_checks = DataQualityOperator(
        task_id='run_data_quality_checks',
        schema='public',
        tables=[
            'artists',
            'songs',
            'time',
            'users',
            'songplays'
        ]
    )

# Task lists
create_tables = [
    create_artists_table,
    create_songplays_table,
    create_songs_table,
    create_staging_events_table,
    create_staging_songs_table,
    create_time_table,
    create_users_table,
]

stage_data = [
    stage_songs_to_redshift,
    stage_events_to_redshift,
]

load_dimensions = [
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table,
]

# DAG dependencies
start_operator >> create_tables >> schema_operator
schema_operator >> stage_data >> staging_operator
staging_operator >> load_songplays_table >> load_dimensions
load_dimensions >> run_quality_checks >> stop_operator
