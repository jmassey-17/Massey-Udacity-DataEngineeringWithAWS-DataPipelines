from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements


default_args = {
    'owner': 'jamie-massey',
    'start_date': datetime(2023, 1, 1),
    # no dependencies on past runs
    'depends_on_past': False, 
    # 3 retries 
    'retries': 3, 
    # retures every 5 minutes 
    'retry_delay': timedelta(minutes = 5),
    # catch up off 
    'catchup': False,
    # no email on retry
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
    catchup = False
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        s3_path='s3://udacity-dend/log-data',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        json_file='s3://udacity-dend/log_json_path.json',
        region='us-west-2',
        
    )


    stage_songs_to_redshift = StageToRedshiftOperator(
                task_id='stage_songs',
                 table='staging_songs',
                 s3_path='s3://udacity-dend/song-data/A/A/A',
                 redshift_conn_id='redshift',
                 aws_credentials_id='aws_credentials',
                 json_file='auto',
                 region='us-west-2',
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table = 'songplays',
        sql = final_project_sql_statements.SqlQueries.songplay_table_insert,
        truncate = False
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        table = 'users',
        sql = final_project_sql_statements.SqlQueries.user_table_insert,
        truncate = False
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table = 'songs',
        sql = final_project_sql_statements.SqlQueries.song_table_insert,
        truncate = False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table = 'artists',
        sql = final_project_sql_statements.SqlQueries.artist_table_insert,
        truncate = False
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table = 'time',
        sql = final_project_sql_statements.SqlQueries.time_table_insert,
        truncate = False
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        tables = ['users', 'songs', 'artists','time'], 
        redshift_conn_id = 'redshift',
    )

    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table
    [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
    run_quality_checks >> end_operator
final_project_dag = final_project()