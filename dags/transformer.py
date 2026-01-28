from datetime import datetime, timedelta
from config import *

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.providers.postgres.operators.postgres import PostgresOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
        'transformer',
        default_args=default_args,
        description='To transform the raw current weather to a modeled dataset',
        schedule_interval=timedelta(minutes=5),
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['take-home'],
) as dag:

    # @TODO: Fill in the below
    t1 = PostgresOperator(
    task_id="create_modeled_dataset_table",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="""
        CREATE TABLE IF NOT EXISTS current_weather (
               
                provider                 TEXT NOT NULL,
                provider_city_id         BIGINT NOT NULL,
                name                     TEXT,
                country                  TEXT,

                observation_time_utc     TIMESTAMPTZ NOT NULL,
                coord_lat                DOUBLE PRECISION,
                coord_lon                DOUBLE PRECISION,

                temp                     DOUBLE PRECISION,
                weather_description      TEXT,
                wind_speed               DOUBLE PRECISION,
                cloudiness_pct           INTEGER,

                ingested_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

                CONSTRAINT pk_current_weather PRIMARY KEY (provider, provider_city_id, observation_time_utc)
        );
    """,
    )

    # @TODO: Fill in the below
    t2 = PostgresOperator(
    task_id="transform_raw_into_modelled",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="""       
        INSERT INTO current_weather (
                provider,
                provider_city_id,
                name,
                country,
                observation_time_utc,
                coord_lat,
                coord_lon,
                temp,
                weather_description,
                wind_speed,
                cloudiness_pct,
                ingested_at
            )
            SELECT
                provider,
                provider_city_id,
                name,
                country,
                observation_time_utc,
                coord_lat,
                coord_lon,
                temp,
                weather_description,
                wind_speed,
                cloudiness_pct,
                ingested_at
            FROM raw_current_weather
            ORDER BY observation_time_utc DESC
            ON CONFLICT DO NOTHING; 
    """,
    )

    t1 >> t2