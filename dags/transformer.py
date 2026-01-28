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
            provider_city_id         BIGINT NOT NULL,
            name                     TEXT,
            country                  TEXT,
            coord_lat                DOUBLE PRECISION,
            coord_lon                DOUBLE PRECISION,
            observation_time_utc     TIMESTAMPTZ NOT NULL,
            timezone_offset_seconds  INTEGER,
            weather_id               INTEGER,
            weather_group            TEXT,
            weather_description      TEXT,
            weather_icon             TEXT,
            temp                     DOUBLE PRECISION,
            feels_like               DOUBLE PRECISION,
            temp_min                 DOUBLE PRECISION,
            temp_max                 DOUBLE PRECISION,
            pressure_hpa             INTEGER,
            humidity_pct             INTEGER,
            wind_speed               DOUBLE PRECISION,
            wind_deg                 INTEGER,
            wind_gust                DOUBLE PRECISION,
            cloudiness_pct           INTEGER,
            precip_mm_last_1h        DOUBLE PRECISION,
            precip_mm_last_3h        DOUBLE PRECISION,
            sunrise_time_utc         TIMESTAMPTZ,
            sunset_time_utc          TIMESTAMPTZ,
            ingested_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_city_observation_model UNIQUE (provider_city_id, observation_time_utc)
        );
    """,
    )

    # @TODO: Fill in the below
    t2 = PostgresOperator(
    task_id="transform_raw_into_modelled",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="""
        INSERT INTO current_weather (
            provider_city_id, name, country,
            coord_lat, coord_lon,
            observation_time_utc, timezone_offset_seconds,
            weather_id, weather_group, weather_description, weather_icon,
            temp, feels_like, temp_min, temp_max, pressure_hpa, humidity_pct,
            wind_speed, wind_deg, wind_gust,
            cloudiness_pct, precip_mm_last_1h, precip_mm_last_3h,
            sunrise_time_utc, sunset_time_utc
        )
        SELECT
            provider_city_id,
            name,
            country,
            coord_lat,
            coord_lon,
            observation_time_utc,
            timezone_offset_seconds,
            weather_id,
            weather_group,
            weather_description,
            weather_icon,
            temp,
            feels_like,
            temp_min,
            temp_max,
            pressure_hpa,
            humidity_pct,
            wind_speed,
            wind_deg,
            wind_gust,
            cloudiness_pct,
            precip_mm_last_1h,
            precip_mm_last_3h,
            sunrise_time_utc,
            sunset_time_utc
        FROM raw_current_weather
        ORDER BY observation_time_utc DESC
        ON CONFLICT ON CONSTRAINT uq_city_observation_model DO NOTHING;
    """,
    )

    t1 >> t2