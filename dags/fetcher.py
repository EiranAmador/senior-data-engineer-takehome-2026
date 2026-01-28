from datetime import datetime, timedelta, timezone
import time
from config import *
import requests
import json


# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
        'fetcher',
        default_args=default_args,
        description='To fetch the weather data',
        schedule_interval=timedelta(minutes=5),
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['take-home'],
) as dag:

    def normalize_openweather_data(data: dict) -> dict:
        # Normalize timestamps to UTC ISO
        timestamp_utc = to_utc_iso(data["dt"])
        
        sys_info = data.get("sys", {}) or {}
        sunrise_utc = to_utc_iso(sys_info["sunrise"]) if "sunrise" in sys_info else None
        sunset_utc  = to_utc_iso(sys_info["sunset"])  if "sunset"  in sys_info else None

        weather = (data["weather"][0] if data.get("weather") else {}) or {}
        wind     = data.get("wind", {}) or {}
        clouds   = data.get("clouds", {}) or {}
        rain     = data.get("rain", {}) or {}
        snow     = data.get("snow", {}) or {}

        normalized = {
            "provider": "openweather",
            "provider_city_id": data.get("id"),
            "name": data.get("name"),
            "country": sys_info.get("country"),
            "coord": {
                "lat": data["coord"].get("lat"),
                "lon": data["coord"].get("lon"),
            },
            "observation_time_utc": timestamp_utc,
            "timezone_offset_seconds": data.get("timezone"), 
            "weather": {
                "id": weather.get("id"),
                "group": weather.get("main"),         
                "description": weather.get("description"),
                "icon": weather.get("icon"),
            },
            "main": {
                "temp": data["main"].get("temp"),
                "feels_like": data["main"].get("feels_like"),
                "temp_min": data["main"].get("temp_min"),
                "temp_max": data["main"].get("temp_max"),
                "pressure_hpa": data["main"].get("pressure"),
                "humidity_pct": data["main"].get("humidity"),
            },
            "wind": {
                "speed": wind.get("speed"),
                "deg": wind.get("deg"),
                "gust": wind.get("gust"),
            },
            "cloudiness_pct": clouds.get("all"),
            "precip_mm_last_1h": (
                rain.get("1h")
                or snow.get("1h")
                or None
            ),
            "precip_mm_last_3h": (
                rain.get("3h")
                or snow.get("3h")
                or None
            ),

            # Solar (if available)
            "sunrise_time_utc": sunrise_utc,
            "sunset_time_utc": sunset_utc,

        }

        return normalized
    
    # @TODO: Add your function here. Example here: https://airflow.apache.org/docs/apache-airflow/stable/_modules/airflow/example_dags/example_python_operator.html
    # Hint: How to fetch the weather data from OpenWeatherMap?
    def fetch_weather_data(lat: float, lon: float, units: str = "", lang: str = ""): 
        # Uses OpenWeather API to obtain raw weather data.
        # The data is normalized and returned to be used on other steps with XCOM.   
        url = build_weather_url(lat=lat, lon=lon, units=units, lang=lang)
        resp = requests.get(url, timeout=15)
        data = resp.json()
        # Validate keys are present on response. These keys can vary depending on specific concerns. 
        for k in ("coord", "weather", "main", "dt", "id"):
            if k not in data:
                raise ValueError(f"Missing expected key {k} in API response. Request: {url}. Response: {data}")
        
        normalized = normalize_openweather_data(data)
        normalized["raw_json"] = data
        return normalized

    def fetch_weather_data_by_cities(): 
        results = []
        
        if not CITIES:
            raise RuntimeError("No cities found. Verify and populate configuration file.")

        for city in CITIES:
            lat=city["lat"]
            lon=city["lon"]
            city_data = fetch_weather_data(lat, lon, WEATHER_UNITS, LANGUAGE)
            results.append(city_data)
        
        return results

    t1 = PythonOperator(
        task_id='ingest_api_data',
        python_callable=fetch_weather_data_by_cities
    )

    # @TODO: Fill in the below
    t2 = PostgresOperator(
        task_id="create_raw_dataset",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS raw_current_weather (
            provider                 TEXT NOT NULL,
            provider_city_id         BIGINT,
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

            raw_json                 JSONB NOT NULL,
            ingested_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

            -- Avoid duplicates for the same city & observation timestamp
            CONSTRAINT uq_city_observation UNIQUE (provider_city_id, observation_time_utc)
        );
        """,
    )
    
    # Read XCom from t1 and insert rows
    def store_dataset(**context):
        rows = context["ti"].xcom_pull(task_ids="ingest_api_data")
        if not rows:
            raise RuntimeError("No data returned from fetch_weather_data_by_cities")

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        insert_sql = """
            INSERT INTO raw_current_weather (
                provider, provider_city_id, name, country,
                coord_lat, coord_lon,
                observation_time_utc, timezone_offset_seconds,
                weather_id, weather_group, weather_description, weather_icon,
                temp, feels_like, temp_min, temp_max, pressure_hpa, humidity_pct,
                wind_speed, wind_deg, wind_gust,
                cloudiness_pct, precip_mm_last_1h, precip_mm_last_3h,
                sunrise_time_utc, sunset_time_utc,
                raw_json
            ) VALUES (
                %(provider)s, %(provider_city_id)s, %(name)s, %(country)s,
                %(coord_lat)s, %(coord_lon)s,
                %(observation_time_utc)s, %(timezone_offset_seconds)s,
                %(weather_id)s, %(weather_group)s, %(weather_description)s, %(weather_icon)s,
                %(temp)s, %(feels_like)s, %(temp_min)s, %(temp_max)s, %(pressure_hpa)s, %(humidity_pct)s,
                %(wind_speed)s, %(wind_deg)s, %(wind_gust)s,
                %(cloudiness_pct)s, %(precip_mm_last_1h)s, %(precip_mm_last_3h)s,
                %(sunrise_time_utc)s, %(sunset_time_utc)s,
                %(raw_json)s
            )
            ON CONFLICT ON CONSTRAINT uq_city_observation DO NOTHING;
        """
        for payload in rows:
            hook.run(insert_sql, parameters=payload)

    # @TODO: Fill in the below
    t3 = PythonOperator(
        task_id="store_dataset",
        python_callable=store_dataset,
        provide_context=True,
    )

    t1 >> t2 >> t3