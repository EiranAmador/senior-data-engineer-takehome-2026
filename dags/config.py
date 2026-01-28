"""
Configuration file for the solution.

Provides values used across the solution to standardize sources and simplify updates.
"""

import os
from urllib.parse import urlencode
from airflow.models import Variable
from typing import List, Dict, Optional
from datetime import datetime, timezone

CITIES: List[Dict] = [
    {
        "name": "San José",
        "country": "CR",
        "lat": 9.9281,
        "lon": -84.0907,
    },
    {
        "name": "Ann Arbor",
        "country": "US-MI",
        "lat": 42.2808,
        "lon": -83.7430,
    },
    {
        "name": "Tokyo",
        "country": "JP",
        "lat": 35.6762,
        "lon": 139.6503,
    },

    {
        "name": "York",
        "country": "GB",
        "lat": 53.959965,
        "lon": -1.087298,
    }
]

# Standard, metric and imperial units are available. (https://openweathermap.org/current?collection=current_forecast&collection=current_forecast&collection=current_forecast#geo)
WEATHER_UNITS: Optional[str] = None

# Full list (https://openweathermap.org/current?collection=current_forecast&collection=current_forecast&collection=current_forecast#multi)
LANGUAGE: Optional[str] = None

OPENWEATHER_API_KEY = Variable.get("OPENWEATHER_API_KEY", "API_KEY")

OPENWEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

POSTGRES_CONN_ID = "postgres_conn"

# Helper function that returns a full URL for the OpenWeather API
def build_weather_url(lat: float, lon: float, units: str = "", lang: str = "") -> str:
    if not OPENWEATHER_API_KEY:
        raise RuntimeError(
            "Unable to obtain OPENWEATHER API key. Varify configuration file and/or Airflow variable configuration."
        )     

    params = {
        "lat": lat,
        "lon": lon,
        "units": units,
        "lang": lang,
        "appid": OPENWEATHER_API_KEY
    }

    return f"{OPENWEATHER_BASE_URL}?{urlencode(params)}"

# Helper function that ormalizes ntimestamps to UTC
def to_utc_iso(ts):
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
