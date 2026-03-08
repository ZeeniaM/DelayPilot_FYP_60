import logging
import os
from typing import List

import pandas as pd
import requests
from sqlalchemy import create_engine


logger = logging.getLogger(__name__)


OPENMETEO_URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=48.3538,50.0267,51.4707"
    "&longitude=11.7861,8.5584,-0.4599"
    "&hourly="
    "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,"
    "snowfall,snow_depth,rain,weather_code,surface_pressure,cloud_cover,"
    "cloud_cover_low,cloud_cover_mid,cloud_cover_high,visibility,"
    "vapour_pressure_deficit,wind_speed_10m,wind_direction_10m,wind_gusts_10m,"
    "is_day,dew_point_2m,wet_bulb_temperature_2m,boundary_layer_height,"
    "sunshine_duration"
    "&timezone=GMT,GMT,GMT"
)

# Order of coordinates in OPENMETEO_URL -> corresponding ICAO codes
OPENMETEO_ICAOS: List[str] = ["EDDM", "EDDF", "EGLL"]


def get_engine():
    pg_user = os.getenv("PG_USER", "postgres")
    pg_password = os.getenv("PG_PASSWORD", "delaypilot2026")
    pg_host = os.getenv("PG_HOST", "localhost")
    pg_port = os.getenv("PG_PORT", "5432")
    pg_db = os.getenv("PG_DB", "delaypilot_db")
    url = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    return create_engine(url)


def fetch_openmeteo_multi() -> pd.DataFrame:
    """
    Fetch live hourly weather for EDDM/EDDF/EGLL from Open-Meteo and
    normalize into a DataFrame compatible with `weather_hourly`.
    """
    logger.info("Requesting live weather from Open-Meteo")
    resp = requests.get(OPENMETEO_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # API returns a list of locations
    if not isinstance(data, list):
        logger.error("Unexpected Open-Meteo response format; expected a list, got %s", type(data))
        raise ValueError("Unexpected Open-Meteo response format; expected a list.")

    frames = []
    for idx, loc in enumerate(data):
        hourly = loc.get("hourly", {})
        times = hourly.get("time", [])
        if not times:
            continue

        df = pd.DataFrame(hourly)
        df["hour_utc"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
        df = df.drop(columns=["time"])

        icao = OPENMETEO_ICAOS[idx] if idx < len(OPENMETEO_ICAOS) else None
        df["airport_icao"] = icao

        frames.append(df)

    if not frames:
        logger.error("No hourly data returned from Open-Meteo.")
        raise ValueError("No hourly data returned from Open-Meteo.")

    out = pd.concat(frames, ignore_index=True)
    # Reorder columns: key first
    cols = ["airport_icao", "hour_utc"] + [c for c in out.columns if c not in ("airport_icao", "hour_utc")]
    out = out[cols]
    return out


def update_weather_live() -> None:
    """
    Call Open-Meteo and upsert the returned horizon of weather into
    the `weather_hourly` table (replacing any overlapping rows).
    """
    engine = get_engine()
    df = fetch_openmeteo_multi()

    # For simplicity we overwrite the table with the latest horizon.
    # If you want full history + forecast, you can switch to merge/upsert logic.
    df.to_sql(
        "weather_hourly",
        engine,
        if_exists="replace",
        index=False,
    )
    logger.info("Updated weather_hourly from Open-Meteo with %d rows.", len(df))


if __name__ == "__main__":
    update_weather_live()