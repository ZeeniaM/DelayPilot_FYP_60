import os
from typing import List

import numpy as np
import pandas as pd
from sqlalchemy import create_engine


def get_engine():
    pg_user = os.getenv("PG_USER", "postgres")
    pg_password = os.getenv("PG_PASSWORD", "delaypilot2026")
    pg_host = os.getenv("PG_HOST", "localhost")
    pg_port = os.getenv("PG_PORT", "5432")
    pg_db = os.getenv("PG_DB", "delaypilot_db")

    url = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    return create_engine(url)


def clean_training_features() -> None:
    """
    Basic cleaning for training_features_v3:
      - drop rows with nulls in key categorical/time features
      - drop rows with nulls in core weather fields
      - ensure numeric columns are numeric (coerce errors)
      - optionally clip extreme numeric values to reasonable bounds

    Result is written to: training_features_v3_clean
    """
    engine = get_engine()

    df = pd.read_sql("SELECT * FROM training_features_v3", engine)
    if df.empty:
        raise RuntimeError("training_features_v3 is empty; build it before cleaning.")

    original_rows = len(df)

    # 1) Drop rows with missing key categorical/time features
    required_non_null: List[str] = [
        "movement",
        "airline_iata",
        "other_airport_icao",
        "month",
        "dow",
        "hour",
        "minute",
        "is_weekend",
        "is_peak_wave",
    ]
    existing_required = [c for c in required_non_null if c in df.columns]
    df = df.dropna(subset=existing_required)

    # 2) Drop rows with missing core weather fields
    core_weather = [
        "temperature_2m",
        "wind_speed_10m",
        "precipitation",
        "cloud_cover",
        "surface_pressure",
    ]
    existing_weather = [c for c in core_weather if c in df.columns]
    if existing_weather:
        df = df.dropna(subset=existing_weather)

    # 3) Ensure numeric columns are numeric
    numeric_candidates = [
        "temperature_2m",
        "wind_speed_10m",
        "wind_direction_10m",
        "wind_gusts_10m",
        "snowfall",
        "rain",
        "precipitation",
        "snow_depth",
        "cloud_cover_low",
        "cloud_cover_mid",
        "cloud_cover_high",
        "cloud_cover",
        "surface_pressure",
        "weather_code",
        "relative_humidity_2m",
        "dew_point_2m",
        "apparent_temperature",
        "vapour_pressure_deficit",
        "tot_cnt",
        "dep_cnt",
        "arr_cnt",
        "tot_ifr_cnt",
        "dep_ifr_cnt",
        "arr_ifr_cnt",
        "atfm_arr_delay_min_total",
        "atfm_arr_delay_min_per_arrival",
        "arrivals_delayed_rate",
        "arrivals_delayed15_rate",
    ]
    for col in numeric_candidates:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows that became NaN in critical numeric fields after coercion
    if existing_weather:
        df = df.dropna(subset=existing_weather)

    # 4) Clip obvious ranges to avoid extreme outliers (simple, safe bounds)
    clip_bounds = {
        "temperature_2m": (-40, 50),
        "wind_speed_10m": (0, 60),
        "wind_gusts_10m": (0, 80),
        "precipitation": (0, 200),
        "rain": (0, 200),
        "snowfall": (0, 200),
        "relative_humidity_2m": (0, 100),
        "cloud_cover": (0, 100),
        "cloud_cover_low": (0, 100),
        "cloud_cover_mid": (0, 100),
        "cloud_cover_high": (0, 100),
    }
    for col, (low, high) in clip_bounds.items():
        if col in df.columns:
            df[col] = df[col].clip(lower=low, upper=high)

    cleaned_rows = len(df)
    print(f"training_features_v3: {original_rows} rows before cleaning, {cleaned_rows} after cleaning.")

    # 5) Write cleaned data back to Postgres
    df.to_sql(
        "training_features_v3_clean",
        engine,
        if_exists="replace",
        index=False,
    )

    print("Wrote cleaned features to table: training_features_v3_clean")


if __name__ == "__main__":
    clean_training_features()

