import os
from typing import List

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


FEATURE_COLS_V3: List[str] = [
    "movement",
    "airline_iata",
    "airline_icao",
    "other_airport_icao",
    "codeshare_status",
    "is_cargo",
    "aircraft_model",
    "month",
    "dow",
    "hour",
    "minute",
    "is_weekend",
    "is_peak_wave",
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
    # Congestion / network metrics are optional for now; they may not exist yet
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


def build_features(max_rows: int = 50_000) -> None:
    """
    Build a training/serving features table by joining flights_raw with weather_hourly
    and engineering the time-based columns required by feature_list_v3.

    Result is written to Postgres table: training_features_v3
    """
    engine = get_engine()

    # Read flights and weather from Postgres
    flights = pd.read_sql("SELECT * FROM flights_raw", engine)
    weather = pd.read_sql("SELECT * FROM weather_hourly", engine)

    if flights.empty:
        raise RuntimeError("flights_raw is empty; ingest flights before building features.")
    if weather.empty:
        raise RuntimeError("weather_hourly is empty; ingest weather before building features.")

    # Identify the scheduled time column for flights
    # Based on flights_raw columns, we prioritise dep/arr scheduled times.
    time_col_candidates = [
        "dep_sched_utc",
        "arr_sched_utc",
        "dep_best_utc",
        "arr_best_utc",
    ]
    time_col = None
    for c in time_col_candidates:
        if c in flights.columns:
            time_col = c
            break
    if time_col is None:
        raise KeyError(
            f"Could not find a scheduled time column in flights_raw. "
            f"Checked: {', '.join(time_col_candidates)}. "
            f"Existing columns: {list(flights.columns)}"
        )

    # Parse scheduled time to datetime and derive time-based features
    flights[time_col] = pd.to_datetime(flights[time_col], utc=True, errors="coerce")
    flights = flights.dropna(subset=[time_col])

    flights["month"] = flights[time_col].dt.month
    flights["dow"] = flights[time_col].dt.dayofweek
    flights["hour"] = flights[time_col].dt.hour
    flights["minute"] = flights[time_col].dt.minute
    flights["is_weekend"] = flights["dow"].isin([5, 6]).astype(int)

    # Simple peak wave heuristic: morning 6–10 and evening 16–20
    flights["is_peak_wave"] = flights["hour"].isin(list(range(6, 11)) + list(range(16, 21))).astype(int)

    # Ensure we have an airport_icao column to join with weather.
    # The Aerodatabox FIDS data has airport_iata (e.g. 'MUC') for Munich.
    # We map MUC -> EDDM as ICAO for Munich; this is sufficient for joining
    # to the weather table which uses airport_icao.
    if "airport_icao" not in flights.columns:
        if "airport_iata" in flights.columns:
            flights["airport_icao"] = flights["airport_iata"].map(
                lambda x: "EDDM" if str(x).upper() == "MUC" else None
            )
        else:
            raise KeyError(
                "Expected 'airport_iata' or 'airport_icao' column in flights_raw for joining with weather_hourly."
            )

    # Floor to the hour for joining with hourly weather (use lowercase 'h' for this pandas version)
    flights["hour_utc"] = flights[time_col].dt.floor("h")

    # Ensure weather hour_utc is datetime
    weather["hour_utc"] = pd.to_datetime(weather["hour_utc"], utc=True, errors="coerce")
    weather = weather.dropna(subset=["hour_utc"])

    # Join flights with weather
    merged = flights.merge(
        weather,
        on=["airport_icao", "hour_utc"],
        how="left",
        suffixes=("", "_wx"),
    )

    # Clip to at most max_rows (we already sampled to 50k during ingestion, but keep guard)
    if len(merged) > max_rows:
        merged = merged.sample(n=max_rows, random_state=42)

    # Only keep columns that actually exist; some optional congestion metrics may not be present yet.
    available_features = [c for c in FEATURE_COLS_V3 if c in merged.columns]
    if not available_features:
        raise KeyError(
            "No expected feature columns found in merged dataframe. "
            f"Available columns: {list(merged.columns)}"
        )

    # For now, keep only feature columns. Labels can be added later if needed.
    feature_df = merged[available_features].copy()

    # Write to Postgres
    feature_df.to_sql(
        "training_features_v3",
        engine,
        if_exists="replace",
        index=False,
    )

    print(f"Built training_features_v3 with {len(feature_df)} rows.")


if __name__ == "__main__":
    build_features()

