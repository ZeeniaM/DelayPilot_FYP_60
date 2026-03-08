"""
Transform stage for DelayPilot (SRS Module 1: FE-4, FE-5).
Clean and prepare data for analysis and ML.
"""
import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


# Columns expected for cleaned_flights (must exist and be non-null where required)
REQUIRED_FLIGHT_COLUMNS = [
    "flight_number",
    "origin_airport",
    "destination_airport",
    "scheduled_departure_utc",
    "scheduled_arrival_utc",
]
OPTIONAL_FLIGHT_COLUMNS = [
    "source_id",
    "airline_code",
    "actual_departure_utc",
    "actual_arrival_utc",
    "status",
    "delay_minutes",
]

REQUIRED_WEATHER_COLUMNS = ["location_code", "recorded_at"]
OPTIONAL_WEATHER_COLUMNS = [
    "temperature_celsius",
    "humidity_pct",
    "visibility_km",
    "wind_speed_kmh",
    "conditions",
]


def _parse_dt(series: pd.Series) -> pd.Series:
    """Parse datetime column; invalid -> NaT."""
    return pd.to_datetime(series, errors="coerce")


def transform_flights(df: pd.DataFrame) -> pd.DataFrame:
    """
    FE-4: Clean missing, inconsistent, or duplicate flight records.
    FE-5: Prepare for analysis and ML.
    """
    if df.empty:
        return df

    out = df.copy()

    # Normalize string columns
    for col in ["flight_number", "airline_code", "origin_airport", "destination_airport", "status"]:
        if col in out.columns:
            out[col] = out[col].astype(str).str.strip().replace("nan", None)

    # Datetime columns
    for col in [
        "scheduled_departure_utc",
        "scheduled_arrival_utc",
        "actual_departure_utc",
        "actual_arrival_utc",
    ]:
        if col in out.columns:
            out[col] = _parse_dt(out[col])

    # Drop rows missing required fields (Munich-relevant: origin or destination EDDM)
    for col in REQUIRED_FLIGHT_COLUMNS:
        if col in out.columns:
            out = out.dropna(subset=[col])
        else:
            logger.warning("Missing required flight column: %s", col)
            return pd.DataFrame()

    # Drop duplicates (FE-4)
    if "source_id" not in out.columns or out["source_id"].isna().all():
        out["source_id"] = out.apply(
            lambda r: f"{r.get('flight_number', '')}_{r.get('scheduled_departure_utc', '')}_{r.get('origin_airport', '')}_{r.get('destination_airport', '')}",
            axis=1,
        )
    out = out.drop_duplicates(subset=["source_id"], keep="last")

    # Ensure delay_minutes numeric
    if "delay_minutes" in out.columns:
        out["delay_minutes"] = pd.to_numeric(out["delay_minutes"], errors="coerce")

    return out.reset_index(drop=True)


def transform_weather(df: pd.DataFrame) -> pd.DataFrame:
    """Clean weather data for storage and ML."""
    if df.empty:
        return df

    out = df.copy()
    for col in ["location_code"]:
        if col in out.columns:
            out[col] = out[col].astype(str).str.strip()
    if "recorded_at" in out.columns:
        out["recorded_at"] = _parse_dt(out["recorded_at"])
    for col in REQUIRED_WEATHER_COLUMNS:
        if col in out.columns:
            out = out.dropna(subset=[col])
        else:
            logger.warning("Missing required weather column: %s", col)
            return pd.DataFrame()
    out = out.drop_duplicates(subset=["location_code", "recorded_at"], keep="last")
    return out.reset_index(drop=True)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generic transform entry (e.g. for single-Dataset pipeline).
    For DelayPilot use transform_flights() and transform_weather().
    """
    if df.empty:
        return df
    return df.dropna(how="all").drop_duplicates()
