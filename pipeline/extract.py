"""
Extract stage for DelayPilot (SRS Module 1: FE-1, FE-2).
Pull data from flight and weather sources.
"""
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from config import (
    OPENWEATHERMAP_API_KEY,
    OPENWEATHERMAP_BASE_URL,
    MUNICH_CITY_ID,
    FLIGHT_API_BASE_URL,
    FLIGHT_API_KEY,
)

logger = logging.getLogger(__name__)


def extract_weather() -> pd.DataFrame:
    """
    FE-2: Receive live weather updates.
    Uses OpenWeatherMap (SRS 2.4.2) for Munich. Returns empty DataFrame if key missing.
    """
    if not OPENWEATHERMAP_API_KEY:
        logger.warning("OPENWEATHERMAP_API_KEY not set; skipping weather extraction.")
        return pd.DataFrame()

    url = f"{OPENWEATHERMAP_BASE_URL}/weather"
    params = {"id": MUNICH_CITY_ID, "appid": OPENWEATHERMAP_API_KEY, "units": "metric"}
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        return pd.DataFrame([
            {
                "location_code": "EDDM",  # Munich Airport ICAO
                "recorded_at": datetime.fromtimestamp(data["dt"], tz=timezone.utc).isoformat(),
                "temperature_celsius": data["main"].get("temp"),
                "humidity_pct": data["main"].get("humidity"),
                "visibility_km": (data.get("visibility") or 0) / 1000.0,
                "wind_speed_kmh": (data["wind"].get("speed") or 0) * 3.6,
                "conditions": data["weather"][0].get("main") if data.get("weather") else None,
                "raw_payload": data,
            }
        ])
    except Exception as e:
        logger.exception("Weather extraction failed: %s", e)
        return pd.DataFrame()


def extract_flights_from_api() -> pd.DataFrame:
    """
    FE-2: Receive live flight status from external API.
    Placeholder: implement against your FIDS/scheduling API using FLIGHT_API_BASE_URL and FLIGHT_API_KEY.
    """
    if not FLIGHT_API_BASE_URL:
        logger.warning("FLIGHT_API_BASE_URL not set; skipping API flight extraction.")
        return pd.DataFrame()

    headers = {}
    if FLIGHT_API_KEY:
        headers["Authorization"] = f"Bearer {FLIGHT_API_KEY}"
    try:
        r = requests.get(FLIGHT_API_BASE_URL, headers=headers, timeout=15)
        r.raise_for_status()
        # Adapt to your API response shape; return DataFrame with columns matching raw_flights
        data = r.json()
        if isinstance(data, list):
            return pd.DataFrame(data)
        return pd.DataFrame([data])
    except Exception as e:
        logger.exception("Flight API extraction failed: %s", e)
        return pd.DataFrame()


def extract_flights_from_csv(file_path: str) -> pd.DataFrame:
    """
    FE-1: Collect historical flight data from file (e.g. exported from public aviation sources).
    """
    path = Path(file_path)
    if not path.exists():
        logger.warning("Flight CSV not found: %s", file_path)
        return pd.DataFrame()
    df = pd.read_csv(file_path)
    return df


def extract_raw_flights(csv_path: str | None = None) -> pd.DataFrame:
    """
    Main flight extract: try API first, then optional CSV for historical data.
    """
    df_api = extract_flights_from_api()
    if not df_api.empty:
        return _normalize_flight_columns(df_api)
    if csv_path:
        df_csv = extract_flights_from_csv(csv_path)
        if not df_csv.empty:
            return _normalize_flight_columns(df_csv)
    return pd.DataFrame()


def _normalize_flight_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map common column names to raw_flights schema."""
    column_map = {
        "flight_number": "flight_number",
        "flight_no": "flight_number",
        "airline": "airline_code",
        "airline_code": "airline_code",
        "origin": "origin_airport",
        "origin_airport": "origin_airport",
        "departure_airport": "origin_airport",
        "destination": "destination_airport",
        "destination_airport": "destination_airport",
        "arrival_airport": "destination_airport",
        "scheduled_departure": "scheduled_departure_utc",
        "scheduled_departure_utc": "scheduled_departure_utc",
        "scheduled_arrival": "scheduled_arrival_utc",
        "scheduled_arrival_utc": "scheduled_arrival_utc",
        "actual_departure": "actual_departure_utc",
        "actual_departure_utc": "actual_departure_utc",
        "actual_arrival": "actual_arrival_utc",
        "actual_arrival_utc": "actual_arrival_utc",
        "status": "status",
        "delay_minutes": "delay_minutes",
        "delay": "delay_minutes",
    }
    out = pd.DataFrame()
    for std_name, col in column_map.items():
        for c in df.columns:
            if c.lower() == std_name.lower() or c.lower() == col.lower():
                out[col] = df[c]
                break
    if out.empty and not df.empty:
        out = df.copy()
    return out


def extract_raw() -> pd.DataFrame:
    """
    Legacy single-df entry point; prefer extract_raw_flights() and extract_weather().
    """
    return pd.DataFrame()
