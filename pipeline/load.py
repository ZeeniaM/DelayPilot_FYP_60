"""
Load stage for DelayPilot (SRS Module 1: FE-3, FE-5).
Write raw and cleaned data to PostgreSQL.
"""
import logging
import pandas as pd
from sqlalchemy import text

from db.connection import get_engine, get_session
from db.schema import create_delaypilot_schema

logger = logging.getLogger(__name__)

# Column mapping to DB columns (snake_case)
RAW_FLIGHTS_COLS = [
    "source_id", "flight_number", "airline_code", "origin_airport", "destination_airport",
    "scheduled_departure_utc", "scheduled_arrival_utc", "actual_departure_utc", "actual_arrival_utc",
    "status", "delay_minutes", "raw_payload",
]
CLEANED_FLIGHTS_COLS = [
    "source_id", "flight_number", "airline_code", "origin_airport", "destination_airport",
    "scheduled_departure_utc", "scheduled_arrival_utc", "actual_departure_utc", "actual_arrival_utc",
    "status", "delay_minutes",
]
RAW_WEATHER_COLS = [
    "location_code", "recorded_at", "temperature_celsius", "humidity_pct",
    "visibility_km", "wind_speed_kmh", "conditions", "raw_payload",
]
CLEANED_WEATHER_COLS = [
    "location_code", "recorded_at", "temperature_celsius", "humidity_pct",
    "visibility_km", "wind_speed_kmh", "conditions",
]


def _ensure_schema():
    engine = get_engine()
    create_delaypilot_schema(engine)


def _serialize_payload(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return val


def load_raw_flights(df: pd.DataFrame) -> int:
    """Load extracted flight data into raw_flights. Returns row count inserted."""
    if df.empty:
        return 0
    _ensure_schema()
    engine = get_engine()
    # Ensure we have expected columns; add missing as None
    for c in RAW_FLIGHTS_COLS:
        if c not in df.columns and c != "raw_payload":
            df[c] = None
        elif c == "raw_payload" and c not in df.columns:
            df[c] = None
    df = df[[c for c in RAW_FLIGHTS_COLS if c in df.columns]]
    df["scheduled_departure_utc"] = pd.to_datetime(df["scheduled_departure_utc"], errors="coerce")
    df["scheduled_arrival_utc"] = pd.to_datetime(df["scheduled_arrival_utc"], errors="coerce")
    df["actual_departure_utc"] = pd.to_datetime(df["actual_departure_utc"], errors="coerce")
    df["actual_arrival_utc"] = pd.to_datetime(df["actual_arrival_utc"], errors="coerce")
    if "raw_payload" in df.columns:
        import json
        df["raw_payload"] = df["raw_payload"].apply(
            lambda x: json.dumps(x) if x is not None and not (isinstance(x, str)) else x
        )
    df.to_sql("raw_flights", engine, if_exists="append", index=False, method="multi")
    return len(df)


def load_cleaned_flights(df: pd.DataFrame) -> int:
    """Load transformed flight data into cleaned_flights (upsert by source_id)."""
    if df.empty:
        return 0
    _ensure_schema()
    cols = [c for c in CLEANED_FLIGHTS_COLS if c in df.columns]
    df = df[cols].copy()
    df["scheduled_departure_utc"] = pd.to_datetime(df["scheduled_departure_utc"], errors="coerce")
    df["scheduled_arrival_utc"] = pd.to_datetime(df["scheduled_arrival_utc"], errors="coerce")
    df["actual_departure_utc"] = pd.to_datetime(df["actual_departure_utc"], errors="coerce")
    df["actual_arrival_utc"] = pd.to_datetime(df["actual_arrival_utc"], errors="coerce")
    engine = get_engine()
    with get_session() as session:
        for _, row in df.iterrows():
            session.execute(
                text("""
                    INSERT INTO cleaned_flights (
                        source_id, flight_number, airline_code, origin_airport, destination_airport,
                        scheduled_departure_utc, scheduled_arrival_utc, actual_departure_utc, actual_arrival_utc,
                        status, delay_minutes
                    ) VALUES (
                        :source_id, :flight_number, :airline_code, :origin_airport, :destination_airport,
                        :scheduled_departure_utc, :scheduled_arrival_utc, :actual_departure_utc, :actual_arrival_utc,
                        :status, :delay_minutes
                    )
                    ON CONFLICT (source_id) DO UPDATE SET
                        flight_number = EXCLUDED.flight_number,
                        airline_code = EXCLUDED.airline_code,
                        origin_airport = EXCLUDED.origin_airport,
                        destination_airport = EXCLUDED.destination_airport,
                        scheduled_departure_utc = EXCLUDED.scheduled_departure_utc,
                        scheduled_arrival_utc = EXCLUDED.scheduled_arrival_utc,
                        actual_departure_utc = EXCLUDED.actual_departure_utc,
                        actual_arrival_utc = EXCLUDED.actual_arrival_utc,
                        status = EXCLUDED.status,
                        delay_minutes = EXCLUDED.delay_minutes
                """),
                {
                    "source_id": str(row.get("source_id") or ""),
                    "flight_number": str(row.get("flight_number")),
                    "airline_code": str(row.get("airline_code")) if pd.notna(row.get("airline_code")) else None,
                    "origin_airport": str(row.get("origin_airport")),
                    "destination_airport": str(row.get("destination_airport")),
                    "scheduled_departure_utc": _serialize_payload(row.get("scheduled_departure_utc")),
                    "scheduled_arrival_utc": _serialize_payload(row.get("scheduled_arrival_utc")),
                    "actual_departure_utc": _serialize_payload(row.get("actual_departure_utc")),
                    "actual_arrival_utc": _serialize_payload(row.get("actual_arrival_utc")),
                    "status": str(row.get("status")) if pd.notna(row.get("status")) else None,
                    "delay_minutes": int(row["delay_minutes"]) if pd.notna(row.get("delay_minutes")) else None,
                },
            )
    return len(df)


def load_raw_weather(df: pd.DataFrame) -> int:
    """Load extracted weather into raw_weather."""
    if df.empty:
        return 0
    _ensure_schema()
    for c in RAW_WEATHER_COLS:
        if c not in df.columns and c != "raw_payload":
            df[c] = None
        elif c == "raw_payload" and c not in df.columns:
            df[c] = None
    df = df[[c for c in RAW_WEATHER_COLS if c in df.columns]]
    df["recorded_at"] = pd.to_datetime(df["recorded_at"], errors="coerce")
    if "raw_payload" in df.columns:
        import json
        df["raw_payload"] = df["raw_payload"].apply(
            lambda x: json.dumps(x) if x is not None and not isinstance(x, str) else x
        )
    engine = get_engine()
    df.to_sql("raw_weather", engine, if_exists="append", index=False, method="multi")
    return len(df)


def load_cleaned_weather(df: pd.DataFrame) -> int:
    """Load transformed weather into cleaned_weather."""
    if df.empty:
        return 0
    _ensure_schema()
    cols = [c for c in CLEANED_WEATHER_COLS if c in df.columns]
    df = df[cols].copy()
    df["recorded_at"] = pd.to_datetime(df["recorded_at"], errors="coerce")
    engine = get_engine()
    df.to_sql("cleaned_weather", engine, if_exists="append", index=False, method="multi")
    return len(df)


def load_to_postgres(df: pd.DataFrame, table: str = "pipeline_results") -> int:
    """
    Generic load (legacy). For DelayPilot use load_raw_flights, load_cleaned_flights, etc.
    """
    if df.empty:
        return 0
    import json
    _ensure_schema()
    records = df.to_dict(orient="records")
    with get_session() as session:
        for row in records:
            session.execute(
                text("INSERT INTO pipeline_results (data) VALUES (:data)"),
                {"data": json.dumps(row)},
            )
    return len(records)
