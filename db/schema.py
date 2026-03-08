"""
DelayPilot database schema (PostgreSQL).
Aligns with SRS Module 1: Data Ingestion, Storage & Cleaning.
"""
from sqlalchemy import text


def create_delaypilot_schema(engine):
    """
    Create raw and cleaned tables for flight and weather data.
    Run once or as part of pipeline initialization.
    """
    with engine.begin() as conn:
        # ---- Raw layer (FE-3: store all raw data securely) ----
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw_flights (
                id SERIAL PRIMARY KEY,
                source_id VARCHAR(255),
                flight_number VARCHAR(20),
                airline_code VARCHAR(10),
                origin_airport VARCHAR(10),
                destination_airport VARCHAR(10),
                scheduled_departure_utc TIMESTAMPTZ,
                scheduled_arrival_utc TIMESTAMPTZ,
                actual_departure_utc TIMESTAMPTZ,
                actual_arrival_utc TIMESTAMPTZ,
                status VARCHAR(50),
                delay_minutes INTEGER,
                raw_payload JSONB,
                ingested_at TIMESTAMPTZ DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw_weather (
                id SERIAL PRIMARY KEY,
                location_code VARCHAR(20),
                recorded_at TIMESTAMPTZ,
                temperature_celsius NUMERIC(5,2),
                humidity_pct INTEGER,
                visibility_km NUMERIC(6,2),
                wind_speed_kmh NUMERIC(6,2),
                conditions VARCHAR(100),
                raw_payload JSONB,
                ingested_at TIMESTAMPTZ DEFAULT NOW()
            )
        """))

        # ---- Cleaned layer (FE-4, FE-5: clean and prepare for analysis/ML) ----
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS cleaned_flights (
                id SERIAL PRIMARY KEY,
                source_id VARCHAR(255) UNIQUE,
                flight_number VARCHAR(20) NOT NULL,
                airline_code VARCHAR(10),
                origin_airport VARCHAR(10) NOT NULL,
                destination_airport VARCHAR(10) NOT NULL,
                scheduled_departure_utc TIMESTAMPTZ NOT NULL,
                scheduled_arrival_utc TIMESTAMPTZ NOT NULL,
                actual_departure_utc TIMESTAMPTZ,
                actual_arrival_utc TIMESTAMPTZ,
                status VARCHAR(50),
                delay_minutes INTEGER,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS cleaned_weather (
                id SERIAL PRIMARY KEY,
                location_code VARCHAR(20) NOT NULL,
                recorded_at TIMESTAMPTZ NOT NULL,
                temperature_celsius NUMERIC(5,2),
                humidity_pct INTEGER,
                visibility_km NUMERIC(6,2),
                wind_speed_kmh NUMERIC(6,2),
                conditions VARCHAR(100),
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """))

        # Indexes for common queries (Munich Airport focus)
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_raw_flights_origin
            ON raw_flights(origin_airport);
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_raw_flights_dest
            ON raw_flights(destination_airport);
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_raw_flights_scheduled_dep
            ON raw_flights(scheduled_departure_utc);
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_cleaned_flights_origin_dest
            ON cleaned_flights(origin_airport, destination_airport);
        """))

        # Legacy table for generic pipeline_results (optional)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS pipeline_results (
                id SERIAL PRIMARY KEY,
                data JSONB,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """))
