import logging
import os

import pandas as pd
from sqlalchemy import create_engine

try:
    # Fallback to live weather if the historical CSV is missing
    from ingest_weather_live import update_weather_live
except ImportError:  # pragma: no cover - very unlikely in this project layout
    update_weather_live = None  # type: ignore[misc]


logger = logging.getLogger(__name__)


def get_engine() -> "create_engine":
    """
    Create a SQLAlchemy engine for Postgres using environment variables.

    Expected env vars (with sensible defaults for local dev):
      - PG_USER
      - PG_PASSWORD
      - PG_HOST
      - PG_PORT
      - PG_DB
    """
    pg_user = os.getenv("PG_USER", "postgres")
    pg_password = os.getenv("PG_PASSWORD", "delaypilot2026")
    pg_host = os.getenv("PG_HOST", "localhost")
    pg_port = os.getenv("PG_PORT", "5432")
    pg_db = os.getenv("PG_DB", "delaypilot_db")

    url = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    return create_engine(url)


def load_weather_csv_to_db() -> None:
    """
    Load weather data into the weather_hourly table in Postgres.
    Priority: Try live Open-Meteo first; fall back to historical CSV if not available.
    """
    # Try live weather first
    if update_weather_live is not None:
        try:
            logger.info("Step 1: Attempting to fetch live Open-Meteo weather data...")
            update_weather_live()
            logger.info("Successfully populated weather_hourly with live data.")
            return
        except Exception as e:
            logger.warning("Live weather fetch failed: %s", str(e))
            logger.info("Falling back to historical CSV instead.")
    else:
        logger.warning("Live weather ingestion not available; trying historical CSV.")

    # Fall back to historical CSV
    script_dir = os.path.dirname(os.path.abspath(__file__))
    weather_path = os.path.join(script_dir, "data", "weather_hourly_3airports_utc.csv")

    if not os.path.exists(weather_path):
        logger.error("Weather CSV not found at: %s", weather_path)
        raise FileNotFoundError(f"Weather CSV not found at: {weather_path}")

    logger.info("Reading weather data from: %s", weather_path)
    df = pd.read_csv(weather_path, parse_dates=["hour_utc"])

    # Optional: keep only Munich (EDDM) rows if you want to restrict
    # df = df[df["airport_icao"] == "EDDM"]

    engine = get_engine()
    logger.info("Writing data to Postgres table: weather_hourly")

    df.to_sql(
        "weather_hourly",
        engine,
        if_exists="replace",
        index=False,
    )
    logger.info("Inserted %d rows into weather_hourly.", len(df))


if __name__ == "__main__":
    load_weather_csv_to_db()

