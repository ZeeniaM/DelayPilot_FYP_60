"""
Configuration for the FYP data pipeline.
Load from environment variables (use .env file for local dev).
"""
import os
from dotenv import load_dotenv

load_dotenv()

# PostgreSQL connection (DelayPilot storage; SRS Module 1)
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_DB = os.getenv("POSTGRES_DB", "delaypilot_db")

# External data sources (SRS: flight/weather APIs)
# Flight: FIDS / airport scheduling API (set base URL and key if required)
FLIGHT_API_BASE_URL = os.getenv("FLIGHT_API_BASE_URL", "")
FLIGHT_API_KEY = os.getenv("FLIGHT_API_KEY", "")

# Weather: OpenWeatherMap or Aviation Weather Center (SRS 2.4.2)
OPENWEATHERMAP_API_KEY = os.getenv("OPENWEATHERMAP_API_KEY", "")
OPENWEATHERMAP_BASE_URL = os.getenv(
    "OPENWEATHERMAP_BASE_URL", "https://api.openweathermap.org/data/2.5"
)
MUNICH_CITY_ID = os.getenv("MUNICH_CITY_ID", "2861876")  # OpenWeatherMap city id

def get_connection_string():
    """Build PostgreSQL connection URI."""
    return (
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
