# DelayPilot Data Pipeline (PostgreSQL)

ETL pipeline for **DelayPilot** — AI-powered flight delay prediction and management at Munich Airport (FYP).  
This repo implements **Module 1: Data Ingestion, Storage & Cleaning** and supports **Module 13: Automation & Orchestration** from the DelayPilot SRS.

## SRS mapping

| SRS requirement | Implementation |
|-----------------|----------------|
| **FE-1** Collect historical flight and weather data from public aviation sources | `extract_flights_from_csv()`, `extract_weather()` (OpenWeatherMap) |
| **FE-2** Receive live flight status and weather updates at regular intervals | `extract_flights_from_api()`, `extract_weather()`; run pipeline on a schedule (cron / Prefect / n8n) |
| **FE-3** Store all raw data securely | PostgreSQL tables `raw_flights`, `raw_weather` |
| **FE-4** Clean missing, inconsistent, or duplicate flight records | `transform_flights()`, `transform_weather()` in `pipeline/transform.py` |
| **FE-5** Use SQL scripts / organized data for analysis and ML | Tables `cleaned_flights`, `cleaned_weather`; ready for dbt or ML training |

*Note: The SRS mentions Snowflake; this implementation uses **PostgreSQL** as requested.*

## Setup

1. **Create virtual environment**
   ```bash
   cd fyp-data-pipeline
   python -m venv venv
   venv\Scripts\activate   # Windows
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **PostgreSQL**
   - Install PostgreSQL and create a database, e.g. `CREATE DATABASE delaypilot_db;`

4. **Environment**
   - Copy `.env.example` to `.env`
   - Set at least: `POSTGRES_*` (host, user, password, db)
   - For **weather**: `OPENWEATHERMAP_API_KEY` (and optionally `MUNICH_CITY_ID`)
   - For **live flights**: `FLIGHT_API_BASE_URL` and optionally `FLIGHT_API_KEY` (your FIDS/scheduling API)

## Project layout

```
fyp-data-pipeline/
├── config.py              # DB + API config (env vars)
├── .env.example / .env
├── requirements.txt
├── run_pipeline.py        # Entry: python run_pipeline.py [--flight-csv ...]
├── db/
│   ├── connection.py      # PostgreSQL engine/session
│   └── schema.py          # DelayPilot tables (raw_*, cleaned_*)
└── pipeline/
    ├── extract.py         # Flight (API/CSV) + Weather (OpenWeatherMap)
    ├── transform.py       # Clean flights & weather
    └── load.py            # Load into PostgreSQL (raw + cleaned)
```

## Run the pipeline

```bash
# Full run (weather + flights; flights from API if configured, else nothing)
python run_pipeline.py

# With historical flights from CSV
python run_pipeline.py --flight-csv data/flights.csv

# Weather only
python run_pipeline.py --weather-only

# Flights only (API or CSV)
python run_pipeline.py --flights-only --flight-csv data/flights.csv
```

## Data sources

- **Weather**: OpenWeatherMap (Munich). Set `OPENWEATHERMAP_API_KEY` in `.env`.
- **Flights**:  
  - **Live**: Set `FLIGHT_API_BASE_URL` (and `FLIGHT_API_KEY` if needed) to your FIDS/scheduling API.  
  - **Historical**: Use `--flight-csv` with a CSV that has columns like `flight_number`, `origin_airport`, `destination_airport`, `scheduled_departure_utc`, `scheduled_arrival_utc`, etc. (see `extract.py` for column mapping).

## Automation (Module 13)

To run at regular intervals (e.g. every 15–60 minutes):

- **Windows Task Scheduler** or **cron** (Linux): schedule `python run_pipeline.py`
- **Prefect / n8n**: use this repo as the script/command in a flow that runs on a schedule
- **dbt**: add a dbt project that reads from `cleaned_flights` / `cleaned_weather` for further SQL transformations

## Next steps

- **Module 2 (Feature engineering)**: Build views or tables from `cleaned_flights` and `cleaned_weather` (e.g. hourly delay trends, taxi times).  
- **Module 3 (ML)**: Train delay/cause models using `cleaned_*` tables.  
- **Module 4 (API)**: FastAPI can read from the same PostgreSQL DB for predictions and simulation.
