# DelayPilot Backend (Data Pipeline + API)

Backend for the **DelayPilot** FYP: an AI-powered decision-support tool for flight delays at Munich (MUC).  
This project:

- Ingests **flight + weather data** into PostgreSQL.
- Builds rich **feature tables** that match the pre-trained CatBoost models (`v3_final`).
- Exposes a **FastAPI** endpoint so the frontend can request delay predictions.
- Can be started with **one command**.

This implementation uses **PostgreSQL** instead of Snowflake, as agreed.

---

## 1. Quick start (Windows)

From a command prompt (CMD):

```cmd
cd C:\Users\Dell\fyp-data-pipeline
python -m venv venv
venv\Scripts\activate.bat

pip install -r requirements.txt
pip install fastapi uvicorn catboost
```

Make sure PostgreSQL is running and you have a database:

- Database name: `delaypilot_db`
- User: `postgres`
- Password: `delaypilot2026`

> The code will try to connect using these defaults. You can override them with `PG_USER`, `PG_PASSWORD`, `PG_HOST`, `PG_PORT`, `PG_DB` env vars if needed.

### One-command run

With the virtualenv active:

```cmd
cd C:\Users\Dell\fyp-data-pipeline
python start_delaypilot.py
```

This will:

1. Run the **data pipeline** end-to-end.
2. Start the **FastAPI** server on `http://localhost:8000`.

Then open:

- `http://localhost:8000/docs` for the Swagger UI.
- Use `GET /health` and `POST /predict/from-db` to test predictions.

---

## 2. Project layout (current)

```text
fyp-data-pipeline/
├── api_main.py                    # FastAPI app (prediction API)
├── model_service.py               # V3FinalModelService (CatBoost models)
├── start_delaypilot.py            # One-command launcher (pipeline + API)
├── run_pipeline.py                # Historical + live-fallback ETL pipeline
│
├── ingest_weather.py              # Ingest historical weather CSV OR fall back to live
├── ingest_flights.py              # Ingest historical flight parquet OR fall back to live
├── ingest_weather_live.py         # Fetch live weather from Open-Meteo
├── ingest_flights_live.py         # Fetch live FIDS from Aerodatabox
│
├── build_featured_muc_rxn_wx3.py      # Intermediate features: reactionary + weather
├── build_featured_muc_rxn_wx3_fe.py   # Final features: congestion + history + flags
├── build_training_features.py         # Training-like feature table (simpler)
├── clean_training_features.py         # Cleans training_features_v3
│
├── refresh_live_data.py           # Optional: explicit "live refresh" script
├── models/                        # CatBoost models + metadata
│   ├── cb_bin15_v3_final.cbm
│   ├── cb_bin30_v3_final.cbm
│   ├── cb_reg_delay_ge5_v3_final.cbm
│   ├── metadata_v3_final.json
│   └── cause_groups_v3_final.json
└── requirements.txt
```

---

## 3. Data flow (simple explanation)

1. **Get weather data**
   - Preferred: a historical CSV `weather_hourly_3airports_utc.csv` (3 airports, hourly).
   - If that CSV is missing, the system **automatically calls Open‑Meteo** and fills the `weather_hourly` table with live data.

2. **Get flight data**
   - Preferred: a folder of historical Parquet files (365 days of MUC flights).
   - If that folder is missing, the system **automatically calls Aerodatabox (RapidAPI)** and fills the `flights_raw` table with a window of live flights around “now”.

3. **Build feature tables**
   - `build_featured_muc_rxn_wx3.py`
     - Takes `flights_raw` + `weather_hourly`.
     - Computes:
       - `sched_utc` (main schedule time per flight).
       - Prediction reference time: `ref_ts_utc = sched_utc - 2h`, `ref_hour_utc`.
       - Delay labels: `y_delay_min`, `y_bin15`, `y_bin30`.
       - Reactionary features: delay of the previous flight of the same aircraft (`prev_delay_min_safe`, etc.).
       - Joins MUC weather and other-airport weather onto each flight.
     - Writes to `featured_muc_rxn_wx3` in Postgres.

   - `build_featured_muc_rxn_wx3_fe.py`
     - Takes `featured_muc_rxn_wx3`.
     - Adds:
       - Airport congestion features at MUC (arr/dep counts in ±1h/±2h windows).
       - Airline congestion features (per airline + movement).
       - Historical averages (route delay, airline delay, late-rate).
       - Simple weather flags (strong wind / gusts / any precip / any snow at MUC and remote airport).
       - Optional ANSPerf daily traffic/ATFM joins (if parquet files exist).
     - Writes to `featured_muc_rxn_wx3_fe` in Postgres.

4. **Serve predictions**
   - `model_service.py` loads:
     - `metadata_v3_final.json` (feature list, categorical features, thresholds).
     - `cause_groups_v3_final.json` (for future explanations).
     - Three CatBoost models: `bin15`, `bin30`, `reg_delay_ge5`.
   - `api_main.py` exposes a FastAPI endpoint:
     - `POST /predict/from-db`
       - Request body:
         - `number_raw` – raw flight number (e.g. `"LH 638"`).
         - `sched_utc` – scheduled time (flexible string; e.g. `"2026-03-05 02:35:00+05"`).
       - The API:
         1. Parses `sched_utc` to a proper timezone-aware datetime.
         2. Looks up the matching row in `featured_muc_rxn_wx3_fe`.
         3. Sends that row to `V3FinalModelService.predict_one`.
         4. Returns:
            - `p_delay_15`, `p_delay_30`
            - `pred_delay_15`, `pred_delay_30` (0/1)
            - `minutes_pred`, `minutes_ui`
            - `thresholds` used

---

## 4. Pipelines in more detail

### 4.1 Historical + live-fallback pipeline (`run_pipeline.py`)

This script is what `start_delaypilot.py` runs first. It:

1. **Ingests weather** (`ingest_weather.load_weather_csv_to_db()`):
   - If the historical CSV path exists:
     - Reads it and writes `weather_hourly`.
   - If not:
     - Logs a warning and calls `ingest_weather_live.update_weather_live()` (Open‑Meteo).

2. **Ingests flights** (`ingest_flights.load_flight_parquet_to_db()`):
   - If the Parquet folder exists:
     - Reads all `.parquet` files, concatenates, samples to 50k rows, writes `flights_raw`.
   - If not:
     - Logs a warning and calls `ingest_flights_live.ingest_live_muc_window()` (Aerodatabox).

3. **Builds intermediate features** (`build_featured_muc_rxn_wx3.build_featured_muc_rxn_wx3()`).
4. **Builds final features** (`build_featured_muc_rxn_wx3_fe.build_featured_muc_rxn_wx3_fe()`).
5. **Builds training-like features** (`build_training_features.build_features()`).
6. **Cleans training features** (`clean_training_features.clean_training_features()`).

If any step fails, it logs an error with a stack trace so you can see exactly what happened.

### 4.2 Live-only refresh (`refresh_live_data.py`)

This script does:

1. Fetch live weather (Open‑Meteo) → `weather_hourly`.
2. Fetch live FIDS (Aerodatabox) → `flights_raw`.
3. Rebuild `featured_muc_rxn_wx3`.
4. Rebuild `featured_muc_rxn_wx3_fe`.

Use this if you want to refresh the live window without running the full historical logic.

---

## 5. API usage

Start the system:

```cmd
cd C:\Users\Dell\fyp-data-pipeline
venv\Scripts\activate.bat
python start_delaypilot.py
```

Then open `http://localhost:8000/docs` and use:

- **`GET /health`**
  - Returns `{ "status": "ok" }` if the app is up.

- **`POST /predict/from-db`**
  - Body example:

    ```json
    {
      "number_raw": "LH 638",
      "sched_utc": "2026-03-05 02:35:00+05"
    }
    ```

  - Requirements:
    - The `(number_raw, sched_utc)` pair must exist in the `featured_muc_rxn_wx3_fe` table.
    - `sched_utc` can be `"YYYY-MM-DDTHH:MM:SS+HH:MM"` or `"YYYY-MM-DD HH:MM:SS+HH"`, etc.

  - Responses:
    - **200** – prediction JSON with probabilities and minutes.
    - **400** – invalid `sched_utc` format.
    - **404** – no matching flight in the feature table.
    - **500** – unexpected internal error (also logged with stack trace).

---

## 6. Logging and error handling

All main scripts use Python’s `logging` with a standard format:

```text
YYYY-MM-DD HH:MM:SS,ms - LEVEL - message
```

Examples:

- `INFO - Step 1/6: Ingest weather CSV -> weather_hourly`
- `WARNING - Weather CSV not found at: ...`
- `INFO - Falling back to live Open-Meteo weather ingestion instead.`
- `INFO - Wrote featured_muc_rxn_wx3_fe with 273 rows and 109 columns.`
- `INFO - Received prediction request number_raw=LH 638 sched_utc=...`
- `ERROR/EXCEPTION - Model prediction failed` (with stack trace, if it happens).

This makes it easy to show examiners:

- What happens if data files are missing (fallback to live APIs).
- What happens if DB tables are empty.
- What happens if the API is given bad input or a flight is not found.
- That the system is “production-style” in terms of observability.

---

## 7. Notes about API keys (Aerodatabox)

- For simplicity in this FYP backend:
  - The **RapidAPI key and host** for Aerodatabox are currently **hard-coded** in `ingest_flights_live.py`.
  - In a real production system, you should move these into environment variables or a secure secret store.

The Open‑Meteo endpoint is free and does not require an API key.

---

## 8. What’s ready now vs future work

- **Ready now**:
  - Live + historical ingestion into PostgreSQL (with fallbacks).
  - Full feature engineering pipeline matching the `v3_final` models.
  - Model loading and prediction service.
  - FastAPI endpoint for predictions backed by the feature table.
  - Single-command launcher and structured logging.

- **Future extensions**:
  - Add a `/predict/live` endpoint that fetches and builds features for a given flight on-demand (instead of looking up by `(number_raw, sched_utc)`).
  - Add explanation outputs (using `cause_groups_v3_final.json` and SHAP).
  - Integrate with a frontend UI for planners at MUC (DelayPilot dashboards). 
