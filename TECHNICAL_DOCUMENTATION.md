# DelayPilot Pipeline Technical Documentation

## 1. System Architecture

DelayPilot's data pipeline is a standalone Python service that runs independently from the UI stack. The user-facing system consists of an Express backend and a React frontend, while this repository owns the data ingestion, feature engineering, machine learning inference, background refresh scheduling, and FastAPI layer that serves pipeline data.

At startup, `start_delaypilot.py` acts as the launcher. It first runs `run_pipeline.py` synchronously so the database is populated with weather, flights, engineered features, predictions, and analytics snapshots. Once the ETL completes, it starts the FastAPI application with Uvicorn on port `8000`.

The web application does not talk to the database tables in this repository directly. Instead, the Express backend on port `5000` proxies requests to the FastAPI server on port `8000` for flights, predictions, weather, simulation, and analytics responses. Real-time browser communication is handled by the Express/WebSocket layer; the pipeline itself does not broadcast events.

The PostgreSQL database is shared with the wider DelayPilot platform. In practice, the pipeline writes its own tables into the same Supabase-backed PostgreSQL instance that also stores operational UI-side data. This keeps the web application and the pipeline aligned around a single source of truth while preserving a clean separation of responsibilities between the Python service and the Node/React application.

## 2. Technology Stack

- Language: Python 3.11
- API server: FastAPI + Uvicorn on port `8000`
- Machine learning: CatBoost with two `CatBoostClassifier` models and one `CatBoostRegressor`
- Data processing: `pandas`, `numpy`, `sqlalchemy`, `psycopg2`
- External APIs:
  - AeroDataBox via RapidAPI for live flight information display system data and flight status
  - Open-Meteo for hourly weather data
- Scheduling: standard-library `threading` with daemon threads
- Environment/dependency management: `venv`/virtualenv with `requirements.txt`
- Persistence: PostgreSQL

## 3. Runtime Topology

The runtime sequence is:

1. `start_delaypilot.py` launches the historical/live bootstrap ETL by calling `run_pipeline.py`.
2. `run_pipeline.py` executes the pipeline stages that populate the core tables used by the API.
3. Uvicorn starts `api_main.py` on port `8000`.
4. FastAPI startup creates `flight_status_live` if needed and starts the background scheduler.
5. `background_refresh.py` runs a full live refresh cycle every configured interval.
6. Express on port `5000` proxies UI requests to this API.

## 4. Project File Structure

### Root application files

- `start_delaypilot.py`
  Main launcher. Runs `run_pipeline.py` as a subprocess first, then starts the FastAPI app with Uvicorn on port `8000`.

- `run_pipeline.py`
  The 8-step ETL orchestrator. It loads weather, loads flights, builds intermediate features, builds final features, builds a training-compatible table, optionally cleans it, runs batch predictions, and snapshots dashboard analytics.

- `api_main.py`
  Defines the FastAPI application and all REST endpoints. Its startup hook ensures `flight_status_live` exists and then launches the background scheduler.

- `background_refresh.py`
  Runs the background scheduler thread. It executes a full 6-step refresh cycle every configured interval, stores in-memory pipeline log entries, tracks refresh state in a shared dictionary, and does not perform any websocket broadcasting.

- `model_service.py`
  Contains `V3FinalModelService`. This class loads the three CatBoost model files and model metadata, prepares single-row inference input, and exposes `predict_one()` with per-model `try/except` guards plus numeric NaN/inf coercion.

- `run_batch_predictions.py`
  Loads all rows from `featured_muc_rxn_wx3_fe`, runs all three CatBoost models row by row, derives `ml_cause` and `cause_scores`, and writes `flight_predictions`.

- `ingest_weather.py`
  Bootstrap weather loader. It tries live Open-Meteo ingestion first and falls back to a historical CSV if live ingestion is unavailable.

- `ingest_weather_live.py`
  Fetches hourly weather for Munich, Frankfurt, and London Heathrow from Open-Meteo and writes the resulting horizon to `weather_hourly`.

- `ingest_flights.py`
  Bootstrap historical flight loader. It attempts to load parquet files into `flights_raw`, sampling to a maximum row limit, and falls back to live ingestion if the configured parquet path is unavailable.

- `ingest_flights_live.py`
  Fetches a live AeroDataBox MUC flight window, normalizes departures and arrivals, computes best timestamps and delay labels, and replaces `flights_raw`.

- `build_featured_muc_rxn_wx3.py`
  Joins `flights_raw` with `weather_hourly`, derives unified schedule and label fields, builds reactionary lag features, joins Munich weather and other-airport weather, and writes `featured_muc_rxn_wx3`.

- `build_featured_muc_rxn_wx3_fe.py`
  Adds congestion counts, airline-level congestion, route and airline historical delay aggregates, weather flags, optional ANSPerf daily joins, and writes `featured_muc_rxn_wx3_fe`.

- `build_training_features.py`
  Produces a simplified feature table for model training compatibility and writes `training_features_v3`.

- `clean_training_features.py`
  Cleans `training_features_v3` by dropping missing critical fields, coercing numeric columns, clipping outliers, and writing `training_features_v3_clean`.

- `snapshot_delay_analytics.py`
  Resolves per-flight delay status using confirmed-delay, FIDS, and ML priority logic, then upserts the results into `flight_delay_snapshots`.

- `config.py`
  Loads environment settings and builds the SQLAlchemy/PostgreSQL connection string used by some pipeline components.

- `update_flight_status.py`
  Auxiliary live status updater used by the background refresh cycle to populate `flight_status_live` from the flight status API.

### Supporting directories

- `models/`
  Stores the three `.cbm` CatBoost model files plus `metadata_v3_final.json` and `cause_groups_v3_final.json`.

- `data/`
  Holds local CSV/parquet inputs used for historical bootstrap and optional ANSPerf enrichment.

- `db/`
  Contains helper modules related to database connectivity/schema support.

- `pipeline/`
  Contains lower-level extract/transform/load helper modules from an earlier or supporting pipeline layout.

- `tools/`
  Utility scripts for inspecting and validating external API payloads and stored weather data.

- `venv/`
  Local virtual environment directory.

## 5. ETL Flow

`run_pipeline.py` runs these eight steps:

1. Weather ingest into `weather_hourly`
2. Flight ingest into `flights_raw`
3. Build `featured_muc_rxn_wx3`
4. Build `featured_muc_rxn_wx3_fe`
5. Build `training_features_v3`
6. Build `training_features_v3_clean`
7. Run batch ML predictions into `flight_predictions`
8. Snapshot analytics into `flight_delay_snapshots`

The startup bootstrap is separate from the background refresh cycle. The background cycle is optimized for live operational refreshes and runs:

1. Weather
2. FIDS
3. Feature build pass 1
4. Feature build pass 2
5. Flight Status API
6. Delay snapshot

In the current code, the background cycle does not rerun `run_batch_predictions.py`; it refreshes live data, rebuilds features, updates flight status, and snapshots analytics. The bootstrap pipeline performs batch prediction generation explicitly.

## 6. Database Tables (Pipeline-Owned)

### `weather_hourly`

Stores hourly weather for the monitored airports. Core fields include:

- `airport_icao`
- `hour_utc`
- `temperature_2m`
- `wind_speed_10m`
- `wind_gusts_10m`
- `precipitation`
- `snowfall`
- `visibility`
- `weather_code`
- `cloud_cover`
- `relative_humidity_2m`

Additional Open-Meteo fields are also stored when available, such as rain, pressure, dew point, sunshine duration, and boundary-layer variables.

### `flights_raw`

Stores raw or normalized flight records from historical parquet or the live AeroDataBox window. Common fields include:

- `number_raw`
- `airline_iata`
- `airline_icao`
- `movement`
- `dep_sched_utc`
- `arr_sched_utc`
- `dep_best_utc`
- `arr_best_utc`
- `dep_delay_min`
- `arr_delay_min`
- `y_delay_min`
- `y_bin15`
- `y_bin30`
- `other_airport_icao`
- `aircraft_modeS`
- plus status, aircraft, airport, and timing fields from the FIDS payload

### `featured_muc_rxn_wx3`

Intermediate feature table produced by joining flights and weather, then adding reactionary lag features. It is approximately 123 columns wide in the current project narrative and contains:

- unified schedule and reference timestamps
- delay labels
- previous-flight reactionary features
- Munich weather prefixed as `wx_muc_*`
- connected-airport weather prefixed as `wx_other_*`

### `featured_muc_rxn_wx3_fe`

Final serving feature table used by the API and prediction logic. It extends the previous table with:

- airport congestion rolling counts
- airline congestion rolling counts
- route historical delay aggregates
- airline historical delay aggregates
- binary weather flags
- optional ANSPerf daily traffic and ATFM joins

This is the main table that `/flights`, `/predict/from-db`, `/simulate`, `/flights/propagation`, and analytics-related joins depend on.

### `training_features_v3`

A simplified training-compatible feature subset written for training and compatibility workflows.

### `training_features_v3_clean`

The cleaned version of `training_features_v3`, with critical missing rows removed and numeric outliers clipped.

### `flight_predictions`

Stores batch model outputs. Fields include:

- `number_raw`
- `sched_utc`
- `p_delay_15`
- `p_delay_30`
- `pred_delay_15`
- `pred_delay_30`
- `minutes_pred`
- `minutes_ui`
- `ml_cause`
- `cause_scores` as JSON text
- `predicted_at`

### `flight_status_live`

Stores authoritative flight status updates fetched from the live status API. Fields include:

- `number_raw`
- `flight_date`
- `op_status`
- `etd_utc`
- `atd_utc`
- `eta_utc`
- `ata_utc`
- `confirmed_delay_min`
- `fetched_at`

### `flight_delay_snapshots`

An upserted analytics table used for dashboard trends and cause breakdowns. It stores one current row per flight key and refreshes values over time. Fields include:

- `number_raw`
- `sched_utc`
- `flight_date`
- `resolved_delay_min`
- `delay_source`
- `delay_status`
- `op_status`
- `ml_cause`
- `cause_scores`
- `wx_weather_code`
- `wx_precipitation`
- `ml_p_delay_15`
- `ml_p_delay_30`
- `last_updated_utc`

## 7. Machine Learning Models

The deployed inference stack uses three CatBoost models:

- `clf15` (`CatBoostClassifier`)
  Predicts probability of delay greater than or equal to 15 minutes.
  Threshold: `0.30`
  AUC: `0.7619`
  PR-AUC: `0.5656`

- `clf30` (`CatBoostClassifier`)
  Predicts probability of delay greater than or equal to 30 minutes.
  Threshold: `0.40`
  AUC: `0.7419`
  PR-AUC: `0.3539`

- `reg2` (`CatBoostRegressor`)
  Estimates delay magnitude in minutes for delayed flights.
  Training scope: flights delayed by at least 5 minutes
  Approximate MAE: `16.55` minutes

Feature notes:

- The final serving table is described as roughly 123 columns wide.
- The checked-in `metadata_v3_final.json` currently lists 91 model input features selected from that wider feature table.

Top feature-group contribution summary used in project reporting:

- Weather: `25.3%`
- Historical patterns: `20.1%`
- Airline/route identity: `17.6%`
- Reactionary: `12.1%`
- Time/seasonality: `9.6%`

NaN and safety handling in `model_service.py`:

- numeric columns are coerced to numeric
- `inf` and `-inf` are converted away before inference
- numeric model inputs are converted to `float64`
- missing numeric values are filled with `0`
- each of the three model calls is individually wrapped in `try/except`

## 8. Cause Derivation

`ml_cause` is not a native output of any CatBoost model. The models produce delay probabilities and delay magnitude only.

The pipeline derives cause attribution in `run_batch_predictions.py` using `_derive_cause_scores()`. That routine scores five categories from `0.0` to `1.0` based on engineered feature signals:

- Weather (MUC)
- En-Route Weather
- Reactionary
- ATC / Congestion
- Airline / Turnaround

The highest-scoring category becomes the primary cause. The full score breakdown is normalized into percentages and stored in `flight_predictions.cause_scores` as JSON text so the UI can render contribution bars.

## 9. FastAPI Endpoints

Core operational endpoints:

- `GET /health`
  Returns pipeline status, last run timestamps, and running state.

- `GET /pipeline-logs`
  Returns the in-memory log of recent scheduler events plus health metadata.

- `GET /flights`
  Returns flights from `featured_muc_rxn_wx3_fe` enriched with joins to `flights_raw`, `flight_status_live`, and `flight_predictions`.

- `GET /flights/current`
  Returns a recent subset for the UI table.

- `GET /weather/current`
  Returns the latest weather rows for `EDDM`, `EDDF`, and `EGLL`.

- `POST /predict/from-db`
  Loads one feature row from the database, runs `predict_one()`, and enriches the response with `flight_status_live`.

- `GET /flights/propagation`
  Finds same-tail later rotations on the same day and estimates propagated delay effects.

- `POST /simulate`
  Loads one feature row, applies user overrides, reruns `predict_one()`, and returns baseline vs simulated prediction plus reactionary downstream impact estimates.

- `GET /flights/analytics`
  Reads `flight_delay_snapshots` and returns trend/cause analytics for the dashboard.

Auxiliary endpoint present in the current codebase:

- `POST /auth/login`
  Demo-only authentication stub used by the frontend.

## 10. Background Refresh Cycle

The background scheduler uses daemon threads and runs every 30 minutes by default. The interval can be driven by:

- `system_settings` table key `refresh_interval_minutes`, when available
- otherwise `REFRESH_INTERVAL_MINUTES`
- otherwise a 30-minute fallback

The six steps in each cycle are:

1. Weather
2. FIDS
3. Feature build pass 1
4. Feature build pass 2
5. Flight Status API
6. Delay snapshot

Each step is wrapped in its own `try/except`, so one failure does not stop the remaining steps. Shared refresh state is stored in a module-level dictionary containing `last_ran`, `running`, `last_error`, `fids_last_ran`, and `status_last_ran`.

`last_ran` is updated at cycle end, not cycle start.

Logs are stored in `_pipeline_log` with a maximum of 50 entries. Entries are returned newest-first by `GET /pipeline-logs`.

One implementation detail worth noting: the scheduler intentionally defers the first background cycle until after the initial interval elapses because `start_delaypilot.py` already runs the bootstrap ETL before the API starts.

## 11. Simulation Logic

The simulation endpoint allows selective override of live feature values without modifying the stored database row.

Supported override inputs:

- Munich weather:
  - `wind_speed_10m`
  - `wind_gusts_10m`
  - `precipitation`
  - `snowfall`
  - `visibility`
  - `weather_code`
- Reactionary:
  - `prev_delay_min_safe`
- Congestion:
  - `muc_arr_1h`
  - `muc_dep_1h`

Derived-field recomputation behavior:

- overriding wind speed recomputes `muc_wind_strong`
- overriding wind gusts recomputes `muc_gust_strong`
- overriding precipitation recomputes `muc_precip_any`
- overriding snowfall recomputes `muc_snow_any` and can also force `muc_precip_any`
- overriding previous delay recomputes `prev_late15_safe` and `prev_late30_safe`
- overriding airport congestion rescales the `±2h` counts using the `5/3` ratio
- airline-level congestion is estimated at `15%` of airport totals

The endpoint blocks simulation for flights already marked as `Landed`, `Cancelled`, or `Diverted`.

Reactionary impact estimation:

- queries `flights_raw` for same-airline departures within the next 3 hours
- applies a decay factor of `0.6^n` by downstream hop
- returns an impact list for downstream flights whose estimated added delay is at least 5 minutes

## 12. Environment Variables and Setup

### Intended environment variables

- `PG_USER`
- `PG_PASSWORD`
- `PG_HOST`
- `PG_PORT`
- `PG_DB`
- `RAPIDAPI_KEY`
- `REFRESH_INTERVAL_MINUTES`

### Current codebase note

There is an environment-variable naming mismatch in the repository:

- most runtime modules use `PG_*`
- `config.py` and `.env.example` currently use `POSTGRES_*`

For developers, this means the repository would benefit from standardizing on one naming convention. The live flight ingestion script also currently contains a hard-coded RapidAPI key/host block, even though the intended production design is environment-based configuration.

### Setup

1. Create a virtual environment:
   `python -m venv venv`
2. Activate it:
   `venv\Scripts\activate` on Windows
   `source venv/bin/activate` on macOS/Linux
3. Install dependencies:
   `pip install -r requirements.txt`
4. Set environment variables or provide a `.env` file
5. Start the pipeline:
   `python start_delaypilot.py`

## 13. Integration Notes

- This repository is operationally separate from the Express + React UI.
- Express reads from FastAPI; it does not own the pipeline logic.
- The web application should be treated as read-only with respect to pipeline-managed tables.
- Because pipeline tables are rebuilt or refreshed independently, the API contract is the stable integration surface between the Python service and the UI stack.
