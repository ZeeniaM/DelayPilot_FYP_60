# DelayPilot Pipeline: Technical Documentation (UPDATED)

**Project:** DelayPilot FYP — AI-powered flight delay prediction for Munich Airport (MUC)  
**Date:** March 2026  
**Database:** PostgreSQL  
**ML Models:** CatBoost (v3_final)  
**Key Update:** Background scheduler for live FIDS + Flight Status refresh

---

## Table of Contents

1. [Project Architecture](#project-architecture)
2. [File Structure & Purpose](#file-structure--purpose)
3. [Execution Flow](#execution-flow)
4. [Database Schema](#database-schema)
5. [API Endpoints](#api-endpoints)
6. [Background Refresh & Live Data](#background-refresh--live-data)
7. [Configuration & Environment Variables](#configuration--environment-variables)
8. [Running the Project](#running-the-project)

---

## 1. Project Architecture

### High-Level Overview

```
┌──────────────────────────────────────────────────────┐
│  START: start_delaypilot.py (Main Entry Point)       │
└────────────────┬─────────────────────────────────────┘
                 │
                 ├──→ Historical Pipeline (run_pipeline.py) [subprocess]
                 │     - Ingest weather + flights
                 │     - Build features (6 steps)
                 │     └──→ Completes, returns control
                 │
                 └──→ FastAPI Server (api_main.py)
                      - Load ML models
                      - Enable CORS middleware
                      - Launch Background Scheduler
                      │  ├──→ FIDS Refresh (every 2h, run first)
                      │  └──→ Flight Status Refresh (every 15 min, after FIDS)
                      │
                      └──→ http://localhost:8000 [blocking]
                           - Serve prediction & data endpoints
                           - Scheduler runs independently in threads
```

### System Components

| Component | Purpose |
|-----------|---------|
| **Data Ingestion** | Weather (Open-Meteo) & Flights (Aerodatabox) fetch |
| **Feature Engineering** | Transform raw data into ML-ready features |
| **Model Service** | Load & run 3 CatBoost models (bin15, bin30, reg_delay_ge5) |
| **Background Scheduler** | Autonomous FIDS/Flight Status refresh in background threads |
| **API Server** | FastAPI with CORS, prediction, data listing, and simulation endpoints |
| **Database** | PostgreSQL for flights, weather, features, and live status |
| **Flight Status Updater** | Polls Aerodatabox for operational status of active flights |

---

## 2. File Structure & Purpose

### Core Pipeline Files

#### **Orchestration**
- **`start_delaypilot.py`** (MAIN ENTRY POINT)
  - Single command to launch entire system
  - Runs `run_pipeline.py` as subprocess (historical pipeline)
  - Waits for pipeline to complete
  - Starts FastAPI server with `uvicorn` on http://localhost:8000 (blocking)
  - Logs all major milestones

#### **Pipeline Execution**
- **`run_pipeline.py`** (ETL ORCHESTRATOR)
  - Runs 6 sequential pipeline steps
  - Step 1: Ingest weather (live-first, fallback to CSV)
  - Step 2: Ingest flights (live-first, fallback to parquet)
  - Step 3: Build intermediate features
  - Step 4: Build final features
  - Step 5: Build training-like features
  - Step 6: Clean training features
  - Accepts `--no-clean` flag to skip final cleaning
  - Catches & logs errors at each step

### Model & API Files

- **`model_service.py`** (ML MODEL SERVICE)
  - Loads trained CatBoost models from `models/` directory
  - Loads metadata & cause groups from JSON files
  - Provides `predict_one()` method for single-row predictions
  - Handles categorical encoding & numeric preprocessing
  - Class: `V3FinalModelService`

- **`api_main.py`** (FASTAPI SERVER)
  - FastAPI app for REST API with CORS middleware enabled
  - **Endpoints:**
    - `GET /health` → server status + background refresh state
    - `POST /auth/login` → demo login for frontend (any 4+ char user, 8+ char password)
    - `POST /predict/from-db` → predict delay for flight (enriched with operational status)
    - `GET /flights` → list all flights with status JOINs
    - `GET /flights/current` → recent flights for UI table
    - `GET /weather/current` → current weather conditions
    - `POST /simulate` → what-if analysis with weather/congestion overrides
  - Loads model service at startup
  - Creates `flight_status_live` table on startup
  - Launches background scheduler thread on startup
  - Connects to PostgreSQL for feature + status lookup
  - Returns probabilities, predicted minutes, and operational status
  - **CORS:** Allows `http://localhost:3000`, `http://localhost:5000`, and wildcard for demo

- **`background_refresh.py`** (BACKGROUND SCHEDULER)
  - Runs two independent refresh tasks in daemon threads
  - **FIDS Refresh** (every 2 hours, priority 1):
    - Calls `update_weather_live()` (Open-Meteo)
    - Calls `ingest_live_muc_window()` (Aerodatabox)
    - Rebuilds `featured_muc_rxn_wx3` and `featured_muc_rxn_wx3_fe`
    - Runs immediately on first iteration, then every 120 min
  - **Flight Status Refresh** (every 15 minutes, priority 2):
    - Calls `update_flight_status()` to poll Aerodatabox Flight Status API
    - Waits for FIDS to complete first
    - Then runs immediately, then every 15 min
  - Thread-safe state tracking (read by /health endpoint)
  - Configurable intervals via env vars: `FIDS_INTERVAL_MINUTES`, `FLIGHT_STATUS_INTERVAL_MINUTES`
  - Function: `start_background_refresh()`

- **`update_flight_status.py`** (FLIGHT STATUS UPDATER)
  - Polls Aerodatabox Flight Status API for all flights in `flights_raw`
  - Updates `flight_status_live` table with operational status
  - Reuses RapidAPI credentials from `ingest_flights_live.py`
  - Computes confirmed delay from actual vs scheduled times
  - Function: `update_flight_status()`

---

## 3. Database Schema (Updated)

### New Table: flight_status_live

Table created on API startup to store live operational status.

```
Columns:
- number_raw (VARCHAR) — flight number (PK part 1)
- flight_date (DATE) — local departure/arrival date (PK part 2)
- op_status (VARCHAR) — Scheduled / EnRoute / Landed / Cancelled / Diverted / Unknown
- etd_utc (TIMESTAMPTZ) — revised departure time
- atd_utc (TIMESTAMPTZ) — actual departure time
- eta_utc (TIMESTAMPTZ) — revised arrival time
- ata_utc (TIMESTAMPTZ) — actual arrival time
- confirmed_delay_min (FLOAT) — computed from actual vs scheduled
- fetched_at (TIMESTAMPTZ) — when this record was last polled

Rows: Same as flights_raw (grows via background refresh every 15 min)
```

---

## 4. API Endpoints (Complete Reference)

### 1. GET /health

Server status + background refresh state.

**Response:**
```json
{
  "status": "ok",
  "fids_last_ran": "2026-03-07T14:36:31.991000+00:00",
  "status_last_ran": "2026-03-07T14:36:45.123000+00:00",
  "fids_running": false,
  "status_running": false
}
```

---

### 2. POST /auth/login

Demo login for frontend authentication.

**Request:**
```json
{
  "username": "planner@muc",
  "password": "password123",
  "role": "planner"
}
```

**Response (200):**
```json
{
  "success": true,
  "token": "demo-token-planner@muc",
  "user": {
    "username": "planner@muc",
    "name": "planner@muc",
    "role": "planner"
  }
}
```

---

### 3. POST /predict/from-db

Main prediction endpoint with enriched operational status.

**Request:**
```json
{
  "number_raw": "LH 638",
  "sched_utc": "2026-03-05 02:35:00+05"
}
```

**Response (200):**
```json
{
  "p_delay_15": 0.73,
  "p_delay_30": 0.45,
  "pred_delay_15": 1,
  "pred_delay_30": 0,
  "minutes_pred": 18.5,
  "minutes_ui": 19,
  "thresholds": {
    "bin15": 0.5,
    "bin30": 0.5,
    "reg_delay_ge5": 5.0
  },
  "op_status": "EnRoute",
  "confirmed_delay_min": 15.5,
  "etd_utc": "2026-03-05T02:40:00+00:00",
  "atd_utc": "2026-03-05T02:48:00+00:00",
  "eta_utc": "2026-03-05T05:20:00+00:00",
  "ata_utc": null
}
```

**Status fields are null if `flight_status_live` not yet populated.**

---

### 4. GET /flights

List all flights with operational status.

**Query:**
```
GET /flights?date=2026-03-07
```

**Response:** Array of flights with full status data, joined from `flight_status_live`.

---

### 5. GET /flights/current

Recent flights for UI table.

**Query:**
```
GET /flights/current?limit=50
```

---

### 6. GET /weather/current

Current weather at Munich.

---

### 7. POST /simulate

What-if analysis: override weather, reactionary, or congestion and re-run model.

**Request:**
```json
{
  "number_raw": "LH 638",
  "sched_utc": "2026-03-05 02:35:00+05",
  "wind_speed_10m": 20,
  "precipitation": 5.0,
  "prev_delay_min_safe": 30
}
```

**Response:**
```json
{
  "baseline_prediction": { /* original model output */ },
  "simulated_prediction": { /* modified model output */ },
  "delta": {
    "p_delay_15_delta": 0.15,
    "p_delay_30_delta": 0.08,
    "minutes_delta": 5.2
  }
}
```

**Error (409) if flight landed/cancelled:**
```json
{
  "detail": "FLIGHT_LANDED"
}
```

---

## 5. Background Refresh & Live Data

### Scheduler Architecture

Once API starts, background scheduler runs in daemon threads:

**FIDS Refresh (every 2 hours)**
- Fetch live weather (Open-Meteo)
- Fetch live FIDS (Aerodatabox, 7-hour window)
- Rebuild intermediate & final feature tables
- Environment: `FIDS_INTERVAL_MINUTES` (default 120)

**Flight Status Refresh (every 15 minutes)**
- Poll Aerodatabox Flight Status API
- Update `flight_status_live` with actual times, status, onfirmed delays
- Wait for FIDS to complete first
- Environment: `FLIGHT_STATUS_INTERVAL_MINUTES` (default 15)

### How It Works

```
┌─ API Startup ─┐
├─ Load models  │
├─ Create flight_status_live
├─ Start Background Scheduler
│  ├─ FIDS Task (every 120 min) ──→ Update weather + flights + features
│  └─ Status Task (every 15 min) → Update operational status & times
│
└─ Serve API Requests
   ├─ /predict (enriched with flight_status_live)
   ├─ /flights (with status JOINs)
   └─ /simulate (what-if scenarios)
```

### Key Design Points

- **Non-blocking:** Scheduler in background; API remains responsive
- **Independent intervals:** FIDS and Status run on separate timers
- **State tracking:** `/health` exposes last run times
- **Error resilience:** Failed tasks log but don't crash
- **Thread-safe:** Shared state protected
- **Configurable:** Intervals via environment variables

---

## 6. Security Warnings

### ⚠️ Hardcoded API Credentials

Aerodatabox RapidAPI key is hardcoded in `ingest_flights_live.py`:

```python
RAPIDAPI_KEY = "e21b0e0c6dmsh590b13201fa4425p1c6ff5jsnf880e0aaeb16"
RAPIDAPI_HOST = "aerodatabox.p.rapidapi.com"
```

**For production:**
1. Move to environment variables or `.env` file
2. Use secrets management (AWS Secrets Manager, Vault)
3. Rotate keys regularly
4. Never commit keys to version control

---

## 7. Running the Project

### Installation

```bash
cd C:\Users\zeeni\Downloads\fyp-data-pipeline
python -m venv venv
venv\Scripts\activate.bat
pip install -r requirements.txt
```

### Running

```bash
# Full pipeline + API (recommended)
python start_delaypilot.py

# Monitor in browser
# - Swagger: http://localhost:8000/docs
# - Health: http://localhost:8000/health
```

### Environment Variables

```bash
# PostgreSQL (optional; defaults provided)
set PG_USER=postgres
set PG_PASSWORD=delaypilot2026
set PG_HOST=localhost
set PG_PORT=5432
set PG_DB=delaypilot_db

# Background scheduler intervals (optional)
set FIDS_INTERVAL_MINUTES=120
set FLIGHT_STATUS_INTERVAL_MINUTES=15
```

---

## Summary

**Key Improvements Over Initial Implementation:**

1. ✅ **Live-first data ingestion** (weather + flights)
2. ✅ **Background scheduler** for autonomous refresh
3. ✅ **Operational status enrichment** (flight_status_live table)
4. ✅ **What-if simulation** endpoint for operators
5. ✅ **Multiple endpoints** for data listing & monitoring
6. ✅ **CORS enabled** for frontend integration
7. ✅ **Authentication** scaffolding (demo login)
8. ✅ **Thread-safe** background operations
9. ✅ **Health monitoring** via /health endpoint
10. ⚠️ **Security concern:** Hardcoded API keys (needs remediation)

---

**For user-facing quick start guide, see [README.md](README.md).**  
**For original technical deep-dive, see [PIPELINE_EXPLAIN.md](PIPELINE_EXPLAIN.md).**
