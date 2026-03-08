from datetime import datetime
from typing import Any, Dict, Optional

import logging
import re
import os

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, text

from typing import Optional
from fastapi import Query

from model_service import V3FinalModelService


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_engine():
    pg_user = os.getenv("PG_USER", "postgres")
    pg_password = os.getenv("PG_PASSWORD", "delaypilot2026")
    pg_host = os.getenv("PG_HOST", "localhost")
    pg_port = os.getenv("PG_PORT", "5432")
    pg_db = os.getenv("PG_DB", "delaypilot_db")
    url = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    return create_engine(url)


app = FastAPI(title="DelayPilot Prediction API", version="v1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5000"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# Allow frontend (e.g. React/Vue) running on a different origin to call the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # for FYP/demo; in production, restrict to known origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Singleton model service (loaded once at startup)
model_service = V3FinalModelService(models_dir=str(os.path.join(os.path.dirname(__file__), "models")))
engine = get_engine()
# ── Background refresh scheduler ─────────────────────────────
from background_refresh import start_background_refresh, get_refresh_state

@app.on_event("startup")
def on_startup():
    # Ensure flight_status_live exists before any request tries to JOIN it
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS flight_status_live (
                number_raw          VARCHAR,
                flight_date         DATE,
                op_status           VARCHAR,
                etd_utc             TIMESTAMPTZ,
                atd_utc             TIMESTAMPTZ,
                eta_utc             TIMESTAMPTZ,
                ata_utc             TIMESTAMPTZ,
                confirmed_delay_min FLOAT,
                fetched_at          TIMESTAMPTZ,
                PRIMARY KEY (number_raw, flight_date)
            )
        """))
        conn.commit()
    start_background_refresh()

class DbPredictionRequest(BaseModel):
    """
    Request for prediction using a flight row stored in Postgres.

    For now we look up by:
      - number_raw (flight number string as in Aerodatabox, e.g. 'LH1234')
      - sched_utc (ISO-8601 string, same as stored in featured_muc_rxn_wx3_fe)

    In your real app, you may switch to an explicit flight_id key.
    """

    number_raw: str
    # Accept as string so we can flexibly parse formats from UI/DB
    sched_utc: str


def _parse_sched_utc(value: str) -> datetime:
    """
    Parse sched_utc from a few common string formats into a timezone-aware datetime.

    Accepts:
      - '2026-03-05T02:35:00+00:00'
      - '2026-03-05 02:35:00+00:00'
      - '2026-03-05 02:35:00+05' (will be normalized to +05:00)
    """
    s = (value or "").strip()
    if not s:
        raise ValueError("sched_utc is empty")

    # Allow space instead of 'T'
    if " " in s and "T" not in s:
        # only replace the first space between date and time
        s = s.replace(" ", "T", 1)

    # Normalize timezone like +05 or -03 to +05:00 / -03:00
    m = re.match(r"(.+)([+-]\d{2})$", s)
    if m:
        base, tz_hour = m.groups()
        s = f"{base}{tz_hour}:00"

    return datetime.fromisoformat(s)


# AFTER
class PredictionResponse(BaseModel):
    p_delay_15: float
    p_delay_30: float
    pred_delay_15: int
    pred_delay_30: int
    minutes_pred: float
    minutes_ui: float
    thresholds: Dict[str, Any]
    # Status fields from flight_status_live (None when table not yet populated)
    op_status:           Optional[str]   = None
    confirmed_delay_min: Optional[float] = None
    etd_utc:             Optional[str]   = None
    atd_utc:             Optional[str]   = None
    eta_utc:             Optional[str]   = None
    ata_utc:             Optional[str]   = None


class FlightListItem(BaseModel):
    """Lightweight flight representation for the frontend flights table."""

    number_raw: str
    airline_icao: str | None = None
    movement: str
    route: str
    sched_utc: datetime
    status: str
    predicted_delay_min: float


class LoginRequest(BaseModel):
    username: str
    password: str
    role: str


class LoginUser(BaseModel):
    username: str
    name: str
    role: str


class LoginResponse(BaseModel):
    success: bool
    token: str
    user: LoginUser
    message: Optional[str] = None


@app.post("/auth/login", response_model=LoginResponse)
def login(req: LoginRequest) -> LoginResponse:
    """
    Simple demo login endpoint for the DelayPilot frontend.

    For FYP purposes, we:
      - Accept any username/password that meet the minimum length checks
      - Echo back a fake JWT token and basic user info

    In a real system, this would be replaced with proper authentication.
    """
    logger.info("Login attempt for username=%s role=%s", req.username, req.role)

    if len(req.username) < 4 or len(req.password) < 8:
        logger.warning("Login failed due to validation for username=%s", req.username)
        raise HTTPException(status_code=401, detail="Invalid username or password")

    # Fake token and user; good enough for UI routing and role-based nav
    token = f"demo-token-{req.username}"
    user = LoginUser(username=req.username, name=req.username, role=req.role)

    logger.info("Login succeeded for username=%s role=%s", req.username, req.role)
    return LoginResponse(success=True, token=token, user=user)


@app.get("/flights/current", response_model=list[FlightListItem])
def list_current_flights(limit: int = 200) -> list[FlightListItem]:
    """
    Return a list of recent flights from featured_muc_rxn_wx3_fe for the frontend.

    For simplicity we:
      - sort by sched_utc descending (most recent first)
      - compute a simple status based on y_delay_min
    """
    logger.info("Listing current flights (limit=%s)", limit)

    with engine.connect() as conn:
        query = text(
            """
            SELECT
                number_raw,
                COALESCE(airline_icao, airline_iata, '') AS airline_icao,
                movement,
                other_airport_icao,
                sched_utc,
                y_delay_min
            FROM featured_muc_rxn_wx3_fe
            WHERE number_raw IS NOT NULL
              AND sched_utc IS NOT NULL
            ORDER BY sched_utc DESC
            LIMIT :limit
            """
        )
        rows = conn.execute(query, {"limit": limit}).mappings().all()

    flights: list[FlightListItem] = []
    for r in rows:
        delay_min = float(r.get("y_delay_min") or 0.0)
        movement = (r.get("movement") or "").lower()
        other_airport = (r.get("other_airport_icao") or "").upper()

        # Simple route string for UI
        if movement == "departure":
            route = f"MUC → {other_airport}" if other_airport else "MUC → ?"
        else:
            route = f"{other_airport} → MUC" if other_airport else "? → MUC"

        # Simple status buckets for UI
        if delay_min <= 0:
            status = "On-Time"
        elif delay_min <= 15:
            status = "Minor Delay"
        elif delay_min <= 60:
            status = "Major Delay"
        else:
            status = "Major Delay"

        flights.append(
            FlightListItem(
                number_raw=r["number_raw"],
                airline_icao=r.get("airline_icao") or None,
                movement=movement,
                route=route,
                sched_utc=r["sched_utc"],
                status=status,
                predicted_delay_min=delay_min,
            )
        )

    return flights


@app.post("/predict/from-db", response_model=PredictionResponse)
def predict_from_db(req: DbPredictionRequest):
    """
    Predict using a row from `featured_muc_rxn_wx3_fe` in Postgres.

    This is mainly for integration testing with the historical pipeline.
    Later you can add a separate endpoint that builds features from live APIs.
    """
    logger.info(
        "Received prediction request number_raw=%s sched_utc=%s",
        req.number_raw,
        req.sched_utc,
    )

    try:
        # Parse sched_utc string into a proper datetime that matches DB storage
        try:
            sched_dt = _parse_sched_utc(req.sched_utc)
        except Exception as exc:
            logger.warning("Invalid sched_utc format '%s': %s", req.sched_utc, exc)
            raise HTTPException(status_code=400, detail=f"Invalid sched_utc format: {exc}")

        with engine.connect() as conn:
            query = text(
                """
                SELECT *
                FROM featured_muc_rxn_wx3_fe
                WHERE number_raw = :number_raw
                  AND sched_utc = :sched_utc
                LIMIT 1
                """
            )
            row = conn.execute(
                query,
                {
                    "number_raw": req.number_raw,
                    "sched_utc": sched_dt,
                },
            ).mappings().first()

        if row is None:
            logger.info(
                "No matching flight found in featured_muc_rxn_wx3_fe for number_raw=%s sched_utc=%s",
                req.number_raw,
                sched_dt.isoformat(),
            )
            raise HTTPException(
                status_code=404,
                detail="No matching flight found in featured_muc_rxn_wx3_fe for the given number_raw and sched_utc.",
            )

        # Convert row mapping to DataFrame with a single row
        row_dict = dict(row)
        df = pd.DataFrame([row_dict])

        logger.info(
            "Running model prediction for number_raw=%s sched_utc=%s",
            req.number_raw,
            sched_dt.isoformat(),
        )
        # AFTER
        result = model_service.predict_one(df)
        logger.info(
            "Prediction succeeded for number_raw=%s sched_utc=%s",
            req.number_raw,
            sched_dt.isoformat(),
        )

        # Patch C — enrich with confirmed delay from flight_status_live
        try:
            status_q = text("""
                SELECT op_status, confirmed_delay_min, etd_utc, atd_utc, eta_utc, ata_utc
                FROM flight_status_live
                WHERE number_raw  = :number_raw
                  AND flight_date = DATE(:sched_utc AT TIME ZONE 'UTC')
                LIMIT 1
            """)
            with engine.connect() as conn:
                srow = conn.execute(status_q, {
                    "number_raw": req.number_raw,
                    "sched_utc":  sched_dt,
                }).fetchone()

            if srow:
                result["op_status"]           = srow[0]
                result["confirmed_delay_min"] = float(srow[1]) if srow[1] is not None else None
                result["etd_utc"]             = str(srow[2])   if srow[2] is not None else None
                result["atd_utc"]             = str(srow[3])   if srow[3] is not None else None
                result["eta_utc"]             = str(srow[4])   if srow[4] is not None else None
                result["ata_utc"]             = str(srow[5])   if srow[5] is not None else None
            else:
                result["op_status"]           = "Scheduled"
                result["confirmed_delay_min"] = None
        except Exception as status_exc:
            # Non-fatal — flight_status_live may not exist yet
            logger.warning("Could not fetch flight status for prediction: %s", status_exc)
            result["op_status"] = None
            result["confirmed_delay_min"] = None

        return PredictionResponse(**result)

    except HTTPException:
        # Already a well-formed API error; just propagate.
        raise
    except Exception as exc:
        logger.exception(
            "Prediction failed due to unexpected error for number_raw=%s sched_utc=%s",
            req.number_raw,
            req.sched_utc,
        )
        raise HTTPException(status_code=500, detail="Internal prediction error") from exc


# Replace existing /health with this
@app.get("/health")
def health() -> Dict[str, Any]:
    from background_refresh import get_refresh_state
    state = get_refresh_state()
    return {
        "status":          "ok",
        "last_ran":        str(state["last_ran"])        if state["last_ran"]        else "pending",
        "fids_last_ran":   str(state["fids_last_ran"])   if state["fids_last_ran"]   else "pending",
        "status_last_ran": str(state["status_last_ran"]) if state["status_last_ran"] else "pending",
        "running":         state["running"],
    }

# NEW ENDPOINTS ADDED
"""
api_main.py  — PATCH for /flights endpoint
───────────────────────────────────────────
Replace the existing @app.get("/flights") function in api_main.py
with the version below.

What changes:
  • JOINs flights_raw to get dep_best_utc / arr_best_utc
    (the "actual / estimated" times Aerodatabox provides).
  • Exposes them as actual_utc in the JSON response.
  • Keeps all existing fields intact.
"""

# ── REPLACE the existing /flights route with this ──────────────────

@app.get("/flights")
def get_flights(date: str = None):
    """
    Return all flights in featured_muc_rxn_wx3_fe,
    enriched with actual/revised times and op_status from flight_status_live.

    op_status values: Scheduled | EnRoute | Landed | Cancelled | Diverted | Unknown
    If no status record exists, op_status defaults to 'Scheduled'.
    """
    try:
        date_filter = ""
        params = {}
        if date:
            date_filter = "WHERE DATE(f.sched_utc AT TIME ZONE 'UTC') = :date"
            params["date"] = date

        q = text(f"""
            SELECT
                f.number_raw,
                f.sched_utc,
                f.movement,
                f.airline_iata,
                f.other_airport_icao   AS destination,
                f.y_delay_min,
                f.y_bin15,
                f.y_bin30,
                -- Weather fields for cause derivation in UI
                f.wx_muc_weather_code,
                f.wx_muc_precipitation,
                -- Actual time from flights_raw (best estimate from FIDS)
                CASE
                    WHEN f.movement = 'departure' THEN r.dep_best_utc
                    ELSE r.arr_best_utc
                END AS actual_utc,
                -- Status from flight_status_live (authoritative)
                COALESCE(s.op_status, 'Scheduled')  AS op_status,
                s.etd_utc,
                s.atd_utc,
                s.eta_utc,
                s.ata_utc,
                s.confirmed_delay_min,
                -- ML model predictions from flight_predictions table
                p.minutes_ui        AS ml_minutes_ui,
                p.p_delay_15        AS ml_p_delay_15,
                p.p_delay_30        AS ml_p_delay_30,
                p.pred_delay_15     AS ml_pred_delay_15,
                p.pred_delay_30     AS ml_pred_delay_30,
                p.ml_cause          AS ml_cause,
                p.cause_scores      AS cause_scores
            FROM featured_muc_rxn_wx3_fe f
            LEFT JOIN flights_raw r
                   ON r.number_raw = f.number_raw
                  AND (
                      (f.movement = 'departure' AND r.dep_sched_utc = f.sched_utc)
                   OR (f.movement = 'arrival'   AND r.arr_sched_utc = f.sched_utc)
                  )
            LEFT JOIN flight_status_live s
                   ON s.number_raw  = f.number_raw
                  AND s.flight_date = DATE(f.sched_utc AT TIME ZONE 'UTC')
            LEFT JOIN flight_predictions p
                   ON p.number_raw = f.number_raw
                  AND p.sched_utc  = f.sched_utc
            {date_filter}
            ORDER BY f.sched_utc
        """)

        with engine.connect() as conn:
            rows = conn.execute(q, params).fetchall()

        return [
            {
                "number_raw":            r[0],
                "sched_utc":             str(r[1]),
                "movement":              r[2],
                "airline_iata":          r[3],
                "destination":           r[4],
                "delay_min":             float(r[5]) if r[5] is not None else None,
                "is_delayed_15":         bool(r[6])  if r[6] is not None else None,
                "is_delayed_30":         bool(r[7])  if r[7] is not None else None,
                # Weather for cause derivation
                "wx_muc_weather_code":   float(r[8]) if r[8] is not None else None,
                "wx_muc_precipitation":  float(r[9]) if r[9] is not None else None,
                "actual_utc":            str(r[10])  if r[10] is not None else None,
                # Status fields
                "op_status":             r[11],
                "etd_utc":               str(r[12])  if r[12] is not None else None,
                "atd_utc":               str(r[13])  if r[13] is not None else None,
                "eta_utc":               str(r[14])  if r[14] is not None else None,
                "ata_utc":               str(r[15])  if r[15] is not None else None,
                "confirmed_delay_min":   float(r[16]) if r[16] is not None else None,
                # ML model predictions from flight_predictions table
                "ml_minutes_ui":         float(r[17]) if r[17] is not None else None,
                "ml_p_delay_15":         float(r[18]) if r[18] is not None else None,
                "ml_p_delay_30":         float(r[19]) if r[19] is not None else None,
                "ml_pred_delay_15":      bool(r[20])  if r[20] is not None else None,
                "ml_pred_delay_30":      bool(r[21])  if r[21] is not None else None,
                "ml_cause":              r[22],
                "cause_scores":          r[23],   # JSON string {"Weather (MUC)": 45, ...}
            }
            for r in rows
        ]

    except Exception as e:
        logger.error("/flights error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

"""
api_main.py  — ADD /simulate endpoint
──────────────────────────────────────────────────────────────────
How it works:
  1. Loads the real feature row for the flight from featured_muc_rxn_wx3_fe
  2. Overlays ONLY the parameters the user changed (overrides dict)
  3. Re-runs predict_one() with the modified row
  4. Returns baseline prediction + simulated prediction side-by-side
  5. Also returns a reactionary impact estimate for connected flights

The user only touches the parameters they care about.
Everything else stays true to the real pipeline data.
"""

class SimulateRequest(BaseModel):
    number_raw: str
    sched_utc:  str

    # ── Weather overrides (MUC) ─────────────────────────────────
    # These map directly to weather_hourly columns joined at ref_hour_utc
    wind_speed_10m:     Optional[float] = None   # km/h
    wind_gusts_10m:     Optional[float] = None   # km/h
    precipitation:      Optional[float] = None   # mm
    snowfall:           Optional[float] = None   # mm
    visibility:         Optional[float] = None   # metres (Open-Meteo unit)
    weather_code:       Optional[int]   = None   # WMO code

    # ── Reactionary override ────────────────────────────────────
    # prev_delay_min_safe: delay of previous rotation of the same aircraft
    prev_delay_min_safe: Optional[float] = None  # minutes

    # ── Congestion overrides ────────────────────────────────────
    # These are rolling counts of flights at MUC around sched_utc
    # muc_arr_1h / muc_dep_1h — arrivals/departures in ±1h window
    muc_arr_1h: Optional[float] = None
    muc_dep_1h: Optional[float] = None


@app.post("/simulate")
def simulate_flight(req: SimulateRequest):
    """
    Load a real flight's feature row, apply user overrides,
    run the model, and return baseline vs simulated predictions.
    """
    try:
        # ── 1. Parse sched_utc ──────────────────────────────────
        from dateutil import parser as dtparser
        import pytz, pandas as pd, numpy as np

        try:
            sched_dt = dtparser.parse(req.sched_utc)
            if sched_dt.tzinfo is None:
                sched_dt = pytz.utc.localize(sched_dt)
        except Exception as e:
            raise HTTPException(status_code=422, detail=f"Invalid sched_utc: {e}")

        # ── 2. Load feature row from DB ─────────────────────────
        q = text("""
            SELECT * FROM featured_muc_rxn_wx3_fe
            WHERE number_raw = :number_raw
              AND sched_utc  = :sched_utc
            LIMIT 1
        """)
        with engine.connect() as conn:
            row = conn.execute(q, {"number_raw": req.number_raw, "sched_utc": sched_dt}).fetchone()

        if not row:
            raise HTTPException(status_code=404, detail=f"Flight {req.number_raw} @ {req.sched_utc} not found.")

        # ── 3. Landed guard (uses flight_status_live for authoritative status)
        status_q = text("""
            SELECT op_status
            FROM flight_status_live
            WHERE number_raw  = :number_raw
              AND flight_date = DATE(:sched_utc AT TIME ZONE 'UTC')
            LIMIT 1
        """)
        with engine.connect() as conn:
            status_row = conn.execute(status_q, {
                "number_raw": req.number_raw,
                "sched_utc":  sched_dt,
            }).fetchone()

        # Block simulation only for definitively completed or cancelled flights.
        # Scheduled, EnRoute, Unknown, and missing → allow simulation.
        if status_row and status_row[0] in ("Landed", "Cancelled", "Diverted"):
            raise HTTPException(
                status_code=409,
                detail="FLIGHT_LANDED",   # frontend checks this exact string
            )

        # ── 4. Build baseline DataFrame ─────────────────────────
        columns = list(row._mapping.keys())
        values  = list(row._mapping.values())
        df_base = pd.DataFrame([values], columns=columns)

        # ── 5. Baseline prediction ──────────────────────────────
        baseline = model_service.predict_one(df_base)

        # ── 6. Build override DataFrame ─────────────────────────
        df_sim = df_base.copy()

        # Weather column name mapping (pipeline names vary slightly)
        # Open-Meteo uses these column names after normalization
        weather_map = {
            "wind_speed_10m":  ["wx_muc_wind_speed_10m",  "wind_speed_10m"],
            "wind_gusts_10m":  ["wx_muc_wind_gusts_10m",  "wind_gusts_10m"],
            "precipitation":   ["wx_muc_precipitation",   "precipitation"],
            "snowfall":        ["wx_muc_snowfall",         "snowfall"],
            "visibility":      ["wx_muc_visibility",       "visibility"],
            "weather_code":    ["wx_muc_weather_code",     "weather_code"],
        }

        def apply_override(df, candidates, value):
            """Try each candidate column name; apply to the first found."""
            for col in candidates:
                if col in df.columns:
                    df[col] = value
                    # Also update derived weather flags if wind changes
                    if "wind" in col:
                        if "flag_strong_wind_muc" in df.columns:
                            df["flag_strong_wind_muc"] = int(value >= 50)
                        if "flag_gusts_muc" in df.columns:
                            df["flag_gusts_muc"] = int(value >= 70)
                    if "precipitation" in col or "snowfall" in col:
                        if "flag_any_precip_muc" in df.columns:
                            df["flag_any_precip_muc"] = int(value > 0)
                        if "flag_any_snow_muc" in df.columns and "snow" in col:
                            df["flag_any_snow_muc"] = int(value > 0)
                    return True
            return False

        override_applied = {}

        for field, candidates in weather_map.items():
            val = getattr(req, field)
            if val is not None:
                if apply_override(df_sim, candidates, val):
                    override_applied[field] = val

        # Reactionary override
        if req.prev_delay_min_safe is not None:
            for col in ["prev_delay_min_safe", "reactionary_delay_min"]:
                if col in df_sim.columns:
                    df_sim[col] = req.prev_delay_min_safe
                    override_applied["prev_delay_min_safe"] = req.prev_delay_min_safe
                    break

        # Congestion overrides
        if req.muc_arr_1h is not None:
            for col in ["muc_arr_1h", "arr_1h"]:
                if col in df_sim.columns:
                    df_sim[col] = req.muc_arr_1h
                    override_applied["muc_arr_1h"] = req.muc_arr_1h
                    break

        if req.muc_dep_1h is not None:
            for col in ["muc_dep_1h", "dep_1h"]:
                if col in df_sim.columns:
                    df_sim[col] = req.muc_dep_1h
                    override_applied["muc_dep_1h"] = req.muc_dep_1h
                    break

        # ── 7. Simulated prediction ─────────────────────────────
        simulated = model_service.predict_one(df_sim)

        # ── 8. Reactionary impact estimate ─────────────────────
        # Estimate how many same-airline flights scheduled within +3h
        # could be affected by this flight's simulated delay.
        # We query flights_raw for flights of same airline after sched_utc.
        airline_iata = str(df_base.get("airline_iata", pd.Series([""])).iloc[0])
        reactionary_q = text("""
            SELECT number_raw, dep_sched_utc AS sched
            FROM flights_raw
            WHERE airline_iata = :airline
              AND dep_sched_utc > :sched_utc
              AND dep_sched_utc < :sched_utc + INTERVAL '3 hours'
            ORDER BY dep_sched_utc
            LIMIT 5
        """)
        with engine.connect() as conn:
            rrows = conn.execute(reactionary_q, {
                "airline": airline_iata,
                "sched_utc": sched_dt
            }).fetchall()

        sim_delay = simulated.get("minutes_ui", 0) or 0
        reactionary_impacts = []
        for i, rr in enumerate(rrows):
            # Reactionary delay decays: each downstream flight gets ~60% of upstream
            decay   = 0.6 ** (i + 1)
            r_delay = round(sim_delay * decay)
            if r_delay >= 5:
                reactionary_impacts.append({
                    "flight":    rr[0],
                    "sched":     str(rr[1]),
                    "added_min": r_delay,
                    "severity":  "high" if r_delay >= 25 else "moderate" if r_delay >= 12 else "minor",
                })

        # ── 9. Return ───────────────────────────────────────────
        return {
            "flight":     req.number_raw,
            "sched_utc":  req.sched_utc,
            "overrides":  override_applied,
            "baseline":   baseline,
            "simulated":  simulated,
            "delta": {
                "p_delay_15":  round(simulated["p_delay_15"]  - baseline["p_delay_15"],  4),
                "p_delay_30":  round(simulated["p_delay_30"]  - baseline["p_delay_30"],  4),
                "minutes_ui":  round((simulated.get("minutes_ui") or 0) - (baseline.get("minutes_ui") or 0), 1),
            },
            "reactionary_impact": reactionary_impacts,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"/simulate error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Simulation failed: {str(e)}")

@app.get("/weather/current")
def current_weather():
    """Returns latest weather for MUC (EDDM) from weather_hourly table."""
    try:
        q = """
            SELECT hour_utc, temperature_2m, wind_speed_10m,
                   precipitation, visibility, weather_code
            FROM weather_hourly
            WHERE airport_icao = 'EDDM'
            ORDER BY hour_utc DESC
            LIMIT 1
        """
        with engine.connect() as conn:
            row = conn.execute(text(q)).fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="No weather data")

        return {
            "timestamp": str(row[0]),
            "temperature": row[1],
            "wind_speed": row[2],
            "precipitation": row[3],
            "visibility": row[4],
            "weather_code": row[5],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"/weather/current error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch weather")


# For running with: python api_main.py (useful in dev)
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api_main:app", host="0.0.0.0", port=8000, reload=True)