"""
snapshot_delay_analytics.py
────────────────────────────────────────────────────────────────────────────
Runs as Step 6 in the background refresh cycle.

Reads the current fully-resolved flight state — same JOIN used by /flights
endpoint — applies the same 3-tier delay priority logic as predictionService.js
(Tier 1: confirmed API, Tier 2: FIDS observed, Tier 3: ML model), then
UPSERTS into flight_delay_snapshots.

WHY UPSERT NOT REPLACE:
  flights_raw and featured_muc_rxn_wx3_fe are REPLACED each cycle (live window).
  flight_delay_snapshots accumulates across cycles using PRIMARY KEY
  (number_raw, sched_utc) so the same flight row is updated in place —
  the dashboard charts therefore reflect the full day's history, not just
  the current FIDS window.
────────────────────────────────────────────────────────────────────────────
"""

import logging
import os
from datetime import datetime, timezone

from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS flight_delay_snapshots (
    number_raw          VARCHAR       NOT NULL,
    sched_utc           TIMESTAMPTZ   NOT NULL,
    movement            VARCHAR,
    airline_iata        VARCHAR,
    destination         VARCHAR,
    flight_date         DATE,
    resolved_delay_min  FLOAT,
    delay_source        VARCHAR,
    delay_status        VARCHAR,
    op_status           VARCHAR,
    ml_cause            VARCHAR,
    cause_scores        TEXT,
    wx_weather_code     FLOAT,
    wx_precipitation    FLOAT,
    ml_p_delay_15       FLOAT,
    ml_p_delay_30       FLOAT,
    last_updated_utc    TIMESTAMPTZ,
    PRIMARY KEY (number_raw, sched_utc)
)
"""

UPSERT_SQL = """
INSERT INTO flight_delay_snapshots (
    number_raw, sched_utc, movement, airline_iata, destination, flight_date,
    resolved_delay_min, delay_source, delay_status, op_status,
    ml_cause, cause_scores,
    wx_weather_code, wx_precipitation,
    ml_p_delay_15, ml_p_delay_30,
    last_updated_utc
) VALUES (
    :number_raw, :sched_utc, :movement, :airline_iata, :destination, :flight_date,
    :resolved_delay_min, :delay_source, :delay_status, :op_status,
    :ml_cause, :cause_scores,
    :wx_weather_code, :wx_precipitation,
    :ml_p_delay_15, :ml_p_delay_30,
    :last_updated_utc
)
ON CONFLICT (number_raw, sched_utc)
DO UPDATE SET
    resolved_delay_min  = EXCLUDED.resolved_delay_min,
    delay_source        = EXCLUDED.delay_source,
    delay_status        = EXCLUDED.delay_status,
    op_status           = EXCLUDED.op_status,
    ml_cause            = EXCLUDED.ml_cause,
    cause_scores        = EXCLUDED.cause_scores,
    wx_weather_code     = EXCLUDED.wx_weather_code,
    wx_precipitation    = EXCLUDED.wx_precipitation,
    ml_p_delay_15       = EXCLUDED.ml_p_delay_15,
    ml_p_delay_30       = EXCLUDED.ml_p_delay_30,
    last_updated_utc    = EXCLUDED.last_updated_utc
"""


def _resolve_delay(confirmed_delay, ml_minutes, fids_delay):
    """
    3-tier priority — mirrors predictionService.js mapFlight():
      Tier 1: confirmed_delay_min (Status API)  — always wins
      Tier 2: y_delay_min (FIDS)                — when < 0 (early) or >= 5 (delay)
      Tier 3: ml_minutes_ui (model)             — when FIDS on-time (0–4) and ML >= 5
    """
    if confirmed_delay is not None:
        return float(confirmed_delay), 'confirmed'

    fids = float(fids_delay) if fids_delay is not None else None
    ml   = float(ml_minutes) if ml_minutes  is not None else None

    if fids is not None and (fids < 0 or fids >= 5):
        return fids, 'fids'
    if ml is not None and ml >= 5:
        return ml, 'model'
    if fids is not None:
        return fids, 'fids'
    if ml is not None:
        return ml, 'model'
    return None, 'fids'


def _derive_status(resolved_delay, delay_source, op_status):
    """Mirrors deriveStatus() in predictionService.js."""
    if op_status in ('Landed', 'Cancelled', 'Diverted'):
        return op_status
    if op_status == 'EnRoute':
        if resolved_delay is not None:
            if resolved_delay >= 30: return 'Major Delay'
            if resolved_delay >= 5:  return 'Minor Delay'
            if resolved_delay < 0:   return 'Early'
        return 'En Route'
    if resolved_delay is None:
        return 'On Time'
    if delay_source in ('confirmed', 'fids'):
        if resolved_delay >= 30: return 'Major Delay'
        if resolved_delay >= 5:  return 'Minor Delay'
        if resolved_delay < 0:   return 'Early'
        return 'On Time'
    else:  # model
        if resolved_delay >= 30: return 'Major Delay'
        if resolved_delay >= 5:  return 'Minor Delay'
        return 'On Time'


def _get_engine():
    pg_user     = os.getenv("PG_USER",     "postgres")
    pg_password = os.getenv("PG_PASSWORD", "delaypilot2026")
    pg_host     = os.getenv("PG_HOST",     "localhost")
    pg_port     = os.getenv("PG_PORT",     "5432")
    pg_db       = os.getenv("PG_DB",       "delaypilot_db")
    return create_engine(
        f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    )


def snapshot_delay_analytics() -> None:
    engine = _get_engine()

    # ── 1. Ensure table exists ────────────────────────────────────────────────
    with engine.connect() as conn:
        conn.execute(text(CREATE_TABLE_SQL))
        conn.commit()

    # ── 2. Read current resolved state (same JOIN as /flights) ────────────────
    q = text("""
        SELECT
            f.number_raw,
            f.sched_utc,
            f.movement,
            f.airline_iata,
            f.other_airport_icao                                      AS destination,
            DATE(f.sched_utc AT TIME ZONE 'Europe/Berlin')            AS flight_date,
            s.confirmed_delay_min,
            p.minutes_ui                                              AS ml_minutes_ui,
            f.y_delay_min,
            COALESCE(s.op_status, 'Scheduled')                        AS op_status,
            p.ml_cause,
            p.cause_scores,
            f.wx_muc_weather_code,
            f.wx_muc_precipitation,
            p.p_delay_15,
            p.p_delay_30
        FROM featured_muc_rxn_wx3_fe f
        LEFT JOIN flight_status_live s
               ON s.number_raw  = f.number_raw
              AND s.flight_date = DATE(f.sched_utc AT TIME ZONE 'UTC')
        LEFT JOIN flight_predictions p
               ON p.number_raw = f.number_raw
              AND p.sched_utc  = f.sched_utc
        WHERE f.number_raw IS NOT NULL
          AND f.sched_utc  IS NOT NULL
    """)

    with engine.connect() as conn:
        rows = conn.execute(q).fetchall()

    if not rows:
        logger.warning("[snapshot] No flights found — skipping snapshot.")
        return

    # ── 3. Resolve delay tier + status ───────────────────────────────────────
    now_utc = datetime.now(timezone.utc)
    records = []
    for r in rows:
        (number_raw, sched_utc, movement, airline_iata, destination, flight_date,
         confirmed_delay, ml_minutes, fids_delay, op_status,
         ml_cause, cause_scores,
         wx_weather_code, wx_precipitation,
         ml_p15, ml_p30) = r

        resolved_delay, delay_source = _resolve_delay(confirmed_delay, ml_minutes, fids_delay)
        delay_status = _derive_status(resolved_delay, delay_source, op_status)

        records.append({
            "number_raw":         str(number_raw),
            "sched_utc":          sched_utc,
            "movement":           movement,
            "airline_iata":       airline_iata,
            "destination":        destination,
            "flight_date":        flight_date,
            "resolved_delay_min": resolved_delay,
            "delay_source":       delay_source,
            "delay_status":       delay_status,
            "op_status":          op_status,
            "ml_cause":           str(ml_cause) if ml_cause else None,
            "cause_scores":       str(cause_scores) if cause_scores else None,
            "wx_weather_code":    float(wx_weather_code)  if wx_weather_code  is not None else None,
            "wx_precipitation":   float(wx_precipitation) if wx_precipitation is not None else None,
            "ml_p_delay_15":      float(ml_p15) if ml_p15 is not None else None,
            "ml_p_delay_30":      float(ml_p30) if ml_p30 is not None else None,
            "last_updated_utc":   now_utc,
        })

    # ── 4. Upsert directly (no staging table) ────────────────────────────────
    with engine.connect() as conn:
        conn.execute(text(UPSERT_SQL), records)
        conn.commit()

    delayed = sum(1 for r in records if r["delay_status"] in ("Minor Delay", "Major Delay"))
    logger.info(
        "[snapshot] Upserted %d flights into flight_delay_snapshots (%d delayed).",
        len(records), delayed,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    snapshot_delay_analytics()