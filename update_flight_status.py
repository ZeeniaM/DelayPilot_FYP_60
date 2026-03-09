"""
update_flight_status.py
────────────────────────────────────────────────────────────────────────────
Polls the AeroDataBox Flight Status API (GET /flights/number/{flightNo}/{date})
for every flight in the current flights_raw window and stores the results
in a lightweight table: flight_status_live.

This is a NEW additive script — it does NOT modify flights_raw or any
feature tables. Everything downstream stays safe.

TABLE: flight_status_live
  number_raw          VARCHAR   — matches flights_raw.number_raw (e.g. "LH 638")
  flight_date         DATE      — local departure/arrival date (YYYY-MM-DD)
  op_status           VARCHAR   — Scheduled / EnRoute / Landed / Cancelled / Diverted / Unknown
  etd_utc             TIMESTAMPTZ  — revised departure (if available)
  atd_utc             TIMESTAMPTZ  — actual departure (if available)
  eta_utc             TIMESTAMPTZ  — revised arrival (if available)
  ata_utc             TIMESTAMPTZ  — actual arrival (if available)
  confirmed_delay_min FLOAT        — computed from actual vs scheduled; NULL if not available
  fetched_at          TIMESTAMPTZ  — when this record was last polled

HOW TO USE:
  Called automatically by refresh_live_data.py after FIDS ingestion.
  Can also be run standalone:
      python update_flight_status.py

API USED:
  GET https://aerodatabox.p.rapidapi.com/flights/number/{flightNo}/{dateLocal}
  Tier 2 — costs API quota per call.
  We batch-deduplicate by (number_raw, date) to minimise calls.
────────────────────────────────────────────────────────────────────────────
"""

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pandas as pd
import requests
from sqlalchemy import create_engine, text

# Re-use the same credentials already hard-coded in ingest_flights_live
from ingest_flights_live import RAPIDAPI_KEY, RAPIDAPI_HOST, parse_utc

logger = logging.getLogger(__name__)


# ── DB connection ────────────────────────────────────────────────────────────

def get_engine():
    pg_user     = os.getenv("PG_USER",     "postgres")
    pg_password = os.getenv("PG_PASSWORD", "delaypilot2026")
    pg_host     = os.getenv("PG_HOST",     "localhost")
    pg_port     = os.getenv("PG_PORT",     "5432")
    pg_db       = os.getenv("PG_DB",       "delaypilot_db")
    url = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    return create_engine(url)


# ── AeroDataBox Flight Status call ───────────────────────────────────────────

def fetch_flight_status(flight_no_clean: str, date_local: str) -> Optional[Dict[str, Any]]:
    """
    Call GET /flights/number/{flightNo}/{dateLocal}
    flight_no_clean: flight number WITHOUT space, e.g. "LH638"
    date_local:      YYYY-MM-DD

    Returns the first matching flight dict, or None on error.
    """
    url = f"https://{RAPIDAPI_HOST}/flights/number/{flight_no_clean}/{date_local}"
    headers = {
        "Accept": "application/json",
        "X-RapidAPI-Key":  RAPIDAPI_KEY,
        "X-RapidAPI-Host": RAPIDAPI_HOST,
    }
    MAX_RETRIES = 3
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 404:
                logger.debug("Flight not found: %s on %s", flight_no_clean, date_local)
                return None
            if resp.status_code == 429:
                wait = 10 * attempt   # back-off: 10s, 20s, 30s
                logger.warning("Rate limited by AeroDataBox — pausing %ds (attempt %d)", wait, attempt)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list) and len(data) > 0:
                return data[0]
            return None

        except requests.exceptions.ConnectionError as e:
            # DNS resolution failure or connection reset — transient network issue.
            # Retry up to MAX_RETRIES with exponential back-off before giving up.
            wait = 2 ** attempt   # 2s, 4s, 8s
            if attempt < MAX_RETRIES:
                logger.debug(
                    "Flight Status connection error for %s (attempt %d/%d) — retrying in %ds: %s",
                    flight_no_clean, attempt, MAX_RETRIES, wait, e,
                )
                time.sleep(wait)
            else:
                logger.warning(
                    "Flight Status API error for %s on %s: %s",
                    flight_no_clean, date_local, e,
                )
                return None

        except requests.exceptions.Timeout:
            logger.warning("Flight Status API timeout for %s on %s (attempt %d)", flight_no_clean, date_local, attempt)
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
            else:
                return None

        except requests.RequestException as e:
            logger.warning("Flight Status API error for %s on %s: %s", flight_no_clean, date_local, e)
            return None

    return None


# ── Parse a flight status response dict ─────────────────────────────────────

def parse_status_record(
    number_raw: str,
    flight_date: str,
    flight_dict: Dict[str, Any],
    movement: str = "arrival",       # "arrival" or "departure" relative to MUC
) -> Dict[str, Any]:
    """
    Extract the fields we care about from one AeroDataBox flight status dict.

    MOVEMENT-AWARE DELAY COMPUTATION
    ─────────────────────────────────
    The key insight: `sched_utc` in featured_muc_rxn_wx3_fe is always the
    MUC-perspective scheduled time:
      - departure flight  → dep_sched_utc  (when it leaves MUC)
      - arrival flight    → arr_sched_utc  (when it arrives at MUC)

    So `confirmed_delay_min` must be computed relative to the same reference:
      - departure from MUC → use ATD vs dep_sched  (departure delay)
      - arrival   to  MUC → use ATA vs arr_sched   (arrival delay at MUC)

    Never mix them: an arrival flight's ATD is the departure delay from the
    origin airport, which is a different value and a different time reference.

    AeroDataBox Flight Status response shape (relevant fields):
    {
      "status": "Landed",
      "departure": {
        "scheduledTime": {"utc": "...", "local": "..."},
        "revisedTime":   {"utc": "...", "local": "..."},  ← ETD
        "actualTime":    {"utc": "...", "local": "..."},  ← ATD
        "airport": { "icao": "EDDM" / origin }
      },
      "arrival": {
        "scheduledTime": {"utc": "...", "local": "..."},
        "revisedTime":   {"utc": "...", "local": "..."},  ← ETA
        "actualTime":    {"utc": "...", "local": "..."},  ← ATA
        "airport": { "icao": "EDDM" / destination }
      }
    }
    """
    dep = flight_dict.get("departure") or {}
    arr = flight_dict.get("arrival")   or {}

    def get_utc_ts(section: dict, field: str) -> Optional[pd.Timestamp]:
        raw = (section.get(field) or {}).get("utc")
        if not raw:
            return None
        ts = parse_utc(raw)
        return ts if not pd.isna(ts) else None

    op_status = flight_dict.get("status", "Unknown")

    dep_sched = get_utc_ts(dep, "scheduledTime")
    etd       = get_utc_ts(dep, "revisedTime")
    atd       = get_utc_ts(dep, "actualTime")

    arr_sched = get_utc_ts(arr, "scheduledTime")
    eta       = get_utc_ts(arr, "revisedTime")
    ata       = get_utc_ts(arr, "actualTime")

    # ── Movement-aware confirmed delay ───────────────────────────────────────
    # We compute the delay that is meaningful from MUC's perspective.
    #
    # DEPARTURE from MUC:  sched_utc = dep_sched_utc at MUC
    #   Best:    ATD vs dep_sched  (actual wheels-up vs scheduled)
    #   Revised: ETD vs dep_sched  (revised pushback vs scheduled)
    #
    # ARRIVAL to MUC:  sched_utc = arr_sched_utc at MUC
    #   Best:    ATA vs arr_sched  (actual touchdown vs scheduled)
    #   Revised: ETA vs arr_sched  (revised landing estimate vs scheduled)
    #   NOTE:    Do NOT use ATD vs dep_sched — that's the origin delay, not MUC delay.

    confirmed_delay_min = None

    if movement == "departure":
        # Departure from MUC — use departure times
        if atd and dep_sched:
            confirmed_delay_min = round((atd - dep_sched).total_seconds() / 60, 1)
        elif etd and dep_sched:
            confirmed_delay_min = round((etd - dep_sched).total_seconds() / 60, 1)

    else:
        # Arrival to MUC — use arrival times only
        if ata and arr_sched:
            confirmed_delay_min = round((ata - arr_sched).total_seconds() / 60, 1)
        elif eta and arr_sched:
            confirmed_delay_min = round((eta - arr_sched).total_seconds() / 60, 1)

    return {
        "number_raw":          number_raw,
        "flight_date":         flight_date,
        "op_status":           op_status,
        "etd_utc":             etd,
        "atd_utc":             atd,
        "eta_utc":             eta,
        "ata_utc":             ata,
        "confirmed_delay_min": confirmed_delay_min,
        "fetched_at":          datetime.now(timezone.utc),
    }


# ── Main function ────────────────────────────────────────────────────────────

def update_flight_status(delay_between_calls: float = 0.5) -> None:
    """
    1. Read distinct (number_raw, sched_date, movement) from flights_raw.
    2. For each, call Flight Status API.
    3. Upsert results into flight_status_live.

    movement is fetched so confirmed_delay_min is computed relative to
    the correct time reference (MUC arrival delay vs MUC departure delay).

    delay_between_calls: seconds to sleep between API calls to avoid rate limits.
    """
    engine = get_engine()

    # ── 1. Get distinct flights to check, including movement ───────────────
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT
                number_raw,
                DATE(COALESCE(dep_sched_utc, arr_sched_utc) AT TIME ZONE 'UTC') AS flight_date,
                movement
            FROM flights_raw
            WHERE number_raw IS NOT NULL
              AND COALESCE(dep_sched_utc, arr_sched_utc) IS NOT NULL
            ORDER BY flight_date DESC, number_raw
        """)).fetchall()

    if not rows:
        logger.warning("update_flight_status: no flights in flights_raw to check.")
        return

    logger.info("Fetching Flight Status for %d unique flights...", len(rows))

    # ── Connectivity pre-check ──────────────────────────────────────────────
    # If DNS resolution fails for the AeroDataBox host, skip the entire batch
    # rather than logging hundreds of identical NameResolutionError warnings.
    # This happens when the machine loses internet mid-cycle (e.g. sleep/wake).
    import socket
    try:
        socket.getaddrinfo(RAPIDAPI_HOST, 443, socket.AF_INET)
    except socket.gaierror:
        logger.warning(
            "[update_flight_status] Cannot resolve %s — network unavailable. "
            "Skipping Flight Status update this cycle.",
            RAPIDAPI_HOST,
        )
        return

    # ── 2. Fetch status for each ────────────────────────────────────────────
    records = []
    for number_raw, flight_date, movement in rows:
        # Strip space: "LH 638" → "LH638"
        flight_no_clean = str(number_raw).replace(" ", "")
        date_str        = str(flight_date)   # YYYY-MM-DD
        # Normalise movement: "dep"/"departure" → "departure", everything else → "arrival"
        mvt = str(movement or "arrival").lower().strip()
        if mvt in ("dep", "departure"):
            mvt = "departure"
        else:
            mvt = "arrival"

        flight_dict = fetch_flight_status(flight_no_clean, date_str)

        if flight_dict:
            record = parse_status_record(number_raw, date_str, flight_dict, movement=mvt)
            records.append(record)
            logger.debug(
                "  %s (%s) on %s → %s | delay=%s min",
                number_raw, mvt, date_str,
                record["op_status"],
                record["confirmed_delay_min"],
            )
        else:
            # No data from API — insert Unknown placeholder so we don't keep re-calling
            records.append({
                "number_raw":          number_raw,
                "flight_date":         date_str,
                "op_status":           "Unknown",
                "etd_utc":             None,
                "atd_utc":             None,
                "eta_utc":             None,
                "ata_utc":             None,
                "confirmed_delay_min": None,
                "fetched_at":          datetime.now(timezone.utc),
            })

        time.sleep(delay_between_calls)

    # ── 3. Upsert into flight_status_live ───────────────────────────────────
    if not records:
        logger.info("No status records to write.")
        return

    df = pd.DataFrame(records)

    # Ensure timestamp columns are properly typed
    for col in ["etd_utc", "atd_utc", "eta_utc", "ata_utc", "fetched_at"]:
        df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    # Write to a staging table, then upsert into flight_status_live
    # This is safe: staging is temp, real table is upserted only on (number_raw, flight_date)
    df.to_sql("_flight_status_staging", engine, if_exists="replace", index=False)

    with engine.connect() as conn:
        # Create flight_status_live if it doesn't exist
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

        # Upsert: if row exists, update it; if not, insert
        conn.execute(text("""
            INSERT INTO flight_status_live
                (number_raw, flight_date, op_status,
                 etd_utc, atd_utc, eta_utc, ata_utc,
                 confirmed_delay_min, fetched_at)
            SELECT
                number_raw, flight_date::DATE, op_status,
                etd_utc, atd_utc, eta_utc, ata_utc,
                confirmed_delay_min, fetched_at
            FROM _flight_status_staging
            ON CONFLICT (number_raw, flight_date)
            DO UPDATE SET
                op_status           = EXCLUDED.op_status,
                etd_utc             = EXCLUDED.etd_utc,
                atd_utc             = EXCLUDED.atd_utc,
                eta_utc             = EXCLUDED.eta_utc,
                ata_utc             = EXCLUDED.ata_utc,
                confirmed_delay_min = EXCLUDED.confirmed_delay_min,
                fetched_at          = EXCLUDED.fetched_at
        """))

        # Drop staging table
        conn.execute(text("DROP TABLE IF EXISTS _flight_status_staging"))
        conn.commit()

    landed   = df[df["op_status"] == "Landed"].shape[0]
    cancelled = df[df["op_status"] == "Cancelled"].shape[0]
    enroute  = df[df["op_status"] == "EnRoute"].shape[0]
    logger.info(
        "flight_status_live updated: %d records | Landed=%d, Cancelled=%d, EnRoute=%d",
        len(df), landed, cancelled, enroute
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    update_flight_status()