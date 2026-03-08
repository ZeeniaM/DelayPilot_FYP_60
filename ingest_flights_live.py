import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine

# --- Hard-coded RapidAPI credentials for Aerodatabox ---

RAPIDAPI_KEY = "e21b0e0c6dmsh590b13201fa4425p1c6ff5jsnf880e0aaeb16"
RAPIDAPI_HOST = "aerodatabox.p.rapidapi.com"


logger = logging.getLogger(__name__)


def get_engine():
    pg_user = os.getenv("PG_USER", "postgres")
    pg_password = os.getenv("PG_PASSWORD", "delaypilot2026")
    pg_host = os.getenv("PG_HOST", "localhost")
    pg_port = os.getenv("PG_PORT", "5432")
    pg_db = os.getenv("PG_DB", "delaypilot_db")
    url = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    return create_engine(url)


def parse_utc(s: Any) -> pd.Timestamp:
    if s is None:
        return pd.NaT
    s = str(s).strip()
    if " " in s and s.endswith("Z") and "T" not in s:
        s = s.replace(" ", "T")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return pd.to_datetime(s, errors="coerce", utc=True)


def get_utc(obj: Dict[str, Any], field: str) -> pd.Timestamp:
    """
    obj: departure/arrival dict
    field: 'scheduledTime', 'revisedTime', 'predictedTime', 'runwayTime'
    """
    if not obj:
        return pd.NaT
    return parse_utc(((obj.get(field, {}) or {}).get("utc")))


def fetch_fids_muc_interval(from_local: str, to_local: str) -> Dict[str, Any]:
    """
    Call Aerodatabox FIDS API for MUC for a given local interval.
    Uses the hard-coded RapidAPI key and host.
    """
    url = f"https://{RAPIDAPI_HOST}/flights/airports/iata/MUC/{from_local}/{to_local}"
    params = {
        "withLeg": "true",
        "direction": "Both",
        "withCancelled": "true",
        "withCodeshared": "true",
        "withCargo": "true",
        "withPrivate": "true",
        "withLocation": "false",
    }
    headers = {
        "Accept": "application/json",
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": RAPIDAPI_HOST,
    }
    logger.info(
        "Requesting live FIDS from Aerodatabox: %s to %s", from_local, to_local
    )
    resp = requests.get(url, headers=headers, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def normalize_fids(data: Dict[str, Any], airport_iata: str = "MUC") -> pd.DataFrame:
    """
    Normalize a single FIDS response into a DataFrame corresponding to
    the muc_fids_365d_full schema (subset).
    """
    rows: List[Dict[str, Any]] = []
    for movement_type, items in [
        ("departure", data.get("departures", [])),
        ("arrival", data.get("arrivals", [])),
    ]:
        for it in items:
            dep = it.get("departure", {}) or {}
            arr = it.get("arrival", {}) or {}
            airline = it.get("airline", {}) or {}
            aircraft = it.get("aircraft", {}) or {}

            # For departures, other airport is arrival.airport; for arrivals, other is departure.airport
            other_airport = (
                (arr.get("airport", {}) or {})
                if movement_type == "departure"
                else (dep.get("airport", {}) or {})
            )

            rows.append(
                {
                    "movement": movement_type,
                    "airport_iata": airport_iata,
                    "number_raw": it.get("number"),
                    "call_sign": it.get("callSign"),
                    "status": it.get("status"),
                    "codeshare_status": it.get("codeshareStatus"),
                    "is_cargo": it.get("isCargo"),
                    "airline_name": airline.get("name"),
                    "airline_iata": airline.get("iata"),
                    "airline_icao": airline.get("icao"),
                    "aircraft_model": aircraft.get("model"),
                    "aircraft_reg": aircraft.get("reg"),
                    "aircraft_modeS": aircraft.get("modeS"),
                    "other_airport_iata": other_airport.get("iata"),
                    "other_airport_icao": other_airport.get("icao"),
                    "other_airport_name": other_airport.get("name"),
                    "other_airport_tz": other_airport.get("timeZone"),
                    "dep_sched_utc": get_utc(dep, "scheduledTime"),
                    "dep_rev_utc": get_utc(dep, "revisedTime"),
                    "dep_pred_utc": get_utc(dep, "predictedTime"),
                    "dep_runway_utc": get_utc(dep, "runwayTime"),
                    "arr_sched_utc": get_utc(arr, "scheduledTime"),
                    "arr_rev_utc": get_utc(arr, "revisedTime"),
                    "arr_pred_utc": get_utc(arr, "predictedTime"),
                    "arr_runway_utc": get_utc(arr, "runwayTime"),
                }
            )
    return pd.DataFrame(rows)


def ingest_live_muc_window(hours_before: int = 1, hours_after: int = 6) -> None:
    """
    Fetch a window of live FIDS data around 'now' from Aerodatabox
    and load it into flights_raw, computing schedule-based delays.
    """
    from datetime import timezone
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=hours_before)
    end = now + timedelta(hours=hours_after)

    from_local = start.strftime("%Y-%m-%dT%H:%M")
    to_local = end.strftime("%Y-%m-%dT%H:%M")

    logger.info("Fetching live MUC FIDS from %s to %s", from_local, to_local)
    data = fetch_fids_muc_interval(from_local, to_local)
    df = normalize_fids(data, "MUC")

    if df.empty:
        logger.warning("No live flights returned from Aerodatabox.")
        return

    # Compute best times and delays as in the notebook
    time_cols = [
        "dep_sched_utc",
        "dep_rev_utc",
        "dep_pred_utc",
        "dep_runway_utc",
        "arr_sched_utc",
        "arr_rev_utc",
        "arr_pred_utc",
        "arr_runway_utc",
    ]
    for c in time_cols:
        df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")

    df["dep_best_utc"] = df["dep_runway_utc"].fillna(df["dep_rev_utc"]).fillna(df["dep_pred_utc"])
    df["arr_best_utc"] = df["arr_runway_utc"].fillna(df["arr_rev_utc"]).fillna(df["arr_pred_utc"])

    df["dep_delay_min"] = (df["dep_best_utc"] - df["dep_sched_utc"]).dt.total_seconds() / 60
    df["arr_delay_min"] = (df["arr_best_utc"] - df["arr_sched_utc"]).dt.total_seconds() / 60

    def bucket(x: float) -> float:
        if pd.isna(x):
            return np.nan
        if x < 5:
            return 0
        if x < 15:
            return 1
        if x < 30:
            return 2
        return 3

    df["arr_delay_bucket_5_15_30"] = df["arr_delay_min"].apply(bucket)

    engine = get_engine()

    # For live mode we overwrite flights_raw with this window.
    df.to_sql(
        "flights_raw",
        engine,
        if_exists="replace",
        index=False,
    )

    logger.info("Updated flights_raw with live window: %d rows.", len(df))


if __name__ == "__main__":
    ingest_live_muc_window()