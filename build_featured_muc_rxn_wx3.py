import logging
import os
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import create_engine


logger = logging.getLogger(__name__)


def get_engine():
    pg_user = os.getenv("PG_USER", "postgres")
    pg_password = os.getenv("PG_PASSWORD", "delaypilot2026")
    pg_host = os.getenv("PG_HOST", "localhost")
    pg_port = os.getenv("PG_PORT", "5432")
    pg_db = os.getenv("PG_DB", "delaypilot_db")
    url = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    return create_engine(url)


def build_featured_muc_rxn_wx3(max_rows: Optional[int] = None) -> None:
    """
    DB equivalent of the `featured_muc_rxn_wx3.parquet` notebook.

    Starting from:
      - flights_raw          (Aerodatabox MUC FIDS with labels)
      - weather_hourly       (Open-Meteo hourly weather for 3 airports)

    This function:
      - normalizes movement + schedule time (sched_utc)
      - defines prediction reference time (ref_ts_utc = sched_utc - 2h, ref_hour_utc)
      - builds unified labels (y_delay_min, y_bin15, y_bin30)
      - builds reactionary features using aircraft_modeS (prev_*_safe)
      - joins Munich weather (wx_muc_*) on ref_hour_utc
      - joins other-airport weather (wx_other_*) on (other_airport_icao, ref_hour_utc)

    Result is written to Postgres table: featured_muc_rxn_wx3.
    """
    engine = get_engine()

    logger.info("Loading flights_raw and weather_hourly from Postgres")
    flights = pd.read_sql("SELECT * FROM flights_raw", engine)
    weather = pd.read_sql("SELECT * FROM weather_hourly", engine)

    if flights.empty:
        logger.error("flights_raw is empty; run ingest_flights first.")
        raise RuntimeError("flights_raw is empty; run ingest_flights first.")
    if weather.empty:
        logger.error("weather_hourly is empty; run ingest_weather first.")
        raise RuntimeError("weather_hourly is empty; run ingest_weather first.")

    if max_rows is not None and len(flights) > max_rows:
        flights = flights.sample(n=max_rows, random_state=42).reset_index(drop=True)
        logger.info("Sampled flights_raw down to %d rows for feature build.", len(flights))

    # --- Normalize movement and schedule time ---

    flights["movement"] = (
        flights["movement"]
        .astype(str)
        .str.lower()
        .str.strip()
        .replace({"dep": "departure", "arr": "arrival"})
    )

    flights["dep_sched_utc"] = pd.to_datetime(
        flights.get("dep_sched_utc"), utc=True, errors="coerce"
    )
    flights["arr_sched_utc"] = pd.to_datetime(
        flights.get("arr_sched_utc"), utc=True, errors="coerce"
    )

    flights["sched_utc"] = np.where(
        flights["movement"].eq("departure"),
        flights["dep_sched_utc"],
        flights["arr_sched_utc"],
    )
    flights["sched_utc"] = pd.to_datetime(flights["sched_utc"], utc=True, errors="coerce")

    # Prediction reference time: 2 hours before schedule
    flights["ref_ts_utc"] = flights["sched_utc"] - pd.Timedelta(hours=2)
    flights["ref_hour_utc"] = flights["ref_ts_utc"].dt.floor("h")

    # --- Unified labels ---

    flights["dep_delay_min"] = pd.to_numeric(flights.get("dep_delay_min"), errors="coerce")
    flights["arr_delay_min"] = pd.to_numeric(flights.get("arr_delay_min"), errors="coerce")

    flights["y_delay_min"] = np.where(
        flights["movement"].eq("departure"),
        flights["dep_delay_min"],
        flights["arr_delay_min"],
    )
    flights["y_delay_min"] = pd.to_numeric(flights["y_delay_min"], errors="coerce").clip(
        lower=-300, upper=720
    )

    flights["y_bin15"] = (flights["y_delay_min"] >= 15).astype("int8")
    flights["y_bin30"] = (flights["y_delay_min"] >= 30).astype("int8")

    # best_utc = best available actual event time (departure vs arrival)
    flights["dep_best_utc"] = pd.to_datetime(
        flights.get("dep_best_utc"), utc=True, errors="coerce"
    )
    flights["arr_best_utc"] = pd.to_datetime(
        flights.get("arr_best_utc"), utc=True, errors="coerce"
    )
    flights["best_utc"] = np.where(
        flights["movement"].eq("departure"),
        flights["dep_best_utc"],
        flights["arr_best_utc"],
    )
    flights["best_utc"] = pd.to_datetime(flights["best_utc"], utc=True, errors="coerce")

    # --- Reactionary features using aircraft_modeS ---

    flights["aircraft_modeS"] = (
        flights.get("aircraft_modeS", "")
        .astype(str)
        .str.upper()
        .str.strip()
    )

    flights = flights.sort_values(["aircraft_modeS", "sched_utc"]).reset_index(drop=True)
    g = flights.groupby("aircraft_modeS", sort=False)

    flights["prev_best_utc"] = g["best_utc"].shift(1)
    flights["prev_delay_min"] = g["y_delay_min"].shift(1)

    # Only use previous delay if previous flight finished before ref_ts_utc
    knowable = flights["prev_best_utc"].notna() & (
        flights["prev_best_utc"] <= flights["ref_ts_utc"]
    )

    flights["prev_delay_min_safe"] = np.where(knowable, flights["prev_delay_min"], np.nan)
    flights["prev_late15_safe"] = np.where(
        knowable, (flights["prev_delay_min"] >= 15).astype("int8"), np.nan
    )
    flights["prev_late30_safe"] = np.where(
        knowable, (flights["prev_delay_min"] >= 30).astype("int8"), np.nan
    )

    # --- Prepare weather for joins ---

    weather["hour_utc"] = pd.to_datetime(weather["hour_utc"], utc=True, errors="coerce")
    weather["airport_icao"] = (
        weather["airport_icao"].astype(str).str.upper().str.strip()
    )

    # Munich (EDDM) weather -> wx_muc_*
    wx_muc = (
        weather[weather["airport_icao"] == "EDDM"]
        .drop(columns=["airport_icao"], errors="ignore")
        .copy()
    )
    wx_muc = wx_muc.add_prefix("wx_muc_").rename(
        columns={"wx_muc_hour_utc": "ref_hour_utc"}
    )

    # Other airports (EDDF, EGLL) -> wx_other_*
    wx_other = weather[weather["airport_icao"].isin(["EDDF", "EGLL"])].copy()
    wx_other = wx_other.add_prefix("wx_other_").rename(
        columns={
            "wx_other_airport_icao": "other_airport_icao",
            "wx_other_hour_utc": "ref_hour_utc",
        }
    )

    # Normalize other_airport_icao in flights for join
    flights["other_airport_icao"] = (
        flights.get("other_airport_icao", "")
        .astype(str)
        .str.upper()
        .str.strip()
    )

    # --- Join Munich weather ---

    before = len(flights)
    flights = flights.merge(wx_muc, on="ref_hour_utc", how="left")
    muc_missing = (
        flights.filter(like="wx_muc_").isna().mean().mean() * 100
        if any(c.startswith("wx_muc_") for c in flights.columns)
        else 0.0
    )
    logger.info(
        "Joined Munich weather: rows=%d (expected=%d), Munich wx missing mean=%.3f%%",
        len(flights),
        before,
        muc_missing,
    )

    # --- Join other-airport weather (EDDF/EGLL) ---

    before = len(flights)
    flights = flights.merge(
        wx_other,
        on=["other_airport_icao", "ref_hour_utc"],
        how="left",
    )
    other_any = (
        flights.filter(like="wx_other_").notna().any(axis=1).mean() * 100
        if any(c.startswith("wx_other_") for c in flights.columns)
        else 0.0
    )
    logger.info(
        "Joined other-airport weather: rows=%d (expected=%d), other-airport wx present=%.2f%%",
        len(flights),
        before,
        other_any,
    )

    # --- Persist to Postgres ---

    flights.to_sql(
        "featured_muc_rxn_wx3",
        engine,
        if_exists="replace",
        index=False,
    )
    logger.info(
        "Wrote featured_muc_rxn_wx3 with %d rows and %d columns.",
        len(flights),
        len(flights.columns),
    )


if __name__ == "__main__":
    build_featured_muc_rxn_wx3()

