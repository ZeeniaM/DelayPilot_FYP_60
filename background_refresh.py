"""
background_refresh.py
────────────────────────────────────────────────────────────────────────────
Runs a single combined refresh cycle on one interval:

  STEP 1 — Weather (Open-Meteo)
  STEP 2 — FIDS   (Aerodatabox live window) → flights_raw
  STEP 3 — Feature build pass 1            → featured_muc_rxn_wx3
  STEP 4 — Feature build pass 2            → featured_muc_rxn_wx3_fe
  STEP 5 — Flight Status API               → flight_status_live
  STEP 6 — Delay Analytics Snapshot        → flight_delay_snapshots (upsert)

Running FIDS and Flight Status in the SAME cycle guarantees that
confirmed_delay_min and dep/arr_best_utc always refer to the same
point in time, eliminating the "times match but delay shows" contradiction
caused by the two APIs being on different refresh schedules.

Interval (configurable):
  REFRESH_INTERVAL_MINUTES   default: 30

Usage — called from api_main.py on_startup:
    from background_refresh import start_background_refresh
    @app.on_event("startup")
    def startup():
        start_background_refresh()
────────────────────────────────────────────────────────────────────────────
"""

import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

# ── Single shared interval ────────────────────────────────────────────────────
# Both FIDS and Flight Status run together every REFRESH_INTERVAL_MINUTES.
# Default 30 min: short enough to catch delays early, long enough to stay
# within API rate limits for both Aerodatabox FIDS and Flight Status endpoints.
_interval_cache_checked_at = 0.0
_interval_cache_value_sec: Optional[int] = None
_settings_engine = None
_pipeline_log = []
_log_lock = None

# ── Shared state ──────────────────────────────────────────────────────────────
_state = {
    "last_ran":    None,   # datetime UTC of last successful full cycle
    "running":     False,
    "last_error":  None,
    # Step-level last-ran (useful for /health endpoint)
    "fids_last_ran":   None,
    "status_last_ran": None,
}


def get_refresh_state() -> dict:
    """Return a snapshot of refresh state — safe to call from any thread."""
    return dict(_state)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_pipeline_log(entry: dict) -> None:
    global _log_lock
    if _log_lock is None:
        _log_lock = threading.Lock()

    with _log_lock:
        _pipeline_log.append(dict(entry))
        while len(_pipeline_log) > 50:
            _pipeline_log.pop(0)


def get_pipeline_log() -> list:
    global _log_lock
    if _log_lock is None:
        _log_lock = threading.Lock()

    with _log_lock:
        logs = [dict(entry) for entry in _pipeline_log]

    return sorted(
        logs,
        key=lambda entry: entry.get("timestamp", ""),
        reverse=True,
    )


def _log_step_success(step_number: int, step_name: str) -> None:
    append_pipeline_log({
        "event": f"Step {step_number} completed: {step_name}",
        "status": "success",
        "timestamp": _now_iso(),
    })


def _log_step_failure(step_number: int, step_name: str, exc: Exception) -> None:
    append_pipeline_log({
        "event": f"Step {step_number} failed: {step_name}",
        "status": "error",
        "timestamp": _now_iso(),
        "error": str(exc)[:200],
    })


def _get_settings_engine():
    global _settings_engine
    if _settings_engine is None:
        pg_user = os.getenv("PG_USER", "postgres")
        pg_password = os.getenv("PG_PASSWORD", "delaypilot2026")
        pg_host = os.getenv("PG_HOST", "localhost")
        pg_port = os.getenv("PG_PORT", "5432")
        pg_db = os.getenv("PG_DB", "delaypilot_db")
        url = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
        _settings_engine = create_engine(url)
    return _settings_engine


def _valid_refresh_minutes(value: Any) -> Optional[int]:
    try:
        minutes = int(str(value).strip())
    except (TypeError, ValueError):
        return None

    if 5 <= minutes <= 120:
        return minutes
    return None


def _fallback_refresh_interval_sec() -> int:
    minutes = _valid_refresh_minutes(os.getenv("REFRESH_INTERVAL_MINUTES", "30"))
    return (minutes or 30) * 60


def _read_refresh_interval_minutes_from_db() -> Optional[int]:
    queries = [
        ("SELECT value FROM system_settings WHERE key = :key LIMIT 1", "value"),
        ("SELECT setting_value FROM system_settings WHERE setting_key = :key LIMIT 1", "setting_value"),
        ("SELECT value FROM system_settings WHERE name = :key LIMIT 1", "value"),
        ("SELECT refresh_interval_minutes FROM system_settings LIMIT 1", "refresh_interval_minutes"),
    ]

    engine = _get_settings_engine()
    for sql, column_name in queries:
        try:
            with engine.connect() as conn:
                row = conn.execute(text(sql), {"key": "refresh_interval_minutes"}).mappings().first()
        except Exception as exc:
            logger.debug("[background] Refresh interval settings query failed: %s", exc)
            continue

        if row:
            minutes = _valid_refresh_minutes(row.get(column_name))
            if minutes is not None:
                return minutes

    return None


def get_refresh_interval_sec() -> int:
    global _interval_cache_checked_at, _interval_cache_value_sec

    now = time.monotonic()
    if (
        _interval_cache_value_sec is not None
        and now - _interval_cache_checked_at < 60
    ):
        return _interval_cache_value_sec

    minutes = _read_refresh_interval_minutes_from_db()
    _interval_cache_value_sec = minutes * 60 if minutes is not None else _fallback_refresh_interval_sec()
    _interval_cache_checked_at = now
    return _interval_cache_value_sec


# ── Combined refresh cycle ────────────────────────────────────────────────────

def _run_full_refresh():
    """
    Execute one complete data refresh cycle in order:

      1. Weather  — Open-Meteo hourly data
      2. FIDS     — Aerodatabox live flight window → flights_raw
      3. Features — build featured_muc_rxn_wx3
      4. Features — build featured_muc_rxn_wx3_fe  (what /flights reads)
      5. Status   — Aerodatabox Flight Status API  → flight_status_live
      6. Snapshot — Resolve tier-priority delay + upsert analytics table

    Steps 1-4 and Step 5 run in the same cycle so FIDS data and
    Flight Status data are always from the same refresh timestamp.
    Step 5 runs AFTER feature rebuild so it reads the latest flights_raw.
    """
    if _state["running"]:
        logger.info("[background] Refresh already in progress — skipping this tick.")
        append_pipeline_log({
            "event": "Cycle skipped (already running)",
            "status": "skipped",
            "timestamp": _now_iso(),
        })
        return

    _state["running"]    = True
    _state["last_error"] = None
    cycle_start = datetime.now(timezone.utc)
    append_pipeline_log({
        "event": "Refresh cycle started",
        "status": "running",
        "timestamp": cycle_start.isoformat(),
    })
    logger.info("[background] ══ Full refresh cycle starting ══")

    # ── Step 1: Weather ───────────────────────────────────────────────────────
    try:
        from ingest_weather_live import update_weather_live
        update_weather_live()
        logger.info("[background] Step 1/5 ✓ Weather updated.")
        _log_step_success(1, "Weather")
    except Exception as e:
        logger.warning("[background] Step 1/5 ✗ Weather update failed: %s", e)
        _state["last_error"] = str(e)
        _log_step_failure(1, "Weather", e)

    # ── Step 2: FIDS ─────────────────────────────────────────────────────────
    try:
        from ingest_flights_live import ingest_live_muc_window
        ingest_live_muc_window()
        _state["fids_last_ran"] = datetime.now(timezone.utc)
        logger.info("[background] Step 2/5 ✓ FIDS ingested.")
        _log_step_success(2, "FIDS")
    except Exception as e:
        logger.warning("[background] Step 2/5 ✗ FIDS ingest failed: %s", e)
        _state["last_error"] = str(e)
        _log_step_failure(2, "FIDS", e)

    # ── Step 3: Feature build pass 1 ─────────────────────────────────────────
    try:
        from build_featured_muc_rxn_wx3 import build_featured_muc_rxn_wx3
        build_featured_muc_rxn_wx3()
        logger.info("[background] Step 3/5 ✓ featured_muc_rxn_wx3 rebuilt.")
        _log_step_success(3, "Feature build 1")
    except Exception as e:
        logger.warning("[background] Step 3/5 ✗ Feature build pass 1 failed: %s", e)
        _state["last_error"] = str(e)
        _log_step_failure(3, "Feature build 1", e)

    # ── Step 4: Feature build pass 2 ─────────────────────────────────────────
    try:
        from build_featured_muc_rxn_wx3_fe import build_featured_muc_rxn_wx3_fe
        build_featured_muc_rxn_wx3_fe()
        logger.info("[background] Step 4/5 ✓ featured_muc_rxn_wx3_fe rebuilt.")
        _log_step_success(4, "Feature build 2")
    except Exception as e:
        logger.warning("[background] Step 4/5 ✗ Feature build pass 2 failed: %s", e)
        _state["last_error"] = str(e)
        _log_step_failure(4, "Feature build 2", e)

    # ── Step 5: Flight Status API ─────────────────────────────────────────────
    # Runs AFTER feature rebuild so it sees the latest flights_raw rows.
    # This guarantees Flight Status and FIDS data share the same cycle timestamp.
    try:
        from update_flight_status import update_flight_status
        update_flight_status()
        _state["status_last_ran"] = datetime.now(timezone.utc)
        logger.info("[background] Step 5/6 ✓ Flight Status updated.")
        _log_step_success(5, "Flight Status")
    except Exception as e:
        logger.warning("[background] Step 5/6 ✗ Flight Status failed: %s", e)
        _state["last_error"] = str(e)
        _log_step_failure(5, "Flight Status", e)

    # ── Step 6: Delay Analytics Snapshot ─────────────────────────────────────
    # Reads the fully-resolved flight state (same JOIN as /flights endpoint:
    # featured_muc_rxn_wx3_fe + flight_status_live + flight_predictions) and
    # applies the same 3-tier priority logic as predictionService.js before
    # upserting into flight_delay_snapshots.
    # UPSERT (not replace): history accumulates across refresh cycles so the
    # dashboard trend + cause charts improve as more data comes in.
    # Non-fatal: failure here does not affect flights table or ML predictions.
    try:
        from snapshot_delay_analytics import snapshot_delay_analytics
        snapshot_delay_analytics()
        logger.info("[background] Step 6/6 ✓ Delay analytics snapshot written.")
        _log_step_success(6, "Delay snapshot")
    except Exception as e:
        logger.warning("[background] Step 6/6 ✗ Delay analytics snapshot failed: %s", e)
        _state["last_error"] = str(e)
        _log_step_failure(6, "Delay snapshot", e)

    elapsed = (datetime.now(timezone.utc) - cycle_start).total_seconds()
    if _state["last_error"] is None:
        append_pipeline_log({
            "event": "Refresh cycle completed",
            "status": "success",
            "timestamp": _now_iso(),
            "duration_seconds": round(elapsed, 1),
        })
    _state["last_ran"] = datetime.now(timezone.utc)
    _state["running"]  = False
    logger.info("[background] ══ Full refresh cycle complete (%.1fs) ══", elapsed)


# ── Scheduler loop ────────────────────────────────────────────────────────────

def _scheduler_loop():
    """
    Daemon loop — waits refresh_interval_sec before the first refresh,
    then repeats every refresh_interval_sec thereafter.
 
    startup_delaypilot.py already runs run_pipeline.py synchronously before
    the API starts, so all tables are fresh at boot. The first background
    cycle is intentionally deferred to avoid a redundant double-refresh and
    an unnecessary second Aerodatabox API call at startup.
    """
    initial_interval_sec = get_refresh_interval_sec()
    logger.info(
        "[background] Scheduler started — first refresh in %d min, then every %d min.",
        initial_interval_sec // 60,
        initial_interval_sec // 60,
    )
 
    last_ran = time.monotonic()   # ← defers first run by refresh_interval_sec
 
    while True:
        now = time.monotonic()
        refresh_interval_sec = get_refresh_interval_sec()
        if now - last_ran >= refresh_interval_sec:
            t = threading.Thread(
                target=_run_full_refresh,
                name="delaypilot-refresh",
                daemon=True,
            )
            t.start()
            last_ran = now
 
        time.sleep(60)   # check every 60 s — lightweight, no busy-wait
 

# ── Public entry point ────────────────────────────────────────────────────────

_scheduler_thread: threading.Thread = None


def start_background_refresh():
    """
    Start the background scheduler thread.
    Safe to call multiple times — only one thread will ever run.
    """
    global _scheduler_thread
    if _scheduler_thread is not None and _scheduler_thread.is_alive():
        logger.info("[background] Scheduler already running — skipping start.")
        return

    _scheduler_thread = threading.Thread(
        target=_scheduler_loop,
        name="delaypilot-scheduler",
        daemon=True,
    )
    _scheduler_thread.start()
    logger.info("[background] Scheduler thread launched.")



