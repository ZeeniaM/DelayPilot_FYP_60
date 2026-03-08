"""
background_refresh.py
────────────────────────────────────────────────────────────────────────────
Runs a single combined refresh cycle on one interval:

  STEP 1 — Weather (Open-Meteo)
  STEP 2 — FIDS   (Aerodatabox live window) → flights_raw
  STEP 3 — Feature build pass 1            → featured_muc_rxn_wx3
  STEP 4 — Feature build pass 2            → featured_muc_rxn_wx3_fe
  STEP 5 — Flight Status API               → flight_status_live

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

logger = logging.getLogger(__name__)

# ── Single shared interval ────────────────────────────────────────────────────
# Both FIDS and Flight Status run together every REFRESH_INTERVAL_MINUTES.
# Default 30 min: short enough to catch delays early, long enough to stay
# within API rate limits for both Aerodatabox FIDS and Flight Status endpoints.
REFRESH_INTERVAL_SEC = int(os.getenv("REFRESH_INTERVAL_MINUTES", "30")) * 60

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


# ── Combined refresh cycle ────────────────────────────────────────────────────

def _run_full_refresh():
    """
    Execute one complete data refresh cycle in order:

      1. Weather  — Open-Meteo hourly data
      2. FIDS     — Aerodatabox live flight window → flights_raw
      3. Features — build featured_muc_rxn_wx3
      4. Features — build featured_muc_rxn_wx3_fe  (what /flights reads)
      5. Status   — Aerodatabox Flight Status API  → flight_status_live

    Steps 1-4 and Step 5 run in the same cycle so FIDS data and
    Flight Status data are always from the same refresh timestamp.
    Step 5 runs AFTER feature rebuild so it reads the latest flights_raw.
    """
    if _state["running"]:
        logger.info("[background] Refresh already in progress — skipping this tick.")
        return

    _state["running"]    = True
    _state["last_error"] = None
    cycle_start = datetime.now(timezone.utc)
    logger.info("[background] ══ Full refresh cycle starting ══")

    # ── Step 1: Weather ───────────────────────────────────────────────────────
    try:
        from ingest_weather_live import update_weather_live
        update_weather_live()
        logger.info("[background] Step 1/5 ✓ Weather updated.")
    except Exception as e:
        logger.warning("[background] Step 1/5 ✗ Weather update failed: %s", e)
        _state["last_error"] = str(e)

    # ── Step 2: FIDS ─────────────────────────────────────────────────────────
    try:
        from ingest_flights_live import ingest_live_muc_window
        ingest_live_muc_window()
        _state["fids_last_ran"] = datetime.now(timezone.utc)
        logger.info("[background] Step 2/5 ✓ FIDS ingested.")
    except Exception as e:
        logger.warning("[background] Step 2/5 ✗ FIDS ingest failed: %s", e)
        _state["last_error"] = str(e)

    # ── Step 3: Feature build pass 1 ─────────────────────────────────────────
    try:
        from build_featured_muc_rxn_wx3 import build_featured_muc_rxn_wx3
        build_featured_muc_rxn_wx3()
        logger.info("[background] Step 3/5 ✓ featured_muc_rxn_wx3 rebuilt.")
    except Exception as e:
        logger.warning("[background] Step 3/5 ✗ Feature build pass 1 failed: %s", e)
        _state["last_error"] = str(e)

    # ── Step 4: Feature build pass 2 ─────────────────────────────────────────
    try:
        from build_featured_muc_rxn_wx3_fe import build_featured_muc_rxn_wx3_fe
        build_featured_muc_rxn_wx3_fe()
        logger.info("[background] Step 4/5 ✓ featured_muc_rxn_wx3_fe rebuilt.")
    except Exception as e:
        logger.warning("[background] Step 4/5 ✗ Feature build pass 2 failed: %s", e)
        _state["last_error"] = str(e)

    # ── Step 5: Batch ML predictions ─────────────────────────────────────────
    # Runs after feature rebuild — flight_predictions is always in sync with
    # the current featured_muc_rxn_wx3_fe window.
    try:
        from run_batch_predictions import run_batch_predictions
        n = run_batch_predictions()
        logger.info("[background] Step 5/6 ✓ Batch predictions written (%d rows).", n)
    except Exception as e:
        logger.warning("[background] Step 5/6 ✗ Batch predictions failed: %s", e)
        _state["last_error"] = str(e)

    # ── Step 6: Flight Status API ─────────────────────────────────────────────
    # Runs LAST so confirmed_delay_min always shares the same refresh window
    # as flight_predictions and featured_muc_rxn_wx3_fe.
    #─────────────────────────────────────────────
    # Runs AFTER feature rebuild so it sees the latest flights_raw rows.
    # This guarantees Flight Status and FIDS data share the same cycle timestamp.
    try:
        from update_flight_status import update_flight_status
        update_flight_status()
        _state["status_last_ran"] = datetime.now(timezone.utc)
        logger.info("[background] Step 6/6 ✓ Flight Status updated.")
    except Exception as e:
        logger.warning("[background] Step 6/6 ✗ Flight Status failed: %s", e)
        _state["last_error"] = str(e)

    _state["last_ran"] = cycle_start
    _state["running"]  = False
    elapsed = (datetime.now(timezone.utc) - cycle_start).total_seconds()
    logger.info("[background] ══ Full refresh cycle complete (%.1fs) ══", elapsed)


# ── Scheduler loop ────────────────────────────────────────────────────────────

def _scheduler_loop():
    """
    Daemon loop — fires _run_full_refresh() immediately on startup,
    then every REFRESH_INTERVAL_SEC.
    """
    logger.info(
        "[background] Scheduler started — full refresh every %d min.",
        REFRESH_INTERVAL_SEC // 60,
    )

    last_ran = 0.0   # ensures first iteration fires immediately

    while True:
        now = time.monotonic()
        if now - last_ran >= REFRESH_INTERVAL_SEC:
            t = threading.Thread(
                target=_run_full_refresh,
                name="delaypilot-refresh",
                daemon=True,
            )
            t.start()
            last_ran = now

        time.sleep(60)   # check every 60 s


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