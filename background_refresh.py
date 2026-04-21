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
      6. Snapshot — Resolve tier-priority delay + upsert analytics table

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

    # ── Step 5: Flight Status API ─────────────────────────────────────────────
    # Runs AFTER feature rebuild so it sees the latest flights_raw rows.
    # This guarantees Flight Status and FIDS data share the same cycle timestamp.
    try:
        from update_flight_status import update_flight_status
        update_flight_status()
        _state["status_last_ran"] = datetime.now(timezone.utc)
        logger.info("[background] Step 5/6 ✓ Flight Status updated.")
    except Exception as e:
        logger.warning("[background] Step 5/6 ✗ Flight Status failed: %s", e)
        _state["last_error"] = str(e)

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
    except Exception as e:
        logger.warning("[background] Step 6/6 ✗ Delay analytics snapshot failed: %s", e)
        _state["last_error"] = str(e)

    _state["last_ran"] = cycle_start
    _state["running"]  = False
    elapsed = (datetime.now(timezone.utc) - cycle_start).total_seconds()
    logger.info("[background] ══ Full refresh cycle complete (%.1fs) ══", elapsed)


# ── Scheduler loop ────────────────────────────────────────────────────────────

def _scheduler_loop():
    """
    Daemon loop — waits REFRESH_INTERVAL_SEC before the first refresh,
    then repeats every REFRESH_INTERVAL_SEC thereafter.
 
    startup_delaypilot.py already runs run_pipeline.py synchronously before
    the API starts, so all tables are fresh at boot. The first background
    cycle is intentionally deferred to avoid a redundant double-refresh and
    an unnecessary second Aerodatabox API call at startup.
    """
    logger.info(
        "[background] Scheduler started — first refresh in %d min, then every %d min.",
        REFRESH_INTERVAL_SEC // 60,
        REFRESH_INTERVAL_SEC // 60,
    )
 
    last_ran = time.monotonic()   # ← defers first run by REFRESH_INTERVAL_SEC
 
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