import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ingest_flights_live import fetch_fids_muc_interval, normalize_fids
import pandas as pd


def main():
    # Fetch live FIDS
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=1)
    end = now + timedelta(hours=6)

    from_local = start.strftime("%Y-%m-%dT%H:%M")
    to_local = end.strftime("%Y-%m-%dT%H:%M")

    print(f"Fetching live FIDS from {from_local} to {to_local}\n")
    try:
        data = fetch_fids_muc_interval(from_local, to_local)
    except Exception as e:
        print(f"ERROR: {e}")
        return

    # Normalize to DataFrame (this is what gets stored)
    df = normalize_fids(data, "MUC")

    if df.empty:
        print("No flights returned.")
        return

    print("=== COLUMNS CREATED BY normalize_fids() ===")
    print("Total columns:", len(df.columns))
    print("\nColumn names:")
    for i, col in enumerate(df.columns, 1):
        print(f"  {i:2d}. {col}")

    # Check for time-related columns
    print("\n=== TIME-RELATED COLUMNS ===")
    time_cols = [c for c in df.columns if 'utc' in c.lower() or 'time' in c.lower()]
    print("Time columns found:")
    for col in time_cols:
        print(f"  - {col}")

    # Show sample row with selected columns
    print("\n=== SAMPLE ROW (time-related fields only) ===")
    sample_row = df.iloc[0]
    for col in time_cols:
        print(f"{col:20s}: {sample_row[col]}")

    # Show all columns for a sample row
    print("\n=== SAMPLE ROW (ALL COLUMNS) ===")
    print(df.iloc[0].to_string())

    # Count non-null values for time columns
    print("\n=== TIME COLUMNS DATA AVAILABILITY ===")
    for col in time_cols:
        non_null = df[col].notna().sum()
        total = len(df)
        pct = (non_null / total * 100) if total > 0 else 0
        print(f"{col:20s}: {non_null:3d}/{total:3d} ({pct:5.1f}%)")

    # Now simulate the ingest_live_muc_window() transformation
    print("\n=== COLUMNS AFTER ingest_live_muc_window() PROCESSING ===")
    df_processed = df.copy()
    
    # Convert times
    time_cols_to_convert = [
        "dep_sched_utc", "dep_rev_utc", "dep_pred_utc", "dep_runway_utc",
        "arr_sched_utc", "arr_rev_utc", "arr_pred_utc", "arr_runway_utc",
    ]
    for c in time_cols_to_convert:
        df_processed[c] = pd.to_datetime(df_processed[c], utc=True, errors="coerce")

    # Add computed columns
    df_processed["dep_best_utc"] = df_processed["dep_runway_utc"].fillna(df_processed["dep_rev_utc"]).fillna(df_processed["dep_pred_utc"])
    df_processed["arr_best_utc"] = df_processed["arr_runway_utc"].fillna(df_processed["arr_rev_utc"]).fillna(df_processed["arr_pred_utc"])
    df_processed["dep_delay_min"] = (df_processed["dep_best_utc"] - df_processed["dep_sched_utc"]).dt.total_seconds() / 60
    df_processed["arr_delay_min"] = (df_processed["arr_best_utc"] - df_processed["arr_sched_utc"]).dt.total_seconds() / 60

    print(f"Total columns after processing: {len(df_processed.columns)}")
    print("\nAll columns that will be stored in database:")
    for i, col in enumerate(df_processed.columns, 1):
        print(f"  {i:2d}. {col}")

    print("\n=== KEY COLUMNS STORED ===")
    key_cols = ['number_raw', 'movement', 'dep_sched_utc', 'dep_rev_utc', 'dep_runway_utc', 'dep_best_utc', 'dep_delay_min',
                'arr_sched_utc', 'arr_rev_utc', 'arr_runway_utc', 'arr_best_utc', 'arr_delay_min']
    print(df_processed[key_cols].head(5).to_string())


if __name__ == '__main__':
    main()
