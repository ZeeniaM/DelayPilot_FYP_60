import os
import sys
import json
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ingest_flights_live import fetch_fids_muc_interval


def main():
    # Fetch a small window around now
    now = datetime.utcnow()
    start = now - timedelta(hours=1)
    end = now + timedelta(hours=6)

    from_local = start.strftime("%Y-%m-%dT%H:%M")
    to_local = end.strftime("%Y-%m-%dT%H:%M")

    print(f"Fetching live FIDS from {from_local} to {to_local}\n")
    try:
        data = fetch_fids_muc_interval(from_local, to_local)
    except Exception as e:
        print(f"ERROR fetching FIDS: {e}")
        return

    # Print top-level keys
    print("Top-level keys:", list(data.keys()))
    print(f"departures count: {len(data.get('departures', []))}")
    print(f"arrivals count: {len(data.get('arrivals', []))}")

    # Print structure of first departure if available
    if data.get('departures'):
        print("\n--- FIRST DEPARTURE (sample structure) ---")
        dep = data['departures'][0]
        print(json.dumps(dep, indent=2, default=str)[:3000])

    # Print structure of first arrival if available
    if data.get('arrivals'):
        print("\n--- FIRST ARRIVAL (sample structure) ---")
        arr = data['arrivals'][0]
        print(json.dumps(arr, indent=2, default=str)[:3000])

    # Print all time-related fields found across all movements
    print("\n--- TIME FIELDS EXTRACTION CHECK ---")
    all_time_fields = set()
    for movement_type, items in [("departure", data.get("departures", [])), ("arrival", data.get("arrivals", []))]:
        for item in items[:3]:  # check first 3 items per type
            dep = item.get("departure", {}) or {}
            arr = item.get("arrival", {}) or {}
            
            print(f"\n{movement_type.upper()} movement:")
            if dep:
                print(f"  dep keys: {list(dep.keys())}")
                if "scheduledTime" in dep:
                    print(f"    scheduledTime: {dep['scheduledTime']}")
                if "revisedTime" in dep:
                    print(f"    revisedTime: {dep['revisedTime']}")
                if "predictedTime" in dep:
                    print(f"    predictedTime: {dep['predictedTime']}")
                if "runwayTime" in dep:
                    print(f"    runwayTime: {dep['runwayTime']}")
            if arr:
                print(f"  arr keys: {list(arr.keys())}")
                if "scheduledTime" in arr:
                    print(f"    scheduledTime: {arr['scheduledTime']}")
                if "revisedTime" in arr:
                    print(f"    revisedTime: {arr['revisedTime']}")
                if "predictedTime" in arr:
                    print(f"    predictedTime: {arr['predictedTime']}")
                if "runwayTime" in arr:
                    print(f"    runwayTime: {arr['runwayTime']}")

    print("\n--- SUMMARY ---")
    print("Scheduled times:", "Yes (scheduledTime field)" if data.get('departures') and data['departures'][0].get('departure', {}).get('scheduledTime') else "Check failed")
    has_arrival_actual = data.get('arrivals') and data['arrivals'][0].get('arrival', {}).get('runwayTime')
    has_arrival_pred = data.get('arrivals') and data['arrivals'][0].get('arrival', {}).get('predictedTime')
    print("Actual landing times: ", "Yes (runwayTime field)" if has_arrival_actual else "Not in sample")
    print("Estimated landing times: ", "Yes (predictedTime field)" if has_arrival_pred else "Not in sample")


if __name__ == '__main__':
    main()
