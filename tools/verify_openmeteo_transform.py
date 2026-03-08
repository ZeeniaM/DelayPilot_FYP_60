import os
import sys
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ingest_weather_live import OPENMETEO_URL, OPENMETEO_ICAOS
import requests
import pandas as pd


def main():
    r = requests.get(OPENMETEO_URL, timeout=30)
    r.raise_for_status()
    data = r.json()

    report = []
    for i, loc in enumerate(data):
        hourly = loc.get('hourly', {})
        hourly_keys = set(hourly.keys())
        times = hourly.get('time', [])
        df = pd.DataFrame(hourly)
        # mimic ingest code
        df['hour_utc'] = pd.to_datetime(df['time'], utc=True, errors='coerce')
        df = df.drop(columns=['time'])
        df['airport_icao'] = OPENMETEO_ICAOS[i] if i < len(OPENMETEO_ICAOS) else None

        df_cols = set(df.columns)
        expected_cols = (hourly_keys - {'time'}) | {'hour_utc', 'airport_icao'}

        missing_from_df = (hourly_keys - {'time'}) - df_cols
        extra_in_df = df_cols - (hourly_keys - {'time'})

        # row count check
        rows_ok = len(df) == len(times)

        # dtype sample
        dtypes = df.dtypes.to_dict()

        report.append({
            'index': i,
            'assigned_icao': OPENMETEO_ICAOS[i] if i < len(OPENMETEO_ICAOS) else None,
            'hourly_keys_sorted': sorted(list(hourly_keys))[:50],
            'df_columns_sorted': sorted(list(df_cols))[:50],
            'missing_from_df': sorted(list(missing_from_df)),
            'extra_in_df': sorted(list(extra_in_df)),
            'rows_ok': rows_ok,
            'n_rows': len(df),
            'n_times': len(times),
            'dtypes_sample': {k: str(v) for k, v in list(dtypes.items())[:10]},
        })

    print(json.dumps(report, indent=2))

if __name__ == '__main__':
    main()
