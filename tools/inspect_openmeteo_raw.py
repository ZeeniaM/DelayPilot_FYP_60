import os
import sys
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ingest_weather_live import OPENMETEO_URL, OPENMETEO_ICAOS
import requests


def main():
    r = requests.get(OPENMETEO_URL, timeout=30)
    r.raise_for_status()
    data = r.json()
    print('TYPE:', type(data))
    print('NUM_LOCATIONS:', len(data))
    for i, loc in enumerate(data):
        print('\n--- LOCATION', i, '---')
        print('assigned_icao:', OPENMETEO_ICAOS[i] if i < len(OPENMETEO_ICAOS) else None)
        top_keys = list(loc.keys())
        print('top_keys:', top_keys)
        # common metadata fields
        for k in ('location_id','latitude','longitude','elevation','utc_offset_seconds','timezone','timezone_abbreviation'):
            if k in loc:
                print(f'{k}:', loc[k])
        # Print coordinate pair if present
        if 'latitude' in loc and 'longitude' in loc:
            print('coords:', loc.get('latitude'), loc.get('longitude'))
        # hourly
        hourly = loc.get('hourly')
        if isinstance(hourly, dict):
            print('hourly_keys:', list(hourly.keys()))
            times = hourly.get('time', [])
            print('hourly_len(time):', len(times))
            print('hourly_sample_times[0:5]:', times[:5])
        else:
            print('no hourly data')
    # print compact first location JSON for manual inspection
    if len(data) > 0:
        print('\n--- FIRST LOCATION RAW (compact) ---')
        print(json.dumps(data[0], indent=2)[:2000])

if __name__ == '__main__':
    main()
