import os
import sys
import traceback

# Ensure workspace root is on sys.path so imports from project root work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ingest_weather_live import fetch_openmeteo_multi


def main():
    try:
        df = fetch_openmeteo_multi()
        print("ROWS:", len(df))
        print("COLUMNS:", list(df.columns))
        # Print first 50 rows as CSV for readability
        print(df.head(50).to_csv(index=False))
    except Exception as e:
        print("ERROR:", e)
        traceback.print_exc()


if __name__ == "__main__":
    main()
