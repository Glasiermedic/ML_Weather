# build_pws_observations.py
#
# Fetch PWS history from Weather Underground / weather.com for
# a limited range per station to avoid API blocking / security flags.
#
# KORMCMIN127: 2024-11-25 -> today
# KORMCMIN133: 2025-03-29 -> today
#
# Output:
#   data/pws/pws_observations_2024-11-25_to_<today>.csv
#
# Requires:
#   - WEATHER_API_KEY in .env or environment
#   - requests, pandas, python-dotenv

import os
import time
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict

import requests
import pandas as pd
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ------------- CONFIG ------------- #

# Per-station start dates to keep API calls reasonable
PWS_STATION_WINDOWS: Dict[str, date] = {
    "KORMCMIN127": date(2024, 12, 20),
    "KORMCMIN133": date(2025, 4, 25),
}

# End date = "present" (UTC date)
END_DATE = date.today()

# Units: 'e' = Imperial, 'm' = Metric
UNITS = "e"

# Seconds to sleep between each daily API request
REQUEST_SLEEP_SECONDS = 1.0

# Output folder
DATA_DIR = Path("data/pws")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ------------- CORE FUNCTIONS ------------- #


def fetch_pws_day(
    station_id: str,
    day: date,
    api_key: str,
    units: str = "e",
) -> pd.DataFrame:
    """
    Fetch one day of history data for a single PWS station
    from Weather Underground API.

    Returns a pandas DataFrame with a 'timestamp' column and 'station_id'.
    """
    base_url = "https://api.weather.com/v2/pws/history/hourly"

    date_str = day.strftime("%Y%m%d")  # e.g. 20251115

    params = {
        "stationId": station_id,
        "format": "json",
        "units": units,
        "date": date_str,
        "apiKey": api_key,
    }

    try:
        resp = requests.get(base_url, params=params, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"[{station_id}] {day} - request failed: {e}")
        return pd.DataFrame()

    data = resp.json()
    observations = data.get("observations", [])
    if not observations:
        logging.warning(f"[{station_id}] {day} - no observations returned")
        return pd.DataFrame()

    # Flatten JSON
    df = pd.json_normalize(observations)

    # Standardize timestamp column (prefer UTC)
    if "obsTimeUtc" in df.columns:
        df["timestamp"] = pd.to_datetime(df["obsTimeUtc"], errors="coerce", utc=True)
    elif "obsTimeLocal" in df.columns:
        df["timestamp"] = pd.to_datetime(df["obsTimeLocal"], errors="coerce")
    else:
        ts_col = None
        for c in df.columns:
            if "time" in c.lower():
                ts_col = c
                break
        if ts_col is not None:
            df["timestamp"] = pd.to_datetime(df[ts_col], errors="coerce")
        else:
            logging.warning(f"[{station_id}] {day} - no obvious time column found")
            df["timestamp"] = pd.NaT

    df = df.sort_values("timestamp").reset_index(drop=True)

    # Add station id as a column
    df["station_id"] = station_id

    return df


def fetch_pws_range(
    station_id: str,
    start_date: date,
    end_date: date,
    api_key: str,
    units: str = "e",
) -> pd.DataFrame:
    """
    Fetch history for a station from start_date to end_date (inclusive).
    Uses a small sleep between days to avoid triggering API rate limits.
    """
    all_dfs = []
    current = start_date

    logging.info(
        f"[{station_id}] Fetching data from {start_date.isoformat()} "
        f"to {end_date.isoformat()}"
    )

    while current <= end_date:
        df_day = fetch_pws_day(station_id, current, api_key, units)
        if not df_day.empty:
            all_dfs.append(df_day)

        # Rate limiting: sleep a bit between requests
        time.sleep(REQUEST_SLEEP_SECONDS)
        current += timedelta(days=1)

    if not all_dfs:
        logging.warning(f"[{station_id}] No data collected in range.")
        return pd.DataFrame()

    full_df = pd.concat(all_dfs, ignore_index=True)
    return full_df


def build_combined_pws_observations(
    station_windows: Dict[str, date],
    end_date: date,
    api_key: str,
    units: str = "e",
) -> pd.DataFrame:
    """
    Fetch the configured range for each PWS station and return a single
    combined DataFrame.
    Ensures:
      - 'timestamp' column present
      - 'station_id' present
      - sorted + deduped
    """
    all_frames = []

    for station_id, start_date in station_windows.items():
        if start_date > end_date:
            logging.warning(
                f"[{station_id}] start_date {start_date} is after end_date {end_date}; skipping."
            )
            continue

        df_station = fetch_pws_range(station_id, start_date, end_date, api_key, units)
        if df_station.empty:
            continue
        all_frames.append(df_station)

    if not all_frames:
        logging.warning("No PWS data fetched for any station.")
        return pd.DataFrame()

    df_all = pd.concat(all_frames, ignore_index=True)

    # Filter again just in case, and dedupe
    min_start = min(station_windows.values())
    start_ts = pd.Timestamp(min_start).tz_localize("UTC", nonexistent="NaT", ambiguous="NaT")
    end_ts = pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
    end_ts = end_ts.tz_localize("UTC", nonexistent="NaT", ambiguous="NaT")

    # If timestamps ended up naive, compare as naive
    if getattr(df_all["timestamp"].dtype, "tz", None) is None:
        df_all = df_all[
            df_all["timestamp"].notna()
            & (df_all["timestamp"] >= pd.Timestamp(min_start))
            & (df_all["timestamp"] <= pd.Timestamp(end_date) + pd.Timedelta(days=1))
        ]
    else:
        df_all = df_all[
            df_all["timestamp"].notna()
            & (df_all["timestamp"] >= start_ts)
            & (df_all["timestamp"] <= end_ts)
        ]

    df_all = df_all.drop_duplicates(
        subset=["station_id", "timestamp"]
    ).sort_values(["station_id", "timestamp"]).reset_index(drop=True)

    return df_all


def main():
    load_dotenv()
    api_key = os.getenv("WEATHER_API_KEY")

    if not api_key:
        raise SystemExit("❌ WEATHER_API_KEY is not set in the environment or .env file.")

    min_start = min(PWS_STATION_WINDOWS.values())
    logging.info(
        f"Building limited PWS observations from {min_start.isoformat()} "
        f"to {END_DATE.isoformat()} for stations: {list(PWS_STATION_WINDOWS.keys())}"
    )

    df = build_combined_pws_observations(
        station_windows=PWS_STATION_WINDOWS,
        end_date=END_DATE,
        api_key=api_key,
        units=UNITS,
    )

    if df.empty:
        logging.warning("No PWS observations to save.")
        return

    out_path = DATA_DIR / f"pws_observations_{min_start.isoformat()}_to_{END_DATE.isoformat()}.csv"
    df.to_csv(out_path, index=False)

    logging.info(f"[OK] Saved {len(df)} rows to {out_path}")


if __name__ == "__main__":
    main()
