# fetch_airport_mesonet_data.py
#
# Fetch multi-year historical ASOS/METAR data for selected airports
# using the Iowa State Mesonet ASOS API.
#
# Docs: https://mesonet.agron.iastate.edu/request/download.phtml?network=AWOS
#
# Output: data/airports/asos_airports_YYYY-MM-DD_to_YYYY-MM-DD.csv

import os
import io
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional

import requests
import pandas as pd

# <-- NEW: Postgres-backed station service
from services.stations_service_pg import get_station_ids, get_station_info

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ---- Configuration ----

MESONET_ASOS_BASE_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"

# Date window: last 5 years from "today" (UTC)
END_DATE = datetime.utcnow().date()
START_DATE = END_DATE - timedelta(days=5 * 365)  # approx 5 years

# Where to store CSV locally
OUTPUT_DIR = os.path.join("data", "airports")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ---- Station helpers (backed by Postgres) ----

def get_airport_ids() -> List[str]:
    """
    Return all airport station_ids from the stations table.
    """
    return get_station_ids("airport")


def get_airport_name(station_id: str) -> str:
    """
    Look up a friendly station name for an airport from Postgres.
    Falls back to the raw station_id if not found.
    """
    info: Optional[Dict] = get_station_info("airport", station_id)
    if not info:
        return station_id
    # expecting columns: id, type, code, name, latitude, longitude, elevation_m, metadata
    return info.get("name") or station_id


# ---- Mesonet helpers ----

def _date_parts(d: datetime.date) -> Tuple[int, int, int]:
    """Return (year, month, day) tuple for a date."""
    return d.year, d.month, d.day


def build_mesonet_params(station_id: str,
                         start_date: datetime.date,
                         end_date: datetime.date) -> Dict[str, str]:
    """
    Build query parameters for the Mesonet ASOS API for a single station
    and a start/end date window.
    """
    y1, m1, d1 = _date_parts(start_date)
    y2, m2, d2 = _date_parts(end_date)

    params = {
        "station": station_id,
        "data": "all",          # all available variables
        "year1": str(y1),
        "month1": str(m1),
        "day1": str(d1),
        "year2": str(y2),
        "month2": str(m2),
        "day2": str(d2),
        "tz": "Etc/UTC",        # keep everything in UTC
        "format": "onlycomma",  # pure CSV, no metadata comments
        "latlon": "1",          # include lat/lon fields
    }
    return params


def fetch_asos_for_station(station_id: str,
                           start_date: datetime.date,
                           end_date: datetime.date) -> pd.DataFrame:
    """
    Fetch ASOS/METAR history for a single station from Mesonet ASOS API
    between start_date and end_date (inclusive).
    Returns a pandas DataFrame with 'timestamp', 'station_id', and 'station_name' columns.
    """
    station_name = get_airport_name(station_id)
    params = build_mesonet_params(station_id, start_date, end_date)

    logging.info(
        f"Fetching Mesonet ASOS data for {station_id} ({station_name}) "
        f"from {start_date} to {end_date}"
    )

    try:
        resp = requests.get(MESONET_ASOS_BASE_URL, params=params, timeout=60)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching ASOS data for {station_id}: {e}")
        return pd.DataFrame()

    text = resp.text.strip()
    if not text:
        logging.warning(f"No text returned for station {station_id}")
        return pd.DataFrame()

    # Try to read as CSV
    try:
        df = pd.read_csv(io.StringIO(text))
    except Exception as e:
        logging.error(f"Failed to parse CSV for station {station_id}: {e}")
        return pd.DataFrame()

    if df.empty:
        logging.warning(f"No rows parsed for station {station_id}")
        return df

    # Mesonet CSV usually has:
    # 'station', 'valid', 'tmpf', 'dwpf', 'relh', 'drct', 'sknt', 'p01i', 'alti', ...
    # We'll create a canonical 'timestamp' and attach station metadata.

    if "valid" in df.columns:
        # Parse 'valid' as UTC timestamp
        df["timestamp"] = pd.to_datetime(df["valid"], errors="coerce", utc=True)
    else:
        logging.warning(f"'valid' column missing for station {station_id}")
        df["timestamp"] = pd.NaT

    # Attach consistent station_id column
    if "station" in df.columns:
        df["station_id"] = df["station"].astype(str)
    else:
        df["station_id"] = station_id

    # Attach friendly station_name (from Postgres)
    df["station_name"] = station_name

    # Filter by our global window (with timestamps)
    start_ts = pd.Timestamp(START_DATE).tz_localize("UTC")
    end_ts = (
        pd.Timestamp(END_DATE)
        + pd.Timedelta(days=1)
        - pd.Timedelta(microseconds=1)
    ).tz_localize("UTC")

    df = df[
        df["timestamp"].notna()
        & (df["timestamp"] >= start_ts)
        & (df["timestamp"] <= end_ts)
    ]

    if df.empty:
        logging.warning(f"No rows within global window for station {station_id}")

    return df


def fetch_airport_data() -> pd.DataFrame:
    """
    Fetch ASOS/METAR data for all airport stations (read from Postgres)
    over the last ~5 years.
    Returns a single concatenated DataFrame.
    """
    all_frames = []

    airport_ids = get_airport_ids()
    if not airport_ids:
        logging.warning("No airport stations found in Postgres.")
        return pd.DataFrame()

    logging.info(
        f"Starting Mesonet ASOS fetch for airports: {airport_ids} "
        f"from {START_DATE} to {END_DATE}"
    )

    for station in airport_ids:
        df_station = fetch_asos_for_station(station, START_DATE, END_DATE)
        if not df_station.empty:
            all_frames.append(df_station)

    if not all_frames:
        logging.warning("No airport ASOS data fetched for any station.")
        return pd.DataFrame()

    df_all = pd.concat(all_frames, ignore_index=True)

    # Final sanity filter + dedupe
    start_ts = pd.Timestamp(START_DATE).tz_localize("UTC")
    end_ts = (
        pd.Timestamp(END_DATE)
        + pd.Timedelta(days=1)
        - pd.Timedelta(microseconds=1)
    ).tz_localize("UTC")

    df_all = df_all[
        df_all["timestamp"].notna()
        & (df_all["timestamp"] >= start_ts)
        & (df_all["timestamp"] <= end_ts)
    ]

    df_all = df_all.drop_duplicates(
        subset=["station_id", "timestamp"]
    ).sort_values(
        ["station_id", "timestamp"]
    ).reset_index(drop=True)

    return df_all


def main():
    logging.info("Starting airport ASOS data fetch via Mesonet...")
    df = fetch_airport_data()

    if df.empty:
        logging.warning("No airport ASOS data to save.")
        return

    start_str = START_DATE.isoformat()
    end_str = END_DATE.isoformat()
    out_path = os.path.join(
        OUTPUT_DIR,
        f"asos_airports_{start_str}_to_{end_str}.csv"
    )

    df.to_csv(out_path, index=False)
    logging.info(f"Saved airport ASOS data to {out_path}")

    # ---- Placeholder: BigQuery load or DB ingest ----
    # from utils.bigquery import load_dataframe_to_bq
    # load_dataframe_to_bq(
    #     df,
    #     table_id="your_project.your_dataset.asos_airports",
    #     write_disposition="WRITE_APPEND",
    # )


if __name__ == "__main__":
    main()
