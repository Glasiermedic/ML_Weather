# fetch_pws_data.py
#
# Fetch multi-year historical data for Personal Weather Stations (PWS)
# using the Weather Underground / weather.com PWS API.
#
# Station metadata (ids, friendly names, etc.) come from Postgres
# via services.stations_service_pg.
#
# Output: data/pws/pws_YYYY-MM-DD_to_YYYY-MM-DD.csv

import os
import io
import logging
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import List, Dict, Optional

import requests
import pandas as pd
from dotenv import load_dotenv

from services.stations_service_pg import get_station_ids, get_station_info

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Load environment (.env) so WEATHER_API_KEY is available
load_dotenv()

# ------------- CONFIG ------------- #

# Weather Underground / weather.com API key
API_KEY = os.getenv("WEATHER_API_KEY")

# Units: 'e' = Imperial, 'm' = Metric
UNITS = "e"

# Date window: last 5 years from "today" (UTC)
END_DATE: date = datetime.utcnow().date()
START_DATE: date = END_DATE - timedelta(days=5 * 365)  # approx 5 years

# Output folder (change to Path("data/raw") if you prefer the old layout)
OUTPUT_DIR = Path("data/pws")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

WU_BASE_URL = "https://api.weather.com/v2/pws/history/hourly"


# ------------- STATION HELPERS (Postgres) ------------- #

def get_pws_ids() -> List[str]:
    """
    Return all PWS station logical IDs (codes) from the stations table.
    In the schema we discussed, this is stations.code where type='pws'.
    """
    return get_station_ids("pws")


def get_pws_info(station_id: str) -> Optional[Dict]:
    """
    Return full station info dict for a PWS.
    Expected shape (from stations_service_pg):
        {
          "id": 1,
          "type": "pws",
          "code": "KORMCMIN127",
          "name": "Backyard PWS - McMinnville",
          "latitude": 45.2,
          "longitude": -123.2,
          "elevation_m": 60.0,
          "metadata": {"wu_station_id": "KORMCMIN127", ...}
        }
    """
    return get_station_info("pws", station_id)


def get_pws_name(station_id: str) -> str:
    info = get_pws_info(station_id)
    if not info:
        return station_id
    return info.get("name") or station_id


def get_wu_station_id(station_id: str) -> str:
    """
    Return the ID to send to Weather Underground API.

    Priority:
      1) metadata["wu_station_id"]
      2) stations.code
      3) the logical station_id passed in
    """
    info = get_pws_info(station_id)
    if not info:
        return station_id

    metadata = info.get("metadata") or {}
    if isinstance(metadata, str):
        # If stored as JSON text, try to parse
        try:
            metadata = json.loads(metadata)
        except Exception:
            metadata = {}

    if "wu_station_id" in metadata:
        return metadata["wu_station_id"]

    code = info.get("code")
    if code:
        return code

    return station_id


# ------------- CORE FETCH FUNCTIONS ------------- #

def fetch_pws_day(
    station_id: str,
    date_obj: date,
    api_key: str,
    units: str = "e"
) -> pd.DataFrame:
    """
    Fetch one day of history data for a single PWS station from
    Weather Underground API. Returns a pandas DataFrame with
    flattened observation fields.
    """
    wu_station_id = get_wu_station_id(station_id)
    date_str = date_obj.strftime("%Y%m%d")  # e.g. 20250115

    params = {
        "stationId": wu_station_id,
        "format": "json",
        "units": units,
        "date": date_str,
        "apiKey": api_key,
    }

    logging.debug(
        f"Requesting WU PWS day for {station_id} (WU id={wu_station_id}) "
        f"date={date_str}"
    )

    resp = requests.get(WU_BASE_URL, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    observations = data.get("observations", [])
    if not observations:
        logging.warning(
            f"No observations for PWS {station_id} (WU id={wu_station_id}) "
            f"on {date_str}"
        )
        return pd.DataFrame()

    # Normalize JSON to DataFrame
    df = pd.json_normalize(observations)

    # Standardize timestamp column
    # Typically: obsTimeUtc
    if "obsTimeUtc" in df.columns:
        df["timestamp"] = pd.to_datetime(df["obsTimeUtc"], errors="coerce", utc=True)
    elif "obsTimeLocal" in df.columns:
        df["timestamp"] = pd.to_datetime(df["obsTimeLocal"], errors="coerce")
    else:
        # fallback: first datetime-looking col
        time_col = None
        for c in df.columns:
            if "time" in c.lower():
                time_col = c
                break
        if time_col:
            df["timestamp"] = pd.to_datetime(df[time_col], errors="coerce")
        else:
            logging.error(
                f"No obvious time column in WU response for PWS {station_id}; "
                f"columns={df.columns.tolist()}"
            )
            df["timestamp"] = pd.NaT

    df = df.sort_values("timestamp").reset_index(drop=True)

    # Add station id as a column (logical ID) for joining to our registry
    df["station_id"] = station_id

    # Attach station_name now to keep everything self-contained
    df["station_name"] = get_pws_name(station_id)

    return df


def fetch_pws_range(
    station_id: str,
    start_date: date,
    end_date: date,
    api_key: str,
    units: str = "e"
) -> pd.DataFrame:
    """
    Fetch history for a station from start_date to end_date (inclusive).
    Returns a concatenated DataFrame.
    """
    all_dfs: List[pd.DataFrame] = []
    current = start_date

    logging.info(
        f"Fetching data for PWS {station_id} from {start_date} to {end_date}"
    )

    while current <= end_date:
        try:
            df_day = fetch_pws_day(station_id, current, api_key, units)
            if not df_day.empty:
                all_dfs.append(df_day)
        except Exception as e:
            logging.error(
                f"Error fetching PWS {station_id} {current}: {e}"
            )
        current += timedelta(days=1)

    if not all_dfs:
        logging.warning(
            f"No data collected for PWS {station_id} in range "
            f"{start_date} to {end_date}"
        )
        return pd.DataFrame()

    full_df = pd.concat(all_dfs, ignore_index=True)
    return full_df


def fetch_pws_data() -> pd.DataFrame:
    """
    Fetch PWS data for all PWS stations (from Postgres) over the last ~5 years.
    Returns a single concatenated DataFrame.
    """
    if not API_KEY:
        raise SystemExit(
            "❌ WEATHER_API_KEY is not set in the environment. "
            "Set it in your .env or environment before running."
        )

    pws_ids = get_pws_ids()
    if not pws_ids:
        logging.warning("No PWS stations found in Postgres.")
        return pd.DataFrame()

    logging.info(
        f"Starting PWS fetch for stations: {pws_ids} "
        f"from {START_DATE} to {END_DATE}"
    )

    all_frames: List[pd.DataFrame] = []

    for station in pws_ids:
        df_station = fetch_pws_range(station, START_DATE, END_DATE, API_KEY, UNITS)
        if not df_station.empty:
            all_frames.append(df_station)

    if not all_frames:
        logging.warning("No PWS data fetched for any station.")
        return pd.DataFrame()

    df_all = pd.concat(all_frames, ignore_index=True)

    # Final sanity filter + dedupe
    # (WU is already date-filtered by the API, but this keeps us consistent
    #  with the buoy/airport scripts.)
    start_ts = pd.Timestamp(START_DATE)
    end_ts = (
        pd.Timestamp(END_DATE)
        + pd.Timedelta(days=1)
        - pd.Timedelta(microseconds=1)
    )

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


# ------------- MAIN ------------- #

def main():
    logging.info("Starting PWS data fetch...")
    df = fetch_pws_data()

    if df.empty:
        logging.warning("No PWS data to save.")
        return

    start_str = START_DATE.isoformat()
    end_str = END_DATE.isoformat()
    out_path = OUTPUT_DIR / f"pws_{start_str}_to_{end_str}.csv"

    df.to_csv(out_path, index=False)
    logging.info(f"Saved PWS data to {out_path}")

    # ---- Placeholder: BigQuery load or DB ingest ----
    # from utils.bigquery import load_dataframe_to_bq
    # load_dataframe_to_bq(
    #     df,
    #     table_id="your_project.your_dataset.pws",
    #     write_disposition="WRITE_APPEND",
    # )


if __name__ == "__main__":
    main()
