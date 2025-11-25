# fetch_buoy_data.py

import os
import io
import gzip
import logging
from datetime import datetime, timedelta
from typing import List

import requests
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ---- Configuration ----

BUOY_STATIONS: List[str] = [
    "46050",  # Stonewall Banks, OR
    "46029",  # Columbia River Bar, OR/WA
    "46041",  # Cape Elizabeth, OR
    "46087",  # Tillamook, OR
    "46047",  # Neah Bay, WA
    "51001",  # Northwest Hawaii
]

NDBC_BASE_URL = "https://www.ndbc.noaa.gov/data/historical/stdmet"
NDBC_REALTIME_URL = "https://www.ndbc.noaa.gov/data/realtime2"

# Date window: last 5 years from "today"
END_DATE = datetime.utcnow().date()
START_DATE = END_DATE - timedelta(days=5 * 365)  # approx 5 years

# Where to store CSV locally (change as needed)
OUTPUT_DIR = os.path.join("data", "buoys")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def get_year_range(start_date: datetime.date, end_date: datetime.date) -> List[int]:
    """Return list of years that intersect [start_date, end_date]."""
    return list(range(start_date.year, end_date.year + 1))

def build_realtime_url(station_id: str) -> str:
    return f"{NDBC_REALTIME_URL}/{station_id}.txt"

def build_ndbc_url(station_id: str, year: int) -> str:
    """
    Build the NDBC historical stdmet URL:
    Example: https://www.ndbc.noaa.gov/data/historical/stdmet/46050h2024.txt.gz
    """
    filename = f"{station_id}h{year}.txt.gz"
    return f"{NDBC_BASE_URL}/{filename}"


def fetch_ndbc_year(station_id: str, year: int) -> pd.DataFrame:
    """
    Fetch a single station-year stdmet file from NDBC and return as DataFrame.
    If file or data aren't available, returns empty DataFrame.
    """
    url = build_ndbc_url(station_id, year)
    logging.info(f"Fetching {station_id} {year} from {url}")

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if resp.status_code == 404:
            logging.warning(f"No file for station {station_id}, year {year} (404).")
        else:
            logging.error(f"HTTP error for {station_id} {year}: {e}")
        return pd.DataFrame()
    except requests.exceptions.RequestException as e:
        logging.error(f"Request error for {station_id} {year}: {e}")
        return pd.DataFrame()

    try:
        # Decompress .gz in-memory
        with gzip.GzipFile(fileobj=io.BytesIO(resp.content)) as gz:
            raw_text = gz.read().decode("utf-8", errors="ignore")
    except Exception as e:
        logging.error(f"Failed to decompress/parse gzip for {station_id} {year}: {e}")
        return pd.DataFrame()

    lines = raw_text.splitlines()

    # Drop comment / metadata lines (starting with '#'), keep only pure data rows
    data_lines = [ln for ln in lines if (not ln.startswith("#")) and ln.strip()]

    if not data_lines:
        logging.warning(f"No data lines for {station_id} {year}")
        return pd.DataFrame()

    # Canonical NDBC stdmet columns (order as documented by NDBC)
    # Not all stations/years have all columns; we'll truncate as needed.
    canonical_cols = [
        "YY", "MM", "DD", "hh", "mm",
        "WDIR",  # Wind direction (degT)
        "WSPD",  # Wind speed (m/s or kt depending on unit)
        "GST",   # Wind gust
        "WVHT",  # Significant wave height
        "DPD",   # Dominant wave period
        "APD",   # Average wave period
        "MWD",   # Mean wave direction
        "PRES",  # Sea-level pressure
        "ATMP",  # Air temperature
        "WTMP",  # Sea surface temperature
        "DEWP",  # Dewpoint temperature
        "VIS",   # Visibility
        "TIDE",  # Tide
    ]

    # Read the data rows as a whitespace-delimited table with no header
    df = pd.read_csv(
        io.StringIO("\n".join(data_lines)),
        sep=r"\s+",
        header=None,
        engine="python",
    )

    # Align the number of columns to the canonical list
    n = min(df.shape[1], len(canonical_cols))
    df = df.iloc[:, :n]
    df.columns = canonical_cols[:n]

    # ---- Build timestamp ----

    # Some files use 4-digit year but the header is still "YY"; that's fine for pandas.
    # We just treat whatever is in "YY" as the year value.
    time_cols = {"YY", "MM", "DD", "hh", "mm"}
    if time_cols.issubset(set(df.columns)):
        time_df = df[["YY", "MM", "DD", "hh", "mm"]].rename(
            columns={
                "YY": "year",
                "MM": "month",
                "DD": "day",
                "hh": "hour",
                "mm": "minute",
            }
        )

        df["timestamp"] = pd.to_datetime(time_df, errors="coerce", utc=False)
    else:
        logging.warning(
            f"Missing expected time columns for {station_id} {year}, "
            f"columns present: {df.columns.tolist()}"
        )
        df["timestamp"] = pd.NaT

    df["station_id"] = station_id

    # ---- Filter by global date window using Timestamps (no .dt.date) ----
    start_ts = pd.Timestamp(START_DATE)
    end_ts = pd.Timestamp(END_DATE) + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)

    df = df[
        df["timestamp"].notna()
        & (df["timestamp"] >= start_ts)
        & (df["timestamp"] <= end_ts)
    ]

    return df

def fetch_ndbc_realtime(station_id: str) -> pd.DataFrame:
    """
    Fetch current realtime stdmet data (last ~45 days) for a station.
    Returns a DataFrame with a 'timestamp' column and 'station_id'.
    """
    url = build_realtime_url(station_id)
    logging.info(f"Fetching realtime data for {station_id} from {url}")

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if resp.status_code == 404:
            logging.warning(f"No realtime file for station {station_id} (404).")
        else:
            logging.error(f"HTTP error for realtime {station_id}: {e}")
        return pd.DataFrame()
    except requests.exceptions.RequestException as e:
        logging.error(f"Request error for realtime {station_id}: {e}")
        return pd.DataFrame()

    lines = resp.text.splitlines()

    # Keep only non-empty lines
    non_empty = [ln for ln in lines if ln.strip()]

    # Find the first non-comment line as header
    header_idx = None
    for i, ln in enumerate(non_empty):
        if not ln.startswith("#"):
            header_idx = i
            break

    if header_idx is None:
        logging.warning(f"No header/data in realtime file for {station_id}")
        return pd.DataFrame()

    header_line = non_empty[header_idx]
    raw_col_names = header_line.split()

    # --- Deduplicate column names so pandas doesn't error ---
    col_names = []
    seen = {}
    for name in raw_col_names:
        if name in seen:
            seen[name] += 1
            col_names.append(f"{name}_{seen[name]}")
        else:
            seen[name] = 0
            col_names.append(name)

    records = non_empty[header_idx + 1:]
    if not records:
        logging.warning(f"No records in realtime file for {station_id}")
        return pd.DataFrame()

    df = pd.read_csv(
        io.StringIO("\n".join(records)),
        sep=r"\s+",
        names=col_names,
        engine="python",
    )

    # Realtime stdmet usually uses YY instead of YYYY
    if {"YY", "MM", "DD", "hh", "mm"}.issubset(df.columns):
        # Convert YY -> YYYY (assume 2000+YY for modern buoys)
        df["YYYY"] = 2000 + df["YY"].astype(int)

        df["timestamp"] = pd.to_datetime(
            df[["YYYY", "MM", "DD", "hh", "mm"]].rename(
                columns={
                    "YYYY": "year",
                    "MM": "month",
                    "DD": "day",
                    "hh": "hour",
                    "mm": "minute",
                }
            ),
            errors="coerce"
        )
    else:
        logging.warning(
            f"Missing expected time columns in realtime for {station_id}, "
            f"columns present: {df.columns.tolist()}"
        )
        df["timestamp"] = pd.NaT

    df["station_id"] = station_id

    # Filter to your global window (using timestamps, not .dt.date)
    start_ts = pd.Timestamp(START_DATE)
    end_ts = pd.Timestamp(END_DATE) + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)

    df = df[
        df["timestamp"].notna()
        & (df["timestamp"] >= start_ts)
        & (df["timestamp"] <= end_ts)
    ]

    return df


def fetch_buoy_data() -> pd.DataFrame:
    """
    Fetch buoy data for all configured stations over the last 5 years.
    Uses historical yearly archives up to last full year,
    and realtime feed for the current year.
    """
    all_frames = []

    # Historical years: from START.year up to year before END.year
    hist_years = list(range(START_DATE.year, END_DATE.year))
    logging.info(
        f"Fetching NDBC buoy data from {START_DATE} to {END_DATE} "
        f"(historical years={hist_years}, plus realtime for {END_DATE.year})"
    )

    # 1) Historical archives
    for station in BUOY_STATIONS:
        for year in hist_years:
            df_year = fetch_ndbc_year(station, year)
            if not df_year.empty:
                all_frames.append(df_year)

        # 2) Realtime for current year
        df_rt = fetch_ndbc_realtime(station)
        if not df_rt.empty:
            all_frames.append(df_rt)

    if not all_frames:
        logging.warning("No data fetched for any buoys.")
        return pd.DataFrame()

    df_all = pd.concat(all_frames, ignore_index=True)

    # Filter again just to be safe and dedupe
    start_ts = pd.to_datetime(START_DATE)
    end_ts = pd.to_datetime(END_DATE)
    df_all = df_all[
        (df_all["timestamp"].notna()) &
        (df_all["timestamp"] >= start_ts) &
        (df_all["timestamp"] <= end_ts)
    ]

    df_all = df_all.drop_duplicates(subset=["station_id", "timestamp"]).sort_values(
        ["station_id", "timestamp"]
    ).reset_index(drop=True)

    return df_all



def main():
    logging.info("Starting buoy data fetch...")
    df = fetch_buoy_data()

    if df.empty:
        logging.warning("No buoy data to save.")
        return

    # ---- Local CSV output ----
    start_str = START_DATE.isoformat()
    end_str = END_DATE.isoformat()
    out_path = os.path.join(
        OUTPUT_DIR,
        f"ndbc_buoys_{start_str}_to_{end_str}.csv"
    )

    df.to_csv(out_path, index=False)
    logging.info(f"Saved buoy data to {out_path}")

    # ---- Placeholder: BigQuery load or DB ingest ----
    # Here is where you can call your existing BigQuery upload logic, e.g.:
    #
    # from utils.bigquery import load_dataframe_to_bq
    # load_dataframe_to_bq(
    #     df,
    #     table_id="your_project.your_dataset.ndbc_buoys",
    #     write_disposition="WRITE_APPEND"
    # )
    #
    # Or whatever pattern you're using for the PWS data.


if __name__ == "__main__":
    main()
