import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import time


# ------------- CONFIG ------------- #

# Airport stations (ICAO IDs)
AIRPORT_STATIONS = {
    "KEUG": "eugene",
    "KSLE": "salem",
    "KMMV": "mcminnville",
    "KONP": "newport",
}

# Output folder
DATA_DIR = Path("data/raw_airports")

# NOAA base URL
NOAA_BASE = "https://api.weather.gov"

# Chunk size (days) per request to avoid huge responses
CHUNK_DAYS = 7

# User-Agent header (NOAA requires something identifying)
HEADERS = {
    "User-Agent": "ml-weather-demo (rolfson.data@gmail.com)",
    "Accept": "application/geo+json",
}


# ------------- CORE FUNCTIONS ------------- #

def fetch_station_chunk(station_id: str, start: datetime, end: datetime) -> pd.DataFrame:
    """
    Fetch observations for one station between start and end (UTC)
    using api.weather.gov/stations/{stationId}/observations.
    Returns a DataFrame of flattened properties, or empty DF if no data.
    """
    url = f"{NOAA_BASE}/stations/{station_id}/observations"

    params = {
        "start": start.replace(microsecond=0).isoformat() + "Z",
        "end": end.replace(microsecond=0).isoformat() + "Z",
        # "limit": 1000,  # default is fine; can uncomment if needed
    }

    print(f"  → Requesting {station_id} {params['start']} to {params['end']}")

    resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
    if resp.status_code != 200:
        print(f"    ❌ HTTP {resp.status_code}: {resp.text[:200]!r}")
        return pd.DataFrame()

    data = resp.json()
    features = data.get("features", [])
    if not features:
        print("    ⚠️  No features in response")
        return pd.DataFrame()

    # Flatten properties
    df = pd.json_normalize(features)

    # Standardize timestamp
    if "properties.timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["properties.timestamp"])
        df = df.set_index("timestamp").sort_index()
    else:
        # fallback
        print("    ⚠️  No properties.timestamp column found")
        return pd.DataFrame()

    return df


def fetch_station_range(station_id: str, start: datetime, end: datetime) -> pd.DataFrame:
    """
    Fetch full range [start, end] for a station in CHUNK_DAYS chunks.
    Returns concatenated DataFrame (possibly empty).
    """
    all_dfs = []
    current = start

    print(f"\n📡 Fetching {station_id} from {start.date()} to {end.date()}")

    while current < end:
        chunk_end = min(current + timedelta(days=CHUNK_DAYS), end)
        df_chunk = fetch_station_chunk(station_id, current, chunk_end)

        if not df_chunk.empty:
            all_dfs.append(df_chunk)

        current = chunk_end
        # polite pause to avoid hammering the API
        time.sleep(0.5)

    if not all_dfs:
        print(f"⚠️  No data collected for {station_id} in range")
        return pd.DataFrame()

    df_full = pd.concat(all_dfs)
    df_full = df_full[~df_full.index.duplicated(keep="first")]
    df_full = df_full.sort_index()

    print(f"✅ Collected {len(df_full)} observations for {station_id}")
    return df_full


def standardize_airport_df(raw_df: pd.DataFrame, station_id: str) -> pd.DataFrame:
    """
    Take the raw NOAA observation DF and extract key numeric fields into
    a cleaner frame:
      - temp (C)
      - dewpoint (C)
      - pressure (Pa or hPa, depending on your choice)
      - wind_speed (m/s)
      - wind_gust (m/s)
      - precip_last_hour (mm)
    """
    df = raw_df.copy()

    # Helper: pick nested numeric values from columns like properties.temperature.value
    def col_exists(name: str) -> bool:
        return name in df.columns

    out = pd.DataFrame(index=df.index)

    # Temperature
    if col_exists("properties.temperature.value"):
        out[f"temp_{station_id}"] = df["properties.temperature.value"]

    # Dewpoint
    if col_exists("properties.dewpoint.value"):
        out[f"dewpoint_{station_id}"] = df["properties.dewpoint.value"]

    # Wind speed & gust
    if col_exists("properties.windSpeed.value"):
        out[f"wind_speed_{station_id}"] = df["properties.windSpeed.value"]
    if col_exists("properties.windGust.value"):
        out[f"wind_gust_{station_id}"] = df["properties.windGust.value"]

    # Pressure (barometric)
    if col_exists("properties.barometricPressure.value"):
        out[f"pressure_{station_id}"] = df["properties.barometricPressure.value"]

    # Precipitation last hour
    if col_exists("properties.precipitationLastHour.value"):
        out[f"precip_last_hour_{station_id}"] = df["properties.precipitationLastHour.value"]

    # You can add more fields here if useful:
    # visibility, relativeHumidity, etc.

    # Resample to hourly mean (NOAA obs are often ~hourly already)
    out = out.resample("1H").mean()

    return out


def save_station_csv(station_id: str, alias: str, start: datetime, end: datetime, out_dir: Path = DATA_DIR) -> Path:
    """
    Fetch all data for a station, standardize numeric fields,
    and save to data/raw_airports/<alias>.csv.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_df = fetch_station_range(station_id, start, end)
    if raw_df.empty:
        out_path = out_dir / f"{alias}_EMPTY.csv"
        print(f"⚠️  Nothing to save for {station_id}, writing empty marker: {out_path}")
        out_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame().to_csv(out_path, index=False)
        return out_path

    std_df = standardize_airport_df(raw_df, station_id)
    out_path = out_dir / f"{alias}.csv"
    std_df.to_csv(out_path)
    print(f"💾 Saved standardized data for {station_id} → {out_path} ({len(std_df)} rows)")
    return out_path


# ------------- MAIN ------------- #

if __name__ == "__main__":
    # Last 5 years up to now
    end = datetime.utcnow()
    start = end - timedelta(days=1 * 365)
    print("⏱ Date range:", start.date(), "→", end.date())

    for station_id, alias in AIRPORT_STATIONS.items():
        save_station_csv(
            station_id=station_id,
            alias=alias,
            start=start,
            end=end,
            out_dir=DATA_DIR,
        )
