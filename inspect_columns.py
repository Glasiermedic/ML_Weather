# inspect_columns.py
#
# Inspect column schemas across multiple datasets:
#   - PWS combined observations
#   - NDBC buoys
#   - Airport ASOS (Mesonet)
#   - (optionally) upper-air, when available
#
# Outputs:
#   1) Console summary of columns + dtypes per dataset
#   2) Station list per dataset (if station_id is present)
#   3) A "rotated" table with rows = column names,
#      columns = dataset names, values = non-null counts,
#      saved as data/schema/column_nonnull_matrix.csv

import os
import logging
from typing import Dict, Optional

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ---- Config: paths to the CSVs you already generated ----
# Adjust START/END dates or patterns if needed.
DATASETS: Dict[str, str] = {
    # Combined PWS observations (built via build_pws_observations_csv.py)
    "pws": r"data\pws\pws_observations_2024-11-25_to_2025-11-18.csv",

    # NDBC historical + realtime buoy data
    "ndbc_buoys": r"data\buoys\ndbc_buoys_2020-11-19_to_2025-11-18.csv",

    # Airport ASOS data from Mesonet
    "airports": r"data\airports\asos_airports_2020-11-19_to_2025-11-18.csv",

    # Placeholder for future upper-air data
    "upper_air": r"data\upper_air\upper_air_2020-11-17_to_2025-11-18.csv",
}

# Where to write the non-null count matrix
SCHEMA_DIR = "data/schema"
os.makedirs(SCHEMA_DIR, exist_ok=True)


def load_dataset(name: str, path: str) -> Optional[pd.DataFrame]:
    """Load a dataset if the file exists; otherwise return None."""
    if not os.path.exists(path):
        logging.warning(f"[{name}] File not found: {path}")
        return None

    logging.info(f"[{name}] Reading columns from: {path}")
    try:
        df = pd.read_csv(path, low_memory=False)
    except Exception as e:
        logging.error(f"[{name}] Failed to read {path}: {e}")
        return None

    logging.info(f"[{name}] Loaded shape: {df.shape[0]} rows, {df.shape[1]} columns")
    return df


def main():
    # 1. Load all datasets that actually exist
    dfs: Dict[str, pd.DataFrame] = {}
    for name, path in DATASETS.items():
        df = load_dataset(name, path)
        if df is not None:
            dfs[name] = df

    if not dfs:
        logging.error("No datasets loaded. Nothing to inspect.")
        return

    # 2. Column + dtype + station list per dataset
    print("\n================ COLUMN / DTYPE BY DATASET ================\n")

    for name, df in dfs.items():
        print(f"Dataset: {name}")
        print(f"File: {DATASETS[name]}")
        print(f"Row count: {df.shape[0]}")
        print(f"Column count: {df.shape[1]}")
        print("Columns (name : dtype):")

        # Show columns + pandas dtype
        for col in df.columns:
            print(f"  - {col}: {df[col].dtype}")

        # Station list if station_id present
        if "station_id" in df.columns:
            stations = sorted(df["station_id"].dropna().astype(str).unique())
            print(f"  Unique station_id count: {len(stations)}")
            # If list is long, just show first few
            preview = ", ".join(stations[:10])
            suffix = "..." if len(stations) > 10 else ""
            print(f"  station_id sample: {preview}{suffix}")
        elif "station" in df.columns:
            stations = sorted(df["station"].dropna().astype(str).unique())
            print(f"  Unique station count (from 'station' col): {len(stations)}")
            preview = ", ".join(stations[:10])
            suffix = "..." if len(stations) > 10 else ""
            print(f"  station sample: {preview}{suffix}")

        print()

    # 3. Build union of all columns across datasets
    all_columns = set()
    for df in dfs.values():
        all_columns.update(df.columns)

    all_columns = sorted(all_columns)

    print("\n=============== COLUMN UNION ACROSS DATASETS ===============\n")
    for col in all_columns:
        print(f"  - {col}")
    print()

    # 4. Build "rotated" non-null matrix: rows = column, cols = dataset, values = non-null counts
    #    If a dataset doesn't have a column, count is 0.
    matrix = pd.DataFrame(index=all_columns, columns=dfs.keys(), dtype="Int64")

    for name, df in dfs.items():
        for col in all_columns:
            if col in df.columns:
                nn = df[col].notna().sum()
            else:
                nn = 0
            matrix.loc[col, name] = nn

    print("=============== NON-NULL COUNT MATRIX ======================\n")
    print(matrix.fillna(0))

    # 5. Save matrix to CSV for easier offline inspection
    out_path = os.path.join(SCHEMA_DIR, "column_nonnull_matrix.csv")
    matrix.to_csv(out_path)
    print(f"\n[OK] Saved non-null count matrix to {out_path}\n")


if __name__ == "__main__":
    main()
