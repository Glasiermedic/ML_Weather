"""
stations_service_pg.py

Postgres-backed station metadata service.

Provides a simple interface:

- get_station_ids(station_type) -> List[str]
- get_station_info(station_id) -> Dict | None
- get_all_stations(station_type=None) -> List[Dict]

Expected Postgres table (example):

CREATE TABLE stations (
    station_id      TEXT PRIMARY KEY,
    station_name    TEXT,
    station_type    TEXT NOT NULL,  -- 'pws', 'airport', 'buoy'
    source_system   TEXT,           -- 'WU_PWS', 'NDBC', 'MESONET_ASOS', etc.
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    elevation_m     DOUBLE PRECISION,
    timezone        TEXT,
    active          BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

You need a DSN in env, e.g.:

  WEATHER_DB_DSN="dbname=weather user=postgres password=secret host=localhost port=5432"

Install driver:

  pip install psycopg2-binary
"""

from __future__ import annotations

import os
from dotenv import load_dotenv, find_dotenv
from typing import List, Optional, Dict, Any

import psycopg2
from psycopg2.extras import RealDictCursor



# This searches up the directory tree for a .env file and loads it
load_dotenv(find_dotenv())
# ---- Config ----

# Prefer a single DSN env var; adjust name if you prefer
DB_DSN = os.getenv("WEATHER_DB_DSN")

if not DB_DSN:
    # You can either:
    #  - set WEATHER_DB_DSN in your .env / environment
    #  - or hard-code a fallback DSN here (not recommended for prod)
    #
    # Example:
    # DB_DSN = "dbname=weather user=postgres password=secret host=localhost port=5432"
    raise RuntimeError(
        "WEATHER_DB_DSN is not set. "
        "Set it in your environment or .env file, e.g.\n"
        'WEATHER_DB_DSN="dbname=weather user=postgres password=secret host=localhost port=5432"'
    )


def _get_conn():
    """
    Get a new Postgres connection using DB_DSN.
    Use RealDictCursor so we get dict rows instead of tuples.
    """
    return psycopg2.connect(DB_DSN, cursor_factory=RealDictCursor)


# ---- Public API ----

def get_station_ids(station_type: str) -> List[str]:
    """
    Return a list of station_id values for a given station_type.

    station_type expected values:
      - 'pws'
      - 'airport'
      - 'buoy'
    """
    sql = """
        SELECT station_id
        FROM stations
        WHERE station_type = %s
          AND active = TRUE
        ORDER BY station_id
    """

    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (station_type,))
        rows = cur.fetchall()

    return [row["station_id"] for row in rows]


def get_station_info(station_id: str) -> Optional[Dict[str, Any]]:
    """
    Return a dict of all columns for a single station_id, or None if not found.
    """
    sql = """
        SELECT *
        FROM stations
        WHERE station_id = %s
        LIMIT 1
    """

    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (station_id,))
        row = cur.fetchone()

    return dict(row) if row else None


def get_all_stations(station_type: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Return a list of station dicts.

    If station_type is provided, filter to that type.
    Otherwise, return all stations.
    """
    if station_type:
        sql = """
            SELECT *
            FROM stations
            WHERE station_type = %s
            ORDER BY station_type, station_id
        """
        params = (station_type,)
    else:
        sql = """
            SELECT *
            FROM stations
            ORDER BY station_type, station_id
        """
        params = ()

    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    return [dict(r) for r in rows]


# Optional: small self-test when run directly
if __name__ == "__main__":
    print("Testing stations_service_pg ...")

    try:
        all_stations = get_all_stations()
        print(f"Total stations: {len(all_stations)}")
        if all_stations:
            print("First station example:", all_stations[0])

        pws_ids = get_station_ids("pws")
        print(f"PWS station IDs: {pws_ids}")

    except Exception as e:
        print("Error during self-test:", e)
