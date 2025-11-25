# stations_registry.py

from typing import Dict, List, Optional, Literal, TypedDict

SourceType = Literal["buoy", "airport", "pws"]


class StationInfo(TypedDict, total=False):
    station_id: str
    name: str
    source_type: SourceType
    provider: str         # e.g. "NDBC", "Mesonet", "CWOP"
    region: str           # e.g. "OR Coast", "Willamette Valley"
    notes: str


# ---- MASTER STATION REGISTRY ----
# This is the **single place** you update when you add/remove stations.

STATION_REGISTRY: Dict[SourceType, Dict[str, StationInfo]] = {
    "buoy": {
        "46050": {
            "station_id": "46050",
            "name": "Stonewall Banks, OR",
            "source_type": "buoy",
            "provider": "NDBC",
            "region": "Oregon Coast",
        },
        "46029": {
            "station_id": "46029",
            "name": "Columbia River Bar, OR/WA",
            "source_type": "buoy",
            "provider": "NDBC",
            "region": "Oregon/Washington Coast",
        },
        "46041": {
            "station_id": "46041",
            "name": "Cape Elizabeth, OR",
            "source_type": "buoy",
            "provider": "NDBC",
            "region": "Oregon Coast",
        },
        "46087": {
            "station_id": "46087",
            "name": "Tillamook, OR",
            "source_type": "buoy",
            "provider": "NDBC",
            "region": "Oregon Coast",
        },
        "46047": {
            "station_id": "46047",
            "name": "Neah Bay, WA",
            "source_type": "buoy",
            "provider": "NDBC",
            "region": "Washington Coast",
        },
        "51001": {
            "station_id": "51001",
            "name": "Northwest Hawaii",
            "source_type": "buoy",
            "provider": "NDBC",
            "region": "Hawaii",
        },
    },

    "airport": {
        "KEUG": {
            "station_id": "KEUG",
            "name": "Eugene Airport",
            "source_type": "airport",
            "provider": "Mesonet ASOS",
            "region": "Willamette Valley",
        },
        "KSLE": {
            "station_id": "KSLE",
            "name": "Salem Airport",
            "source_type": "airport",
            "provider": "Mesonet ASOS",
            "region": "Willamette Valley",
        },
        "KMMV": {
            "station_id": "KMMV",
            "name": "McMinnville Airport",
            "source_type": "airport",
            "provider": "Mesonet ASOS",
            "region": "Willamette Valley",
        },
        "KONP": {
            "station_id": "KONP",
            "name": "Newport Airport",
            "source_type": "airport",
            "provider": "Mesonet ASOS",
            "region": "Oregon Coast",
        },
    },

    # PWS entries:
    "pws": {
         "KORMCMIN133": {
             "station_id": "KORMCMIN133",
             "name": "propdada",
             "source_type": "pws",
             "provider": "Weather Underground",
             "region": "McMinnville",
         },
         "KORMCMIN127": {
             "station_id": "KORMCMIN127",
             "name": "dustprop",
             "source_type": "pws",
             "provider": "Weather Underground",
             "region": "McMinnville",
         },
    },
}


# ---- SUPPORT FUNCTIONS ----

def get_station_ids(source_type: SourceType) -> List[str]:
    """
    Return a list of station IDs for a given source_type ('buoy', 'airport', 'pws').
    """
    return list(STATION_REGISTRY.get(source_type, {}).keys())


def get_station_info(source_type: SourceType, station_id: str) -> Optional[StationInfo]:
    """
    Look up full metadata for a single station.
    """
    return STATION_REGISTRY.get(source_type, {}).get(station_id)


def all_stations_flat() -> List[StationInfo]:
    """
    Flatten all stations across all source types into a single list.
    Useful when enriching merged DataFrames.
    """
    stations: List[StationInfo] = []
    for stype, stations_dict in STATION_REGISTRY.items():
        stations.extend(stations_dict.values())
    return stations
