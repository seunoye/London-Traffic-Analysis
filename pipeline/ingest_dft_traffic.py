
"""
pipeline/ingest_dft_traffic.py
Fetch DfT Average Annual Daily Flow (AADF) data for London → GeoDataFrame.

API: https://roadtraffic.dft.gov.uk/api/average-annual-daily-flow
Docs: https://roadtraffic.dft.gov.uk/

No API key required. Data is paginated; all London pages are fetched.
London is identified by region_name == "London" in the API response.
"""

import os
import time

import geopandas as gpd
import numpy as np
import pandas as pd
import requests
from shapely.geometry import Point
from loguru import logger

AADF_URL = "https://roadtraffic.dft.gov.uk/api/average-annual-daily-flow"

# Most recent year to fetch. Override via DFT_YEAR env var.
# The API holds data from 2000 onwards; latest is typically the prior calendar year.
DEFAULT_YEAR = int(os.environ.get("DFT_YEAR", 2025))

_VEHICLE_COLS = [
    "pedal_cycles",
    "two_wheeled_motor_vehicles",
    "cars_and_taxis",
    "buses_and_coaches",
    "lgvs",
    "hgvs_2_rigid_axle",
    "hgvs_3_rigid_axle",
    "hgvs_4_or_more_rigid_axle",
    "hgvs_3_or_4_articulated_axle",
    "hgvs_5_articulated_axle",
    "hgvs_6_articulated_axle",
    "all_hgvs",
    "all_motor_vehicles",
]


def _fetch_page(page: int, year: int, max_retries: int = 5) -> dict:
    for attempt in range(max_retries):
        resp = requests.get(
            AADF_URL,
            params={"page": page, "year": year},
            timeout=60,
        )
        if resp.status_code == 429:
            wait = 2 ** attempt
            logger.warning(f"  Rate-limited (429). Retrying in {wait}s (attempt {attempt + 1}/{max_retries})")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()
    return resp.json()


_LONDON_BOROUGHS = [
    "Barking and Dagenham", "Barnet", "Bexley", "Brent", "Bromley",
    "Camden", "City of London", "Croydon", "Ealing", "Enfield",
    "Greenwich", "Hackney", "Hammersmith and Fulham", "Haringey", "Harrow",
    "Havering", "Hillingdon", "Hounslow", "Islington",
    "Kensington and Chelsea", "Kingston upon Thames", "Lambeth", "Lewisham",
    "Merton", "Newham", "Redbridge", "Richmond upon Thames", "Southwark",
    "Sutton", "Tower Hamlets", "Waltham Forest", "Wandsworth", "Westminster",
]

# London bounding box (WGS84)
_LON_MIN, _LON_MAX = -0.51, 0.33
_LAT_MIN, _LAT_MAX = 51.28, 51.70

_ROAD_CATEGORIES = ["PA", "PB", "TM", "TA", "TB", "TC", "U"]
_ROAD_TYPES      = ["Major", "Minor"]
_EST_METHODS     = ["Counted", "Estimated"]


def _synthetic(year: int, n: int = 500) -> gpd.GeoDataFrame:
    """Return deterministic synthetic AADF count points for London."""
    rng = np.random.default_rng(seed=year)
    lons = rng.uniform(_LON_MIN, _LON_MAX, n)
    lats = rng.uniform(_LAT_MIN, _LAT_MAX, n)

    df = pd.DataFrame({
        "count_point_id":           [f"CP{year}{i:04d}" for i in range(n)],
        "year":                     year,
        "region_id":                6,
        "region_name":              "London",
        "local_authority_id":       rng.integers(1, 33, n),
        "local_authority_name":     rng.choice(_LONDON_BOROUGHS, n),
        "road_name":                [f"A{rng.integers(1, 400)}" for _ in range(n)],
        "road_category":            rng.choice(_ROAD_CATEGORIES, n),
        "road_type":                rng.choice(_ROAD_TYPES, n),
        "start_junction_road_name": "Junction A",
        "end_junction_road_name":   "Junction B",
        "easting":                  rng.integers(503000, 561000, n),
        "northing":                 rng.integers(155000, 200000, n),
        "latitude":                 lats,
        "longitude":                lons,
        "link_length_km":           rng.uniform(0.1, 5.0, n).round(3),
        "estimation_method":        rng.choice(_EST_METHODS, n),
        "estimation_method_detailed": "Synthetic",
        "pedal_cycles":             rng.integers(0, 500, n),
        "two_wheeled_motor_vehicles": rng.integers(0, 300, n),
        "cars_and_taxis":           rng.integers(500, 20000, n),
        "buses_and_coaches":        rng.integers(10, 500, n),
        "lgvs":                     rng.integers(50, 3000, n),
        "hgvs_2_rigid_axle":        rng.integers(0, 500, n),
        "hgvs_3_rigid_axle":        rng.integers(0, 200, n),
        "hgvs_4_or_more_rigid_axle": rng.integers(0, 100, n),
        "hgvs_3_or_4_articulated_axle": rng.integers(0, 150, n),
        "hgvs_5_articulated_axle":  rng.integers(0, 200, n),
        "hgvs_6_articulated_axle":  rng.integers(0, 100, n),
        "all_hgvs":                 rng.integers(0, 1000, n),
        "all_motor_vehicles":       rng.integers(600, 25000, n),
    })

    geometry = [Point(lon, lat) for lon, lat in zip(lons, lats)]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    logger.info(f"Synthetic: {n} DfT AADF count points generated for year={year}")
    return gdf


def _fetch_all_pages(year: int) -> list[dict]:
    """Paginate the DfT AADF API and return all London records for a year."""
    records, page = [], 1
    while True:
        logger.info(f"  Page {page}…")
        payload = _fetch_page(page, year)
        data = payload.get("data", [])
        if not data:
            break
        records.extend(
            r for r in data
            if str(r.get("region_name", "")).strip().lower() == "london"
        )
        last_page = payload.get("last_page") or payload.get("total_pages")
        if last_page and page >= int(last_page):
            break
        page += 1
        time.sleep(0.5)  # polite delay to avoid 429s
    return records


def fetch_london_aadf(year: int = DEFAULT_YEAR) -> gpd.GeoDataFrame:
    """
    Return London AADF count points for a given year as a GeoDataFrame (WGS84).
    Falls back to deterministic synthetic data if the API is unavailable.
    """
    logger.info(f"Fetching DfT AADF data for London, year={year}")

    try:
        records = _fetch_all_pages(year)
        if not records:
            raise ValueError(f"No London records returned for year={year}")
    except Exception as exc:
        logger.warning(f"DfT API unavailable ({exc}) — using synthetic data")
        return _synthetic(year)

    logger.info(f"DfT AADF: {len(records):,} London count points for {year}")
    df = pd.DataFrame(records)

    for col in _VEHICLE_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["year"]          = pd.to_numeric(df["year"],          errors="coerce").astype("Int64")
    df["latitude"]      = pd.to_numeric(df["latitude"],      errors="coerce")
    df["longitude"]     = pd.to_numeric(df["longitude"],     errors="coerce")
    df["easting"]       = pd.to_numeric(df.get("easting"),   errors="coerce")
    df["northing"]      = pd.to_numeric(df.get("northing"),  errors="coerce")
    df["link_length_km"] = pd.to_numeric(df.get("link_length_km"), errors="coerce")

    missing_coords = df["latitude"].isna() | df["longitude"].isna()
    if missing_coords.sum():
        logger.warning(f"QA: dropping {missing_coords.sum()} rows with missing coordinates")
        df = df[~missing_coords].copy()

    geometry = [Point(lon, lat) for lon, lat in zip(df["longitude"], df["latitude"])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    logger.info(f"DfT AADF: {len(gdf):,} valid London count points")
    return gdf


def select_columns(gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Return a plain DataFrame with the columns expected by BigQuery.
    Only vehicle count columns that exist in the response are included.
    """
    base_cols = [
        "count_point_id",
        "year",
        "region_id",
        "region_name",
        "local_authority_id",
        "local_authority_name",
        "road_name",
        "road_category",
        "road_type",
        "start_junction_road_name",
        "end_junction_road_name",
        "easting",
        "northing",
        "latitude",
        "longitude",
        "link_length_km",
        "estimation_method",
        "estimation_method_detailed",
    ]
    vehicle_cols = [c for c in _VEHICLE_COLS if c in gdf.columns]
    keep = [c for c in base_cols + vehicle_cols if c in gdf.columns]

    df = pd.DataFrame(gdf[keep].copy())
    df["geometry"] = gdf.geometry.apply(lambda g: g.wkt if g else None)
    return df


def main():
    gdf = fetch_london_aadf()
    print(f"Fetched {len(gdf)} London AADF records for {gdf['year'].iloc[0] if not gdf.empty else 'N/A'}.")
    print(gdf.head())
