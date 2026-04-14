"""
pipeline/ingestion_os_roads.py
Download OS Open Roads data → spatial QA.

Requires:
    OS_API_KEY  — key from https://osdatahub.os.uk/
"""

import os, time, zipfile
from pathlib import Path

import geopandas as gpd
import requests
from shapely.geometry import LineString, MultiLineString
from shapely.ops import linemerge
from shapely.validation import make_valid
from loguru import logger

OS_API_KEY = os.environ.get("OS_API_KEY")

# London bounding box in EPSG:27700 (British National Grid)
LONDON_BBOX_BNG = (503000, 155000, 561000, 200000)

LONDON_BOROUGHS = [
    "Barking and Dagenham", "Barnet", "Bexley", "Brent", "Bromley",
    "Camden", "City of London", "Croydon", "Ealing", "Enfield",
    "Greenwich", "Hackney", "Hammersmith and Fulham", "Haringey", "Harrow",
    "Havering", "Hillingdon", "Hounslow", "Islington",
    "Kensington and Chelsea", "Kingston upon Thames", "Lambeth", "Lewisham",
    "Merton", "Newham", "Redbridge", "Richmond upon Thames", "Southwark",
    "Sutton", "Tower Hamlets", "Waltham Forest", "Wandsworth", "Westminster",
]

DATA_DIR = Path(__file__).parent / "data"


def _download_and_extract(product: str, fmt: str) -> Path:
    """Download an OS product zip and extract; returns path to extracted dir."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    extract_dir = DATA_DIR / product
    if extract_dir.exists() and any(extract_dir.rglob("*.gpkg")):
        logger.info(f"Using cached {product} data in {extract_dir}")
        return extract_dir

    if not OS_API_KEY:
        raise EnvironmentError(
            "OS_API_KEY is not set. "
            "Obtain a key from https://osdatahub.os.uk/ and export it."
        )

    url = (
        f"https://api.os.uk/downloads/v1/products/{product}/downloads"
        f"?area=GB&format={fmt}&redirect&key={OS_API_KEY}"
    )

    zip_path = DATA_DIR / f"{product}.zip"
    _download_with_retry(url, zip_path)

    logger.info("Extracting…")
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(extract_dir)
    zip_path.unlink()
    logger.info(f"Extracted to {extract_dir}")
    return extract_dir


def _download_with_retry(url: str, dest: Path, max_attempts: int = 3) -> None:
    for attempt in range(1, max_attempts + 1):
        try:
            logger.info(f"Downloading (attempt {attempt}/{max_attempts}): {dest.name}")
            resp = requests.get(url, stream=True, timeout=600)
            resp.raise_for_status()
            total = 0
            with open(dest, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                    f.write(chunk)
                    total += len(chunk)
                    if total % (50 * 1024 * 1024) < 8 * 1024 * 1024:
                        logger.info(f"  … {total / 1e6:.0f} MB downloaded")
            logger.info(f"Downloaded {total / 1e6:.1f} MB")
            return
        except (requests.ConnectionError, requests.Timeout) as exc:
            if attempt == max_attempts:
                raise
            wait = 2 ** attempt
            logger.warning(f"Download error ({exc}); retrying in {wait}s…")
            time.sleep(wait)


def _find_gpkg(directory: Path, pattern: str = "*.gpkg") -> Path:
    matches = list(directory.rglob(pattern))
    if not matches:
        raise FileNotFoundError(f"No {pattern} found in {directory}")
    if len(matches) > 1:
        logger.warning(
            f"Multiple {pattern} files found; using {matches[0].name}. "
            f"Others: {[m.name for m in matches[1:]]}"
        )
    return matches[0]


def load_os_roads() -> gpd.GeoDataFrame:
    """Download OS Open Roads and filter to London."""
    roads_dir = _download_and_extract("OpenRoads", "GeoPackage")
    gpkg_path = _find_gpkg(roads_dir)

    # Cache as GeoParquet to avoid the SQLite/R-tree issues with GeoPackage writing
    london_parquet = DATA_DIR / "london_roads.parquet"
    if london_parquet.exists():
        logger.info(f"Using cached London roads: {london_parquet}")
        gdf = gpd.read_parquet(london_parquet)
    else:
        logger.info(f"Extracting London roads from {gpkg_path.name} with bbox filter…")
        # bbox is in EPSG:27700, matching the source file CRS — GDAL pushes this down efficiently
        gdf = gpd.read_file(gpkg_path, layer="road_link", bbox=LONDON_BBOX_BNG)
        gdf.to_parquet(london_parquet, index=False)
        logger.info(f"London subset cached to {london_parquet}")

    logger.info(f"Read {len(gdf):,} road links within London bbox")

    col_map = {
        "id": "road_id",
        "name_1": "road_name",
        "road_classification": "road_type",
        "length": "length_m",
    }
    gdf = gdf.rename(columns=col_map)

    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:27700")
    elif gdf.crs.to_epsg() != 27700:
        gdf = gdf.to_crs("EPSG:27700")

    gdf["crs_source"] = "os_open_roads"
    return gdf


def assign_boroughs(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Assign London borough names via spatial join with Boundary-Line."""
    bl_dir = _download_and_extract("BoundaryLine", "GeoPackage")
    bl_gpkg = _find_gpkg(bl_dir)
    logger.info(f"Reading borough boundaries from {bl_gpkg.name}…")

    boroughs = gpd.read_file(bl_gpkg, layer="boundary_line_ceremonial_counties")
    if boroughs.crs and boroughs.crs.to_epsg() != 27700:
        boroughs = boroughs.to_crs("EPSG:27700")

    london_box = gpd.GeoSeries.from_wkt(
        ["POLYGON((503000 155000,561000 155000,561000 200000,503000 200000,503000 155000))"],
        crs="EPSG:27700",
    ).union_all()
    london_boroughs = boroughs[boroughs.geometry.intersects(london_box)].copy()

    if london_boroughs.empty:
        for layer_name in ["district_borough_unitary", "district_borough_unitary_region"]:
            try:
                boroughs = gpd.read_file(bl_gpkg, layer=layer_name)
                if boroughs.crs and boroughs.crs.to_epsg() != 27700:
                    boroughs = boroughs.to_crs("EPSG:27700")
                london_boroughs = boroughs[
                    boroughs["Name"].str.title().isin(
                        [b.title() for b in LONDON_BOROUGHS]
                    )
                ].copy()
                if not london_boroughs.empty:
                    break
            except Exception:
                continue

    if london_boroughs.empty:
        raise RuntimeError(
            "No London borough boundaries found in Boundary-Line data. "
            "Check the layer names in the downloaded GeoPackage."
        )

    name_col = "Name" if "Name" in london_boroughs.columns else london_boroughs.columns[0]
    london_boroughs = london_boroughs[[name_col, "geometry"]].rename(
        columns={name_col: "borough"}
    )
    london_boroughs["borough"] = london_boroughs["borough"].str.title()

    gdf = gpd.sjoin(gdf, london_boroughs, how="left", predicate="within")
    gdf = gdf.drop(columns=["index_right"], errors="ignore")
    matched = gdf["borough"].notna().sum()
    logger.info(f"Borough assignment: {matched:,}/{len(gdf):,} matched")
    return gdf


def _coerce_linestring(geom):
    if geom is None:
        return None
    if isinstance(geom, LineString):
        return geom
    if isinstance(geom, MultiLineString):
        merged = linemerge(geom)
        if isinstance(merged, LineString):
            return merged
        return max(geom.geoms, key=lambda g: g.length)
    return None


def spatial_qa(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    n0 = len(gdf)

    null_mask = gdf.geometry.isna()
    if null_mask.sum():
        logger.warning(f"QA: removing {null_mask.sum()} null geometries")
        gdf = gdf[~null_mask].copy()

    bad_mask = ~gdf.geometry.is_valid
    if bad_mask.sum():
        logger.warning(f"QA: repairing {bad_mask.sum()} invalid geometries")
        repaired = gdf.geometry.copy()
        repaired[bad_mask] = [make_valid(g) for g in gdf.geometry[bad_mask]]
        gdf = gdf.copy()
        gdf["geometry"] = repaired
        gdf = gdf.set_geometry("geometry")

        gdf["geometry"] = [_coerce_linestring(g) for g in gdf.geometry]
        non_line = gdf.geometry.isna()
        if non_line.sum():
            logger.warning(
                f"QA: dropping {non_line.sum()} geometries that could not be coerced to LineString"
            )
            gdf = gdf[~non_line].copy()

    if "length_m" not in gdf.columns or gdf["length_m"].isna().all():
        gdf = gdf.copy()
        gdf["length_m"] = gdf.to_crs("EPSG:27700").geometry.length

    logger.info(f"Roads QA: {n0:,} → {len(gdf):,} valid features")
    return gdf


def main():
    gdf = load_os_roads()
    print(f"Fetched {len(gdf)} OS Open Roads records.")
    print(gdf.head())
