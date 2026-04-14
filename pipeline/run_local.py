"""
pipeline/run_local.py
Run the full pipeline locally — no Docker, Airflow, or PostGIS required.

Loads two datasets directly into BigQuery:
  - OS Open Roads        → roads_clean
  - DfT AADF traffic    → traffic_counts_clean

Required env vars:
    GOOGLE_APPLICATION_CREDENTIALS  path to GCP service-account key JSON
    OS_API_KEY                       OS Data Hub API key
    GCP_PROJECT_ID                   GCP project (default: london-analytics)
    BQ_DATASET                       BigQuery dataset (default: london_analytics_dataset)
    DFT_YEAR                         AADF year to load (default: 2023)

Usage (from repo root):
    GOOGLE_APPLICATION_CREDENTIALS=terraform/keys/la_creds.json \\
    OS_API_KEY=... \\
    GCP_PROJECT_ID=london-analytics \\
    BQ_DATASET=london_analytics_dataset \\
    uv run pipeline/run_local.py
"""

import os, sys
from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent))

import ingestion_os_roads as roads_mod
import ingest_dft_traffic as traffic_mod


def _bq_client() -> bigquery.Client:
    project = os.environ.get("GCP_PROJECT_ID", "london-analytics")
    creds_path = os.environ.get(
        "GOOGLE_APPLICATION_CREDENTIALS",
        str(Path(__file__).parent.parent / "terraform" / "keys" / "la_creds.json"),
    )
    os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", creds_path)
    return bigquery.Client(project=project)


def _load_to_bq(df: pd.DataFrame, table_id: str, bq: bigquery.Client) -> None:
    job_cfg = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    job = bq.load_table_from_dataframe(df, table_id, job_config=job_cfg)
    job.result()
    n = bq.get_table(table_id).num_rows
    logger.info(f"  → {table_id}: {n:,} rows")


def _gdf_to_wkt_df(gdf, required_cols: list) -> pd.DataFrame:
    """Convert a GeoDataFrame to a plain DataFrame with geometry as WKT."""
    for col in required_cols:
        if col not in gdf.columns:
            gdf = gdf.copy()
            gdf[col] = None
    df = pd.DataFrame(gdf[required_cols].copy())
    if "geometry" in df.columns:
        df["geometry"] = gdf.geometry.apply(lambda g: g.wkt if g else None)
    return df


def run_roads(bq: bigquery.Client, dataset: str, project: str) -> int:
    logger.info("=== OS Roads ===")
    gdf = roads_mod.load_os_roads()
    gdf = roads_mod.assign_boroughs(gdf)
    gdf = roads_mod.spatial_qa(gdf)
    gdf = gdf.copy()
    gdf["loaded_at"] = pd.Timestamp.utcnow()

    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")

    required = ["road_id", "road_name", "road_type", "borough",
                "length_m", "geometry", "crs_source", "loaded_at"]
    df = _gdf_to_wkt_df(gdf, required)
    _load_to_bq(df, f"{project}.{dataset}.roads_clean", bq)
    return len(df)


def run_traffic(bq: bigquery.Client, dataset: str, project: str) -> int:
    logger.info("=== DfT AADF Traffic Counts ===")
    gdf = traffic_mod.fetch_london_aadf()
    df = traffic_mod.select_columns(gdf)
    df["loaded_at"] = pd.Timestamp.utcnow()
    _load_to_bq(df, f"{project}.{dataset}.traffic_counts_clean", bq)
    return len(df)


def main():
    project = os.environ.get("GCP_PROJECT_ID", "london-analytics")
    dataset = os.environ.get("BQ_DATASET", "london_analytics_dataset")
    logger.info(f"Target: {project}.{dataset}")

    bq = _bq_client()

    roads_n   = run_roads(bq, dataset, project)
    traffic_n = run_traffic(bq, dataset, project)

    logger.info(
        f"Done — {roads_n:,} roads | {traffic_n:,} traffic count points"
    )
    logger.info("Run 'dbt run' from the dbt/ directory to build the models.")


if __name__ == "__main__":
    main()
