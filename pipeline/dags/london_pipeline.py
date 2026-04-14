"""
DAG: london_pipeline
Orchestrates the full London Network Analysis ingestion pipeline.

  ingest_os_roads  ──┐
                     ├─► (parallel) ──► dbt_run
  ingest_dft_traffic─┘

Each ingest task loads directly to BigQuery (no PostGIS or GCS intermediate).
dbt_run then builds staging views and mart tables.

Required Airflow Variables / env vars on the worker:
    GOOGLE_APPLICATION_CREDENTIALS  path to GCP service-account key JSON
    OS_API_KEY                       OS Data Hub API key
    GCP_PROJECT_ID                   GCP project (default: london-analytics)
    BQ_DATASET                       BigQuery dataset (default: london_analytics_dataset)
    DFT_YEAR                         AADF year to load (default: 2023)
"""

import os, sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

sys.path.insert(0, "/opt/airflow")

import pandas as pd
from google.cloud import bigquery
from loguru import logger

import ingestion_os_roads as roads_mod
import ingest_dft_traffic as traffic_mod


# ── shared helpers ────────────────────────────────────────────────────────────

def _bq_client() -> bigquery.Client:
    project = os.environ.get("GCP_PROJECT_ID", "london-analytics")
    return bigquery.Client(project=project)


def _load_to_bq(df: pd.DataFrame, table_id: str, bq: bigquery.Client) -> None:
    job_cfg = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    bq.load_table_from_dataframe(df, table_id, job_config=job_cfg).result()
    n = bq.get_table(table_id).num_rows
    logger.info(f"  → {table_id}: {n:,} rows")


def _gdf_to_wkt_df(gdf, required_cols: list) -> pd.DataFrame:
    for col in required_cols:
        if col not in gdf.columns:
            gdf = gdf.copy()
            gdf[col] = None
    df = pd.DataFrame(gdf[required_cols].copy())
    if "geometry" in df.columns:
        df["geometry"] = gdf.geometry.apply(lambda g: g.wkt if g else None)
    return df


# ── task callables ────────────────────────────────────────────────────────────

def ingest_os_roads():
    project = os.environ.get("GCP_PROJECT_ID", "london-analytics")
    dataset = os.environ.get("BQ_DATASET", "london_analytics_dataset")
    bq = _bq_client()

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


def ingest_dft_traffic():
    project = os.environ.get("GCP_PROJECT_ID", "london-analytics")
    dataset = os.environ.get("BQ_DATASET", "london_analytics_dataset")
    bq = _bq_client()

    gdf = traffic_mod.fetch_london_aadf()
    df  = traffic_mod.select_columns(gdf)
    df["loaded_at"] = pd.Timestamp.utcnow()
    _load_to_bq(df, f"{project}.{dataset}.traffic_counts_clean", bq)
    return len(df)


# ── DAG ───────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="london_pipeline",
    default_args=default_args,
    description="Ingest OS roads, DfT traffic → BigQuery → dbt",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["london", "ingestion"],
) as dag:

    t_roads = PythonOperator(
        task_id="ingest_os_roads",
        python_callable=ingest_os_roads,
        execution_timeout=timedelta(minutes=15),
    )

    t_traffic = PythonOperator(
        task_id="ingest_dft_traffic",
        python_callable=ingest_dft_traffic,
        execution_timeout=timedelta(minutes=10),
    )

    t_dbt = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir .",
    )

    [t_roads, t_traffic] >> t_dbt
