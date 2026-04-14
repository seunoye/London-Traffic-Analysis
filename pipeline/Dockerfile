# Python 3.12 matches your Codespace version exactly
FROM apache/airflow:2.10.4-python3.12

USER root

# Install GDAL + GEOS + PROJ — required by geopandas / shapely
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    libgdal-dev \
    gdal-bin \
    libgeos-dev \
    libproj-dev \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /tmp/requirements.txt

COPY ingest_dft_traffic.py /opt/airflow/ingest_dft_traffic.py
COPY ingestion_os_roads.py /opt/airflow/ingestion_os_roads.py
