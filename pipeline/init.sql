-- init.sql
-- Runs automatically when the postgres container first starts.
-- Enables PostGIS extensions required by Airflow's geospatial dependencies.

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
