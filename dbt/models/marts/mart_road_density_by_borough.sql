-- models/marts/mart_road_density_by_borough.sql
-- Dashboard Tile 1: road density (km of road per km²) by London borough
-- Partitioned by loaded_date, clustered by borough + road_type

{{
  config(
    materialized = 'table',
    partition_by = {'field': 'loaded_date', 'data_type': 'date'},
    cluster_by   = ['borough', 'road_type'],
    description  = 'Road density per London borough — Looker Studio tile 1'
  )
}}

WITH road_lengths AS (
    SELECT
        borough,
        road_type,
        COUNT(*)                          AS road_count,
        ROUND(SUM(length_m) / 1000.0, 3) AS total_length_km,
        DATE(MAX(loaded_at))              AS loaded_date
    FROM {{ ref('stg_roads') }}
    WHERE borough        IS NOT NULL
      AND borough        != 'Unknown'
      AND is_null_geom   = FALSE
      AND is_zero_length = FALSE
    GROUP BY borough, road_type
),

-- Official ONS borough areas in km²
borough_areas AS (
    SELECT * FROM UNNEST([
        STRUCT('Barking And Dagenham'   AS borough, 36.1  AS area_km2),
        STRUCT('Barnet'                 AS borough, 86.7  AS area_km2),
        STRUCT('Bexley'                 AS borough, 60.6  AS area_km2),
        STRUCT('Brent'                  AS borough, 43.2  AS area_km2),
        STRUCT('Bromley'                AS borough, 150.1 AS area_km2),
        STRUCT('Camden'                 AS borough, 21.8  AS area_km2),
        STRUCT('City Of London'         AS borough, 2.9   AS area_km2),
        STRUCT('Croydon'                AS borough, 86.5  AS area_km2),
        STRUCT('Ealing'                 AS borough, 55.6  AS area_km2),
        STRUCT('Enfield'                AS borough, 81.9  AS area_km2),
        STRUCT('Greenwich'              AS borough, 47.3  AS area_km2),
        STRUCT('Hackney'                AS borough, 19.0  AS area_km2),
        STRUCT('Hammersmith And Fulham' AS borough, 16.4  AS area_km2),
        STRUCT('Haringey'               AS borough, 29.6  AS area_km2),
        STRUCT('Harrow'                 AS borough, 50.5  AS area_km2),
        STRUCT('Havering'               AS borough, 112.4 AS area_km2),
        STRUCT('Hillingdon'             AS borough, 116.4 AS area_km2),
        STRUCT('Hounslow'               AS borough, 56.0  AS area_km2),
        STRUCT('Islington'              AS borough, 14.9  AS area_km2),
        STRUCT('Kensington And Chelsea' AS borough, 12.1  AS area_km2),
        STRUCT('Kingston Upon Thames'   AS borough, 37.3  AS area_km2),
        STRUCT('Lambeth'                AS borough, 26.8  AS area_km2),
        STRUCT('Lewisham'               AS borough, 35.1  AS area_km2),
        STRUCT('Merton'                 AS borough, 37.6  AS area_km2),
        STRUCT('Newham'                 AS borough, 36.2  AS area_km2),
        STRUCT('Redbridge'              AS borough, 56.4  AS area_km2),
        STRUCT('Richmond Upon Thames'   AS borough, 57.3  AS area_km2),
        STRUCT('Southwark'              AS borough, 28.9  AS area_km2),
        STRUCT('Sutton'                 AS borough, 43.8  AS area_km2),
        STRUCT('Tower Hamlets'          AS borough, 19.8  AS area_km2),
        STRUCT('Waltham Forest'         AS borough, 38.8  AS area_km2),
        STRUCT('Wandsworth'             AS borough, 34.3  AS area_km2),
        STRUCT('Westminster'            AS borough, 21.5  AS area_km2)
    ])
),

-- Approximate borough centroids (WGS84) for Looker Studio maps
borough_centroids AS (
    SELECT * FROM UNNEST([
        STRUCT('Barking And Dagenham'   AS borough, 51.5397 AS latitude, 0.1313  AS longitude),
        STRUCT('Barnet'                 AS borough, 51.6252 AS latitude, -0.1517 AS longitude),
        STRUCT('Bexley'                 AS borough, 51.4549 AS latitude, 0.1505  AS longitude),
        STRUCT('Brent'                  AS borough, 51.5588 AS latitude, -0.2817 AS longitude),
        STRUCT('Bromley'                AS borough, 51.3668 AS latitude, 0.0549  AS longitude),
        STRUCT('Camden'                 AS borough, 51.5390 AS latitude, -0.1426 AS longitude),
        STRUCT('City Of London'         AS borough, 51.5155 AS latitude, -0.0922 AS longitude),
        STRUCT('Croydon'                AS borough, 51.3762 AS latitude, -0.0982 AS longitude),
        STRUCT('Ealing'                 AS borough, 51.5130 AS latitude, -0.3089 AS longitude),
        STRUCT('Enfield'                AS borough, 51.6538 AS latitude, -0.0799 AS longitude),
        STRUCT('Greenwich'              AS borough, 51.4892 AS latitude, 0.0648  AS longitude),
        STRUCT('Hackney'                AS borough, 51.5450 AS latitude, -0.0553 AS longitude),
        STRUCT('Hammersmith And Fulham' AS borough, 51.4927 AS latitude, -0.2339 AS longitude),
        STRUCT('Haringey'               AS borough, 51.5906 AS latitude, -0.1110 AS longitude),
        STRUCT('Harrow'                 AS borough, 51.5898 AS latitude, -0.3346 AS longitude),
        STRUCT('Havering'               AS borough, 51.5812 AS latitude, 0.2183  AS longitude),
        STRUCT('Hillingdon'             AS borough, 51.5441 AS latitude, -0.4760 AS longitude),
        STRUCT('Hounslow'               AS borough, 51.4746 AS latitude, -0.3680 AS longitude),
        STRUCT('Islington'              AS borough, 51.5465 AS latitude, -0.1058 AS longitude),
        STRUCT('Kensington And Chelsea' AS borough, 51.5020 AS latitude, -0.1947 AS longitude),
        STRUCT('Kingston Upon Thames'   AS borough, 51.3925 AS latitude, -0.3057 AS longitude),
        STRUCT('Lambeth'                AS borough, 51.4571 AS latitude, -0.1231 AS longitude),
        STRUCT('Lewisham'               AS borough, 51.4452 AS latitude, -0.0209 AS longitude),
        STRUCT('Merton'                 AS borough, 51.4098 AS latitude, -0.1831 AS longitude),
        STRUCT('Newham'                 AS borough, 51.5255 AS latitude, 0.0352  AS longitude),
        STRUCT('Redbridge'              AS borough, 51.5590 AS latitude, 0.0741  AS longitude),
        STRUCT('Richmond Upon Thames'   AS borough, 51.4613 AS latitude, -0.3037 AS longitude),
        STRUCT('Southwark'              AS borough, 51.4734 AS latitude, -0.0755 AS longitude),
        STRUCT('Sutton'                 AS borough, 51.3618 AS latitude, -0.1945 AS longitude),
        STRUCT('Tower Hamlets'          AS borough, 51.5099 AS latitude, -0.0059 AS longitude),
        STRUCT('Waltham Forest'         AS borough, 51.5886 AS latitude, -0.0118 AS longitude),
        STRUCT('Wandsworth'             AS borough, 51.4567 AS latitude, -0.1910 AS longitude),
        STRUCT('Westminster'            AS borough, 51.4975 AS latitude, -0.1357 AS longitude)
    ])
)

SELECT
    r.borough,
    r.road_type,
    r.road_count,
    r.total_length_km,
    ba.area_km2,
    ROUND(r.total_length_km / NULLIF(ba.area_km2, 0), 4) AS road_density_km_per_km2,
    r.loaded_date,
    bc.latitude,
    bc.longitude
FROM road_lengths r
LEFT JOIN borough_areas ba
       ON LOWER(r.borough) = LOWER(ba.borough)
LEFT JOIN borough_centroids bc
       ON LOWER(r.borough) = LOWER(bc.borough)
