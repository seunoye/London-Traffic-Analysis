-- mart_borough_network_summary.sql
-- Per-borough aggregation of road infrastructure.
-- One row per borough with road counts, lengths, and density metrics.

{{
  config(
    materialized = 'table',
    cluster_by   = ['borough'],
    description  = 'Road network summary by London borough'
  )
}}

with roads as (
    select
        borough,
        count(*)                                  as road_count,
        count(distinct road_type)                 as road_type_count,
        round(sum(length_m) / 1000.0, 3)          as total_road_length_km,
        avg(latitude)                             as latitude,
        avg(longitude)                            as longitude
    from {{ ref('stg_roads') }}
    where borough is not null
      and is_null_geom   = false
      and is_zero_length = false
    group by borough
)

select
    borough,
    road_count,
    road_type_count,
    total_road_length_km,
    latitude,
    longitude
from roads
