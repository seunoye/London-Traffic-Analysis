-- stg_roads.sql
-- Light transform: parse WKT geometry → BigQuery GEOGRAPHY, rename columns.

with deduplicated as (
    select
        *,
        row_number() over (partition by road_id order by loaded_at desc) as _rn
    from {{ source('raw', 'roads_clean') }}
    where geometry is not null
)

select
    road_id,
    road_name,
    road_type,
    borough,
    length_m,
    crs_source,
    safe.st_geogfromtext(geometry) as geog,
    st_y(st_centroid(safe.st_geogfromtext(geometry))) as latitude,
    st_x(st_centroid(safe.st_geogfromtext(geometry))) as longitude,
    geometry is null as is_null_geom,
    coalesce(length_m, 0) = 0      as is_zero_length,
    loaded_at
from deduplicated
where _rn = 1
