-- stg_traffic_counts.sql
-- Parse WKT point geometry → BigQuery GEOGRAPHY, cast types, deduplicate.

with deduplicated as (
    select
        *,
        row_number() over (
            partition by count_point_id, year
            order by loaded_at desc
        ) as _rn
    from {{ source('raw', 'traffic_counts_clean') }}
    where latitude  is not null
      and longitude is not null
),

parsed as (
    select
        cast(count_point_id       as string)  as count_point_id,
        cast(year                 as integer) as year,
        cast(region_id            as integer) as region_id,
        region_name,
        cast(local_authority_id   as integer) as local_authority_id,
        local_authority_name                  as borough,
        road_name,
        road_category,
        road_type,
        start_junction_road_name,
        end_junction_road_name,
        cast(easting              as integer) as easting,
        cast(northing             as integer) as northing,
        cast(latitude             as float64) as latitude,
        cast(longitude            as float64) as longitude,
        cast(link_length_km       as float64) as link_length_km,
        estimation_method,
        estimation_method_detailed,

        -- vehicle counts
        cast(pedal_cycles                    as integer) as pedal_cycles,
        cast(two_wheeled_motor_vehicles      as integer) as two_wheeled_motor_vehicles,
        cast(cars_and_taxis                  as integer) as cars_and_taxis,
        cast(buses_and_coaches               as integer) as buses_and_coaches,
        cast(lgvs                            as integer) as lgvs,
        cast(hgvs_2_rigid_axle               as integer) as hgvs_2_rigid_axle,
        cast(hgvs_3_rigid_axle               as integer) as hgvs_3_rigid_axle,
        cast(hgvs_4_or_more_rigid_axle       as integer) as hgvs_4_or_more_rigid_axle,
        cast(hgvs_3_or_4_articulated_axle    as integer) as hgvs_3_or_4_articulated_axle,
        cast(hgvs_5_articulated_axle         as integer) as hgvs_5_articulated_axle,
        cast(hgvs_6_articulated_axle         as integer) as hgvs_6_articulated_axle,
        cast(all_hgvs                        as integer) as all_hgvs,
        cast(all_motor_vehicles              as integer) as all_motor_vehicles,

        safe.st_geogfromtext(geometry) as geog,
        loaded_at
    from deduplicated
    where _rn = 1
)

select * from parsed
