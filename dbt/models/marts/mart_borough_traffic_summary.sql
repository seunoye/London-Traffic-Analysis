-- mart_borough_traffic_summary.sql
-- Per-borough aggregation of DfT AADF traffic counts.
-- One row per borough showing total daily vehicle flows across all count points.

{{
  config(
    materialized = 'table',
    cluster_by   = ['borough'],
    description  = 'Daily traffic flow totals by London borough — Looker Studio tile'
  )
}}

with counts as (
    select
        borough,
        year,
        count(*)                                    as count_points,
        sum(all_motor_vehicles)                     as total_motor_vehicles,
        sum(cars_and_taxis)                         as total_cars_and_taxis,
        sum(pedal_cycles)                           as total_pedal_cycles,
        sum(buses_and_coaches)                      as total_buses_and_coaches,
        sum(lgvs)                                   as total_lgvs,
        sum(all_hgvs)                               as total_hgvs,
        sum(two_wheeled_motor_vehicles)             as total_two_wheeled_motor_vehicles,
        -- average daily flow per count point (useful for density comparison)
        round(avg(all_motor_vehicles), 1)           as avg_motor_vehicles_per_point,
        round(avg(pedal_cycles), 1)                 as avg_pedal_cycles_per_point
    from {{ ref('stg_traffic_counts') }}
    where borough is not null
    group by borough, year
),

-- Approximate borough centroids (WGS84) for Looker Studio maps
borough_centroids as (
    select * from unnest([
        struct('Barking And Dagenham'   as borough, 51.5397 as latitude, 0.1313  as longitude),
        struct('Barnet'                 as borough, 51.6252 as latitude, -0.1517 as longitude),
        struct('Bexley'                 as borough, 51.4549 as latitude, 0.1505  as longitude),
        struct('Brent'                  as borough, 51.5588 as latitude, -0.2817 as longitude),
        struct('Bromley'                as borough, 51.3668 as latitude, 0.0549  as longitude),
        struct('Camden'                 as borough, 51.5390 as latitude, -0.1426 as longitude),
        struct('City Of London'         as borough, 51.5155 as latitude, -0.0922 as longitude),
        struct('Croydon'                as borough, 51.3762 as latitude, -0.0982 as longitude),
        struct('Ealing'                 as borough, 51.5130 as latitude, -0.3089 as longitude),
        struct('Enfield'                as borough, 51.6538 as latitude, -0.0799 as longitude),
        struct('Greenwich'              as borough, 51.4892 as latitude, 0.0648  as longitude),
        struct('Hackney'                as borough, 51.5450 as latitude, -0.0553 as longitude),
        struct('Hammersmith And Fulham' as borough, 51.4927 as latitude, -0.2339 as longitude),
        struct('Haringey'               as borough, 51.5906 as latitude, -0.1110 as longitude),
        struct('Harrow'                 as borough, 51.5898 as latitude, -0.3346 as longitude),
        struct('Havering'               as borough, 51.5812 as latitude, 0.2183  as longitude),
        struct('Hillingdon'             as borough, 51.5441 as latitude, -0.4760 as longitude),
        struct('Hounslow'               as borough, 51.4746 as latitude, -0.3680 as longitude),
        struct('Islington'              as borough, 51.5465 as latitude, -0.1058 as longitude),
        struct('Kensington And Chelsea' as borough, 51.5020 as latitude, -0.1947 as longitude),
        struct('Kingston Upon Thames'   as borough, 51.3925 as latitude, -0.3057 as longitude),
        struct('Lambeth'                as borough, 51.4571 as latitude, -0.1231 as longitude),
        struct('Lewisham'               as borough, 51.4452 as latitude, -0.0209 as longitude),
        struct('Merton'                 as borough, 51.4098 as latitude, -0.1831 as longitude),
        struct('Newham'                 as borough, 51.5255 as latitude, 0.0352  as longitude),
        struct('Redbridge'              as borough, 51.5590 as latitude, 0.0741  as longitude),
        struct('Richmond Upon Thames'   as borough, 51.4613 as latitude, -0.3037 as longitude),
        struct('Southwark'              as borough, 51.4734 as latitude, -0.0755 as longitude),
        struct('Sutton'                 as borough, 51.3618 as latitude, -0.1945 as longitude),
        struct('Tower Hamlets'          as borough, 51.5099 as latitude, -0.0059 as longitude),
        struct('Waltham Forest'         as borough, 51.5886 as latitude, -0.0118 as longitude),
        struct('Wandsworth'             as borough, 51.4567 as latitude, -0.1910 as longitude),
        struct('Westminster'            as borough, 51.4975 as latitude, -0.1357 as longitude)
    ])
)

select
    c.borough,
    c.year,
    c.count_points,
    c.total_motor_vehicles,
    c.total_cars_and_taxis,
    c.total_pedal_cycles,
    c.total_buses_and_coaches,
    c.total_lgvs,
    c.total_hgvs,
    c.total_two_wheeled_motor_vehicles,
    c.avg_motor_vehicles_per_point,
    c.avg_pedal_cycles_per_point,
    -- cycle share: pedal cycles as % of all traffic at count points
    round(
        safe_divide(c.total_pedal_cycles, c.total_pedal_cycles + c.total_motor_vehicles) * 100,
        2
    ) as cycle_share_pct,
    bc.latitude,
    bc.longitude
from counts c
left join borough_centroids bc
       on lower(c.borough) = lower(bc.borough)
