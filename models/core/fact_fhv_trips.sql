{{ config(materialized='table') }}

with fhv_data as (
    select *, 
        'Fhv' as service_type 
    from {{ ref('stg_fhv_tripdata') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
 select     
    fhv_data.PUlocationID,
    fhv_data.DOlocationID,
    fhv_data.pickup_datetime,
    fhv_data.dropoff_datetime   
 from fhv_data
 inner join dim_zones as pickup_zone
 on fhv_data.PUlocationID = pickup_zone.locationid
 inner join dim_zones as dropoff_zone
 on fhv_data.DOlocationID = dropoff_zone.locationid
 where extract(year from pickup_datetime) = 2019

-- select 
--     fhv_data.PUlocationID,
--     fhv_data.DOlocationID
-- from fhv_tripdata
-- inner join dim_zones as pickup_zone
-- on fhv_tripdata.PUlocationID = pickup_zone.locationid
-- inner join dim_zones as dropoff_zone
-- on fhv_tripdata.DOlocationID = dropoff_zone.locationid
-- where date(pickup_datetime) >= Date(2019,01,01) and date(pickup_datetime) <= Date(2019, 12, 31)