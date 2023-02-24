{{ config(materialized='view') }}

select * from {{ source('staging', 'fhv_tripdata') }}
where extract(year from pickup_datetime) = 2019