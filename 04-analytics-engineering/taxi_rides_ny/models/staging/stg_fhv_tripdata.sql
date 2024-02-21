{{
    config(
        materialized='view'
    )
}}

with tripdata as (

    select * from {{ source('staging', 'fhv_tripdata_non_partitioned') }}
    where extract(year from pickup_datetime) = 2019

)

select

    -- identifiers
    dispatching_base_num as dispatching_base_num,
    affiliated_base_number as affiliated_base_number,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    {{ dbt.safe_cast("SR_Flag", api.Column.translate_type("integer")) }} as sr_flag

from tripdata


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}