with source as (
    select * from {{ source('ninox', 'subscriptions') }}
),

renamed as (
    select
        subscription_id,
        customer_id,
        plan_name,
        number_of_licenses,
        cast(start_date as date) as start_date,
        cast(end_date as date) as end_date
    from source
)

select * from renamed