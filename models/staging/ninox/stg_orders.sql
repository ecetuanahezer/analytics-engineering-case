with source as (
    select * from {{ source('ninox', 'orders') }}
),

renamed as (
    select
        order_id,
        subscription_id,
        cast(order_date as date) as order_date,
        gross_amount,
        safe_cast(json_extract_scalar(checkout_metadata, '$.currency') as string) as currency,
        safe_cast(json_extract_scalar(checkout_metadata, '$.exchange_rate') as float64) as exchange_rate,
        safe_cast(json_extract_scalar(checkout_metadata, '$.tax_percentage') as float64) as tax_percentage
    from source
),

calculated as (
    select
        *,
        -- Net Revenue (EUR): (Gross / (1 + tax)) / exchange_rate
        round(
            (gross_amount / (1 + (tax_percentage / 100))) / exchange_rate, 
            2
        ) as net_revenue_eur
    from renamed
)

select * from calculated