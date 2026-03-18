with source as (
    select * from {{ source('ninox', 'users') }}
),

renamed as (
    select
        user_id,
        first_name,
        last_name,
        email,
        country,
        cast(signup_date as date) as signup_date
    from source
)

select * from renamed