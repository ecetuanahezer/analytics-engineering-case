with subscriptions as (
    select * from {{ ref('stg_subscriptions') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

subscription_months as (
    select
        s.subscription_id,
        s.customer_id,
        month_offset
    from subscriptions s
    cross join unnest(generate_array(0, 11)) as month_offset
),

monthly_details as (
    select
        sm.subscription_id,
        sm.customer_id,
        date_add(date_trunc(s.start_date, month), interval sm.month_offset month) as mrr_month,
        o.net_revenue_eur / 12 as mrr_amount
    from subscription_months sm
    join subscriptions s on sm.subscription_id = s.subscription_id
    join orders o on sm.subscription_id = o.subscription_id
)

select * from monthly_details