with monthly_mrr as (
    select * from {{ ref('int_subs_monthly') }}
),

-- find cohort month of each customer
customer_cohorts as (
    select
        customer_id,
        min(mrr_month) as cohort_month
    from monthly_mrr
    group by 1
),

-- calculate month numbers using cohorts
retention_base as (
    select
        m.customer_id,
        c.cohort_month,
        m.mrr_month,
        date_diff(m.mrr_month, c.cohort_month, month) as month_number,
        m.mrr_amount
    from monthly_mrr m
    join customer_cohorts c on m.customer_id = c.customer_id
),

-- find month 0 values
cohort_start_values as (
    select
        cohort_month,
        sum(mrr_amount) as starting_mrr
    from retention_base
    where month_number = 0
    group by 1
)

select
    rb.cohort_month,
    rb.month_number,
    sum(rb.mrr_amount) as retained_mrr,
    csv.starting_mrr,
    (select count(distinct customer_id) from customer_cohorts where cohort_month = rb.cohort_month) as cohort_initial_customer_count,
    -- Retention =  MRR / Start MRR
    round(safe_divide(sum(rb.mrr_amount), csv.starting_mrr), 4) as retention_percentage
from retention_base rb
join cohort_start_values csv on rb.cohort_month = csv.cohort_month
group by 1, 2, csv.starting_mrr, rb.cohort_month
order by 1, 2