with monthly_mrr as (
    select * from {{ ref('int_subs_monthly') }}
),

joined_mrr as (
    select
        customer_id,
        mrr_month,
        mrr_amount as current_mrr,
        lag(mrr_amount) over (partition by customer_id order by mrr_month) as previous_mrr,
        lead(mrr_amount) over (partition by customer_id order by mrr_month) as next_mrr
    from monthly_mrr
),

movements as (
    select
        customer_id,
        mrr_month,
        coalesce(previous_mrr, 0) as start_of_period_mrr,
        coalesce(current_mrr, 0) as end_of_period_mrr,
        -- new mrr
        case 
            when coalesce(previous_mrr, 0) = 0 and current_mrr > 0 then current_mrr 
            else 0 
        end as new_mrr,

        -- Expansion mrr
        case 
            when current_mrr > coalesce(previous_mrr, 0) and coalesce(previous_mrr, 0) > 0 then current_mrr - previous_mrr 
            else 0 
        end as expansion_mrr,

        -- Contraction
        case 
            when current_mrr < previous_mrr and current_mrr > 0 then current_mrr - previous_mrr 
            else 0 
        end as contraction_mrr,

        -- lost mrr
        case 
            when current_mrr > 0 and next_mrr is null then -current_mrr 
            else 0 
        end as lost_mrr
    from joined_mrr
)

select * from movements