with mrr_movements as (
    select * from {{ ref('int_mrr_movements') }}
)

select
    mrr_month,
    sum(start_of_period_mrr) as total_start_mrr,
    sum(new_mrr) as total_new_mrr,
    sum(expansion_mrr) as total_expansion_mrr,
    sum(contraction_mrr) as total_contraction_mrr,
    sum(lost_mrr) as total_lost_mrr,
    sum(end_of_period_mrr) as total_end_mrr,
    count(distinct customer_id) as total_customers
from mrr_movements
group by 1
order by 1