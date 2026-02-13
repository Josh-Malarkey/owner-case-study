{% test assert_advertising_spend_matches_source(model) %}

with

source_total as (
    select
        coalesce(sum(advertising_spend), 0) as total_ad_spend
    from {{ ref('stg_expenses_advertising') }}
),

model_total as (
    select
        coalesce(sum(advertising_cost), 0) as total_ad_cost
    from {{ model }}
)

select
    s.total_ad_spend,
    m.total_ad_cost,
    abs(s.total_ad_spend - m.total_ad_cost) as difference
from source_total as s
cross join model_total as m
where abs(s.total_ad_spend - m.total_ad_cost) > 0.01

{% endtest %}
