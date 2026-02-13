/*
    Fact table: estimated lifetime value per account.
    Aggregates all closed-won opportunities for an account to compute total estimated LTV from subscription and transaction revenue.

    Grain: one row per account_id.
*/

with

funnel as (
    select * from {{ ref('fct_lead_funnel') }}
    where is_closed_won
),

final as (

    select
        account_id,
        channel,

        -- deal counts
        count(*) as total_won_deals,

        -- revenue components (per-deal averages)
        avg(predicted_monthly_sales) as avg_predicted_monthly_sales,
        avg(predicted_monthly_transaction_revenue) as avg_monthly_transaction_revenue,
        avg(monthly_subscription_revenue) as avg_monthly_subscription_revenue,
        avg(predicted_monthly_total_revenue) as avg_monthly_total_revenue,

        -- revenue components (account totals)
        sum(predicted_monthly_total_revenue) as total_monthly_revenue,

        -- LTV (24-month estimate)
        sum(estimated_ltv_24mo) as total_estimated_ltv_24mo,

        -- account profile
        max(location_count) as location_count,
        max(cuisine_types) as cuisine_types,
        max(length(cuisine_types)) as cuisine_count,
        max(marketplaces_used) as marketplaces_used,
        max(online_ordering_used) as online_ordering_used,

        -- timeline
        min(lead_created_date) as first_lead_created_date,
        min(close_date) as first_close_date,
        max(close_date) as latest_close_date,

        -- acquisition velocity
        avg(days_lead_to_close) as avg_days_lead_to_close,
        avg(days_demo_held_to_close) as avg_days_demo_held_to_close

    from funnel
    -- assumptions: exclude leads without a created_at date and without an account_id
    where 1=1
        and lead_created_date is not null
        and account_id is not null
    group by 1, 2

)

select * from final
