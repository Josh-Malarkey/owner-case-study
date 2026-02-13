/*
    Fact table: loss reason categorization for closed-lost opportunities.
    Supports drop-off analysis and identifies addressable loss reasons.

    Grain: one row per lost_reason per channel per month.
*/

with

funnel as (
    select * from {{ ref('fct_lead_funnel') }}
),

final as (
    select
        lead_created_month,
        channel,
        coalesce(lost_reason, 'Unknown / Not Specified') as lost_reason,
        count(*) as lost_count,
        
        -- what % of total losses does this reason represent?
        count(*) / nullif(
            sum(count(*)) over (
                partition by lead_created_month, channel
            ), 0
        ) as pct_of_channel_losses,
        
        -- average activities before loss (was it effort-starved?)
        avg(total_activity_count) as avg_activities_before_loss,
        avg(days_lead_to_close) as avg_days_to_loss,
        avg(predicted_monthly_sales) as avg_predicted_sales_lost
        
    from funnel
    where 1=1
        -- assumption: exclude leads without a created_at month
        and lead_created_month is not null
        and is_closed_lost
    group by 1, 2, 3
)

select * from final
