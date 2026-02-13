/*
    Fact table: unit economics combining CAC and LTV.
    Joins channel-level CAC from fct_channel_economics with account-level LTV from fct_account_ltv to compute CAC:LTV ratios and payback periods.
    
    Grain: one row per month per channel.
*/

with

cac as (
    select * from {{ ref('fct_channel_economics') }}
),

-- aggregate account-level LTV to month x channel for joining
ltv_by_channel as (
    select
        date_trunc('month', first_close_date) as close_month,
        channel,
        count(*) as accounts_won,
        sum(total_estimated_ltv_24mo) as total_ltv_24mo,
        avg(total_estimated_ltv_24mo) as avg_ltv_per_account,
        avg(avg_monthly_total_revenue) as avg_monthly_revenue_per_account
    from {{ ref('fct_account_ltv') }}
    group by 1, 2
),

final as (
    select
        c.lead_created_month,
        c.channel,

        -- from fct_channel_economics
        c.total_leads,
        c.closed_won_deals,
        c.total_cost,
        c.cac,
        c.lead_to_close_rate,

        -- from fct_account_ltv (aggregated)
        coalesce(l.accounts_won, 0) as accounts_won,
        l.total_ltv_24mo,
        l.avg_ltv_per_account,
        l.avg_monthly_revenue_per_account,

        -- CAC:LTV ratio (higher is better; >3 is strong)
        case
            when c.cac is not null
                and c.cac > 0
                and l.avg_ltv_per_account is not null
            then l.avg_ltv_per_account / c.cac
            else null
        end as ltv_to_cac_ratio,

        -- payback period (months to recover CAC from monthly revenue)
        case
            when c.cac is not null
                and c.cac > 0
                and l.avg_monthly_revenue_per_account > 0
            then c.cac / l.avg_monthly_revenue_per_account
            else null
        end as payback_period_months

    from cac as c
    left join ltv_by_channel as l
        on c.lead_created_month = l.close_month
        and c.channel = l.channel
        
    -- assumptions: 
    -- 1) exclude leads without a created_at date b/c revenue and costs cannot be attributed
    -- 2) exclude months without costs
    where 1=1
        and lead_created_month is not null
        and coalesce(total_cost, 0) > 0
)

select * from final
