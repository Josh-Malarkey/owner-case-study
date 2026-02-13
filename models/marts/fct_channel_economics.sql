/*
    Fact table: monthly channel acquisition costs and funnel performance.
    Allocates monthly costs across closed-won deals to compute per-deal CAC.
    LTV and CAC:LTV ratios live in fct_unit_economics.

    Grain: one row per month per channel.
*/

with

funnel as (
    select * from {{ ref('fct_lead_funnel') }}
),

expenses as (
    select * from {{ ref('int_monthly_expenses') }}
),

-- aggregate lead/opp metrics by month and channel
monthly_outcomes as (

    select
        lead_created_month,
        channel,

        count(*) as total_leads,
        count_if(is_converted) as converted_leads,
        count_if(demo_held) as demos_held,
        count_if(is_closed_won) as closed_won_deals,
        count_if(is_closed_lost) as closed_lost_deals,

        -- velocity
        avg(days_lead_to_first_contact) as avg_days_to_first_contact,
        avg(days_contact_to_meeting) as avg_days_contact_to_meeting,
        avg(days_meeting_booked_to_demo_set) as avg_days_meeting_to_demo_set,
        avg(days_demo_set_to_held) as avg_days_demo_set_to_held,
        avg(days_demo_held_to_close) as avg_days_demo_held_to_close,
        avg(days_lead_to_opportunity) as avg_days_lead_to_opportunity,
        avg(days_to_close) as avg_days_opp_to_close,
        avg(days_lead_to_close) as avg_days_lead_to_close,
        avg(days_last_activity_to_close) as avg_days_last_activity_to_close,

        -- activity efficiency
        avg(total_activity_count) as avg_activities_per_lead,
        avg(
            case when is_closed_won then total_activity_count end
        ) as avg_activities_per_won_deal

    from funnel
    group by 1, 2

),

-- join expenses to outcomes
final as (
    select
        o.lead_created_month,
        o.channel,

        -- volume
        o.total_leads,
        o.converted_leads,
        o.demos_held,
        o.closed_won_deals,
        o.closed_lost_deals,

        -- costs
        coalesce(e.total_cost, 0) as total_cost,
        coalesce(e.advertising_cost, 0) as advertising_cost,
        coalesce(e.people_cost, 0) as people_cost,

        -- conversion rates
        div0(o.converted_leads, o.total_leads) as lead_to_opportunity_rate,
        div0(o.demos_held, o.converted_leads) as opportunity_to_demo_rate,
        div0(o.closed_won_deals, o.demos_held) as demo_to_close_rate,
        div0(o.closed_won_deals, o.total_leads) as lead_to_close_rate,

        -- CAC (total cost / closed-won deals in that month)
        -- NOTE: this is a simplification — costs and closes may not align
        -- perfectly by month. A more precise model would use cohort-based
        -- attribution with lag adjustments.
        case
            when o.closed_won_deals > 0
            then coalesce(e.total_cost, 0) / o.closed_won_deals
            else null
        end as cac,

        -- velocity
        o.avg_days_to_first_contact,
        o.avg_days_contact_to_meeting,
        o.avg_days_meeting_to_demo_set,
        o.avg_days_demo_set_to_held,
        o.avg_days_demo_held_to_close,
        o.avg_days_lead_to_opportunity,
        o.avg_days_opp_to_close,
        o.avg_days_lead_to_close,
        o.avg_days_last_activity_to_close,
        o.avg_activities_per_lead,
        o.avg_activities_per_won_deal

    from
        monthly_outcomes as o
    left join
        expenses as e
        on o.lead_created_month = e.expense_month
        and o.channel = e.channel

    -- assumptions: 
    -- 1) exclude leads without a created_at month b/c revenue and costs cannot be attributed
    -- 2) exclude months without costs
    where 1=1
        and lead_created_month is not null
        and coalesce(total_cost, 0) > 0
)

select * from final
