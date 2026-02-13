/*
    Fact table: activity efficiency analysis.
    Measures how many touches (calls, texts, emails) are needed to move prospects through the funnel, segmented by channel and outcome.

    Grain: one row per channel per outcome per lead_created_month.
*/

with

funnel as (
    select * from {{ ref('fct_lead_funnel') }}
),

final as (
    select
        lead_created_month,
        channel,
        current_funnel_stage as outcome,
        count(*) as lead_count,

        -- activity volume
        sum(sales_call_count) as total_calls,
        sum(sales_text_count) as total_texts,
        sum(sales_email_count) as total_emails,
        sum(total_activity_count) as total_activities,

        -- per-lead activity averages
        avg(sales_call_count) as avg_calls_per_lead,
        avg(sales_text_count) as avg_texts_per_lead,
        avg(sales_email_count) as avg_emails_per_lead,
        avg(total_activity_count) as avg_activities_per_lead,

        -- velocity
        avg(days_lead_to_first_contact) as avg_days_to_first_contact,
        avg(days_contact_to_meeting) as avg_days_contact_to_meeting,
        avg(days_meeting_booked_to_demo_set) as avg_days_meeting_to_demo_set,
        avg(days_demo_set_to_held) as avg_days_demo_set_to_held,
        avg(days_demo_held_to_close) as avg_days_demo_held_to_close,
        avg(days_lead_to_opportunity) as avg_days_lead_to_opportunity,
        avg(days_lead_to_close) as avg_days_to_close,
        avg(days_last_activity_to_close) as avg_days_last_activity_to_close,

        -- revenue potential (closed-won only)
        avg(
            case
                when current_funnel_stage = 'closed_won'
                then estimated_ltv_24mo
            end
        ) as avg_ltv_won_deals
        
    from funnel
    -- assumptions: exclude leads without a created_at month
    where lead_created_month is not null
    group by 1, 2, 3

)

select * from final
