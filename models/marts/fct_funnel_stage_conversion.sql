/*
    Fact table: funnel stage conversion rates and velocity by cohort.
    Enables drop-off analysis and time-in-stage trending.

    Grain: one row per lead_created_month per channel.
*/

with

funnel as (
    select * from {{ ref('fct_lead_funnel') }}
),

final as (
    select
        lead_created_month,
        channel,
        
        -- volume at each stage
        count(*) as total_leads,
        count_if(first_contact_date is not null) as contacted,
        count_if(connected_with_decision_maker) as connected_dm,
        count_if(first_meeting_booked_date is not null) as meetings_booked,
        count_if(is_converted) as opportunities_created,
        count_if(demo_held) as demos_held,
        count_if(is_closed_won) as closed_won,
        count_if(is_closed_lost) as closed_lost,
        
        -- stage-to-stage conversion rates
        div0(
            count_if(first_contact_date is not null),
            count(*)
        ) as rate_lead_to_contact,
        div0(
            count_if(connected_with_decision_maker),
            count_if(first_contact_date is not null)
        ) as rate_contact_to_dm,
        div0(
            count_if(first_meeting_booked_date is not null),
            count_if(first_contact_date is not null)
        ) as rate_contact_to_meeting,
        div0(
            count_if(is_converted and first_meeting_booked_date is not null),
            count_if(first_meeting_booked_date is not null)
        ) as rate_meeting_to_opportunity,
        div0(
            count_if(demo_held),
            count_if(is_converted)
        ) as rate_opportunity_to_demo,
        div0(
            count_if(is_closed_won),
            count_if(demo_held)
        ) as rate_demo_to_close,
        
        -- overall funnel efficiency
        div0(count_if(is_closed_won), count(*)) as rate_lead_to_close,
        
        -- velocity
        avg(days_lead_to_first_contact) as avg_days_lead_to_contact,
        avg(days_contact_to_meeting) as avg_days_contact_to_meeting,
        avg(days_meeting_booked_to_demo_set) as avg_days_meeting_to_demo_set,
        avg(days_demo_set_to_held) as avg_days_demo_set_to_held,
        avg(days_demo_held_to_close) as avg_days_demo_held_to_close,
        avg(days_lead_to_opportunity) as avg_days_lead_to_opportunity,
        avg(days_to_close) as avg_days_opp_to_close,
        avg(days_lead_to_close) as avg_days_lead_to_close,
        avg(days_last_activity_to_close) as avg_days_last_activity_to_close,
        
    from funnel
    -- assumptions: exclude leads without a created_at month
    where lead_created_month is not null
    group by 1, 2

)

select * from final
