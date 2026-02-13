/*
    Joins leads to their converted opportunities to create a single
    record per prospect journey from lead creation through close.
    Grain: one row per lead.
*/

with

leads as (
    select * from {{ ref('stg_leads') }}
),

opportunities as (
    select * from {{ ref('stg_opportunities') }}
),

final as (
    select
        -- lead fields
        l.lead_id,
        l.channel,
        l.lead_created_date,
        l.form_submission_date,
        l.first_contact_date,
        l.first_sales_call_date,
        l.first_text_sent_date,
        l.first_meeting_booked_date,
        l.last_sales_call_date as lead_last_sales_call_date,
        l.last_sales_activity_date,
        l.last_sales_email_date,
        l.sales_call_count,
        l.sales_text_count,
        l.sales_email_count,
        l.total_activity_count,
        l.predicted_monthly_sales,
        l.marketplaces_used,
        l.online_ordering_used,
        l.cuisine_types,
        l.location_count,
        l.connected_with_decision_maker,
        l.status as lead_status,
        l.is_converted,
        
        -- opportunity fields (null when lead not converted)
        o.opportunity_id,
        o.account_id,
        o.stage_name,
        o.stage_category,
        o.lost_reason,
        o.closed_lost_notes,
        o.business_issue,
        o.attribution_source,
        o.demo_held,
        o.demo_set_date,
        o.demo_time,
        o.demo_date,
        o.created_date as opportunity_created_date,
        o.close_date,
        o.days_to_close,
        o.days_demo_set_to_held,
        
        -- funnel stage resolution
        case
            when o.stage_category = 'closed_won' then 'closed_won'
            when o.stage_category = 'closed_lost' then 'closed_lost'
            when o.opportunity_id is not null then 'opportunity_open'
            when l.first_meeting_booked_date is not null then 'meeting_booked'
            when l.first_sales_call_date is not null then 'contacted'
            else 'new_lead'
        end as current_funnel_stage,
        
        -- velocity metrics
        datediff('day', l.lead_created_date, l.first_contact_date) as days_lead_to_first_contact,
        datediff('day', l.first_contact_date, l.first_meeting_booked_date) as days_contact_to_meeting,
        datediff('day', l.first_meeting_booked_date, o.demo_set_date) as days_meeting_booked_to_demo_set,
        datediff('day', o.demo_date, o.close_date) as days_demo_held_to_close,
        datediff('day', l.lead_created_date, o.created_date) as days_lead_to_opportunity,
        datediff('day', l.last_sales_activity_date, o.close_date) as days_last_activity_to_close,
        datediff('day', l.lead_created_date, o.close_date) as days_lead_to_close,
        
        -- LTV estimation
        -- subscription: $500/mo. transaction: 5% take rate on predicted monthly sales.
        -- assumption: estimate a 24-month lifespan as a baseline for LTV.
        case
            when o.stage_category = 'closed_won' then
                (500.0 + coalesce(l.predicted_monthly_sales, 0) * 0.05) * 24
            else null
        end as estimated_ltv_24mo
        
    from 
        leads as l
    left join 
        opportunities as o on l.converted_opportunity_id = o.opportunity_id
)

select * from final
