/*
    Fact table: one row per lead representing the full prospect journey.
    Supports funnel conversion analysis, velocity metrics, activity efficiency, and lead-level LTV estimation.

    Grain: one row per lead_id, which means it can grow to hundreds of thousands or millions of rows as the business scales.
*/

with

prospects as (
    select * from {{ ref('int_lead_opportunity_joined') }}
),

final as (
    select
        -- keys
        lead_id,
        opportunity_id,
        account_id,

        -- dimensions
        channel,
        lead_status,
        current_funnel_stage,
        stage_name as opportunity_stage_name,
        stage_category as opportunity_stage_category,
        lost_reason,
        closed_lost_notes,
        business_issue,
        attribution_source,
        cuisine_types,
        length(cuisine_types) as cuisine_count,
        marketplaces_used,
        online_ordering_used,
        location_count,
        connected_with_decision_maker,

        -- funnel flags
        is_converted,
        demo_held,
        current_funnel_stage = 'closed_won' as is_closed_won,
        current_funnel_stage = 'closed_lost' as is_closed_lost,

        -- dates
        form_submission_date,
        first_sales_call_date,
        first_text_sent_date,
        first_meeting_booked_date,
        first_contact_date,
        demo_set_date,
        demo_time,
        demo_date,
        opportunity_created_date,
        close_date,
        last_sales_activity_date,

        -- cohort dates
        lead_created_date,
        date_trunc('month', lead_created_date) as lead_created_month,

        -- activity metrics
        sales_call_count,
        sales_text_count,
        sales_email_count,
        total_activity_count,

        -- velocity metrics (days)
        days_lead_to_first_contact,
        days_contact_to_meeting,
        days_meeting_booked_to_demo_set,
        days_demo_set_to_held,
        days_demo_held_to_close,
        days_lead_to_opportunity,
        days_to_close,
        days_lead_to_close,
        days_last_activity_to_close,

        -- revenue & LTV
        predicted_monthly_sales,
        coalesce(predicted_monthly_sales, 0) * 0.05 as predicted_monthly_transaction_revenue,
        500.0 as monthly_subscription_revenue,
        500.0 + coalesce(predicted_monthly_sales, 0) * 0.05 as predicted_monthly_total_revenue,
        estimated_ltv_24mo
        
    from prospects
    -- assumptions: exclude leads without a created_at month
    where lead_created_month is not null
)

select * from final
