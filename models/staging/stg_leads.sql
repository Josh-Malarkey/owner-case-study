with

source as (
    select * from {{ source('gtm_case', 'leads') }}
),

final as (
    select
        lead_id,
        converted_opportunity_id,
        
        -- assumption: inbound leads have a form submission; outbound leads are BDR-sourced with no form submission date.
        case
            when form_submission_date is not null then 'inbound'
            else 'outbound'
        end as channel,
        {{ fix_year_offset('form_submission_date') }} as form_submission_date,
        first_sales_call_date::date as first_sales_call_date,
        first_text_sent_date::date as first_text_sent_date,
        
        -- assumption: create a unified first contact date based on first call, if applicable, fallback to first text
        coalesce(first_sales_call_date::date, first_text_sent_date::date) as first_contact_date,
        first_meeting_booked_date::date as first_meeting_booked_date,
        
        -- assumption: create unified lead_created date for inbound/outbound reps based on form submission date or first sales contact
        coalesce({{ fix_year_offset('form_submission_date') }}, first_sales_call_date::date, first_text_sent_date::date) as lead_created_date,
        last_sales_call_date,
        last_sales_activity_date,
        last_sales_email_date,
        sales_call_count,
        sales_text_count,
        sales_email_count,
        (coalesce(sales_call_count, 0) + coalesce(sales_text_count, 0) + coalesce(sales_email_count, 0)) as total_activity_count,
        
        -- assumption: predicted_sales_with_owner is a monthly metric
        {{ parse_currency('predicted_sales_with_owner') }} as predicted_monthly_sales,
        marketplaces_used,
        online_ordering_used,
        cuisine_types,
        location_count,
        connected_with_decision_maker,
        status,
        converted_opportunity_id is not null as is_converted
        
    from source
)

select * from final
