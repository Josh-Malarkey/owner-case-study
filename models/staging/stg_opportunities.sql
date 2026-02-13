with

source as (
    select * from {{ source('gtm_case', 'opportunities') }}
),

final as (
    select
        -- keys
        opportunity_id,
        account_id,

        -- stage
        stage_name,
        case
            when stage_name ilike '%closed won%' then 'closed_won'
            when stage_name ilike '%closed lost%' then 'closed_lost'
            else 'open'
        end as stage_category,

        -- loss analysis
        lost_reason_c as lost_reason,
        closed_lost_notes_c as closed_lost_notes,

        -- qualification
        business_issue_c as business_issue,
        how_did_you_hear_about_us_c as attribution_source,

        -- demo fields
        demo_held,
        {{ fix_year_offset('demo_set_date') }} as demo_set_date,
        demo_time,
        demo_time::date as demo_date,

        -- dates
        created_date::date as created_date,
        {{ fix_year_offset('close_date') }} as close_date,
        last_sales_call_date_time,
        last_sales_call_date_time::date as last_sales_call_date,

        -- velocity
        datediff('day', created_date::date, {{ fix_year_offset('close_date') }}) as days_to_close,
        datediff('day', {{ fix_year_offset('demo_set_date') }}, demo_time::date) as days_demo_set_to_held
        
    from source
    
    -- De-duplication of 4 opportunity_ids in source
    -- Keep the most recently created record per opportunity_id.
    qualify row_number() over (partition by opportunity_id order by created_date desc) = 1
)

select * from final
