with

source as (
    select * from {{ source('gtm_case', 'expenses_salary_and_commissions') }}
),

final as (
    select
        try_to_date(month, 'Mon-YY') as expense_month,
        {{ parse_currency('outbound_sales_team') }} as outbound_team_cost,
        {{ parse_currency('inbound_sales_team') }} as inbound_team_cost
    from source
)

select * from final
