with

source as (
    select * from {{ source('gtm_case', 'expenses_advertising') }}
),

final as (
    select
        try_to_date(month, 'Mon-YY') as expense_month,
        {{ parse_currency('advertising') }} as advertising_spend
    from source
)

select * from final
