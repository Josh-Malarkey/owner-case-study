/*
    Combines advertising and salary/commission expenses into a single
    monthly expense view, broken out by channel.
    Uses a date spine to ensure every month is represented, even if
    no expense data exists for that month.
    Grain: one row per month per channel.
*/

with

advertising as (
    select * from {{ ref('stg_expenses_advertising') }}
),

salary as (
    select * from {{ ref('stg_expenses_salary') }}
),

-- generate a continuous monthly spine from the earliest to latest month
month_bounds as (
    select
        min(expense_month) as min_month,
        max(expense_month) as max_month
    from (
        select expense_month from advertising
        union
        select expense_month from salary
    )
),

date_spine as (
    select
        dateadd('month', row_number() over (order by null) - 1, min_month) as expense_month
    from month_bounds,
        lateral flatten(input => array_generate_range(
            0,
            datediff('month', min_month, max_month) + 1
        ))
),

prep as (
    select
        ds.expense_month,
        
        -- inbound costs = advertising + inbound sales team
        coalesce(a.advertising_spend, 0) + coalesce(s.inbound_team_cost, 0) as inbound_total_cost,
        coalesce(a.advertising_spend, 0) as inbound_advertising_cost,
        coalesce(s.inbound_team_cost, 0) as inbound_people_cost,
        
        -- outbound costs = outbound sales team only (no ad spend)
        coalesce(s.outbound_team_cost, 0) as outbound_total_cost,
        coalesce(s.outbound_team_cost, 0) as outbound_people_cost

    from
        date_spine as ds
    left join
        advertising as a on ds.expense_month = a.expense_month
    left join
        salary as s on ds.expense_month = s.expense_month
),

-- unpivot to one row per channel per month for easier downstream joins
final as (
    select
        expense_month,
        'inbound' as channel,
        inbound_total_cost as total_cost,
        inbound_advertising_cost as advertising_cost,
        inbound_people_cost as people_cost
    from prep
    union all
    select
        expense_month,
        'outbound' as channel,
        outbound_total_cost as total_cost,
        0 as advertising_cost,
        outbound_people_cost as people_cost
    from prep
)

select * from final
