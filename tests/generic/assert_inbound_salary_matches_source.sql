{% test assert_inbound_salary_matches_source(model) %}

with

source_total as (
    select
        coalesce(sum(inbound_team_cost), 0) as total_inbound_salary
    from {{ ref('stg_expenses_salary') }}
),

model_total as (
    select
        coalesce(sum(people_cost), 0) as total_inbound_people
    from {{ model }}
    where channel = 'inbound'
)

select
    s.total_inbound_salary,
    m.total_inbound_people,
    abs(s.total_inbound_salary - m.total_inbound_people) as difference
from source_total as s
cross join model_total as m
where abs(s.total_inbound_salary - m.total_inbound_people) > 0.01

{% endtest %}
