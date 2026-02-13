{% test assert_outbound_salary_matches_source(model) %}

with

source_total as (
    select
        coalesce(sum(outbound_team_cost), 0) as total_outbound_salary
    from {{ ref('stg_expenses_salary') }}
),

model_total as (
    select
        coalesce(sum(people_cost), 0) as total_outbound_people
    from {{ model }}
    where channel = 'outbound'
)

select
    s.total_outbound_salary,
    m.total_outbound_people,
    abs(s.total_outbound_salary - m.total_outbound_people) as difference
from source_total as s
cross join model_total as m
where abs(s.total_outbound_salary - m.total_outbound_people) > 0.01

{% endtest %}
