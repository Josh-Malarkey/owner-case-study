{% macro fix_year_offset(column_name) %}
    dateadd('year', 2000, {{ column_name }})
{% endmacro %}
