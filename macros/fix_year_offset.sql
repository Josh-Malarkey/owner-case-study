-- Some dates have year off by 2000 (e.g. 0024 instead of 2024).
{% macro fix_year_offset(column_name) %}
    dateadd('year', 2000, {{ column_name }})
{% endmacro %}
