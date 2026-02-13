{% macro parse_currency(column_name, precision=38, scale=2) %}
    try_to_number(
        replace(replace(replace(replace(replace({{ column_name }}, 'US$', ''), ' ', ''), chr(160), ''), '\t', ''), ',', '.'),
        {{ precision }},
        {{ scale }}
    )
{% endmacro %}
