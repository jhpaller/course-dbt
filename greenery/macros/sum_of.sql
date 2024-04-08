{% macro sum_of(col_name, col_value) %}

SUM(DECODE({{ col_name}}, '{{ col_value }}', 1, 0))

{% endmacro %}