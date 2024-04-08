{% macro row_number(partition_by, order_by, asc_or_desc) %}

    ROW_NUMBER() OVER (PARTITION BY {{ partition_by }} ORDER BY {{ order_by }} {{ asc_or_desc }})

{% endmacro %}