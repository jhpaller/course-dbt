{% macro row_number(partition_by, order_by, is_asc) %}

    {% if is_asc == TRUE %}

    ROW_NUMBER() OVER (PARTITION BY {{ partition_by }} ORDER BY {{ order_by }} ASC)

    {% else %}

    ROW_NUMBER() OVER (PARTITION BY {{ partition_by }} ORDER BY {{ order_by }} DESC)

    {% endif %}

{% endmacro %}