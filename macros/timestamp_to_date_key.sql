
{% macro timestamp_to_date_key(column_name) %}
    (cast( strftime( {{ column_name }}, '%Y%m%d') as integer))
{% endmacro %}
