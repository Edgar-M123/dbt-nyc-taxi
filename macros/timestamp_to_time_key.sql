
{% macro timestamp_to_time_key(column_name) %}
    (hour( {{column_name}} ) * 100 + minute( {{column_name}} ))
{% endmacro %}
