{# Generates an integer date key in YYYYMMDD format from a timestamp column.
   Use this in both fact and dimension models to ensure consistent key formats. #}

{% macro generate_date_key(timestamp_column) %}
    to_number(to_char(cast({{ timestamp_column }} as date), 'YYYYMMDD'))
{% endmacro %}
