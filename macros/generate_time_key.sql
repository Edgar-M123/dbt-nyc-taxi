{# Generates an integer time key in HHMM format (e.g. 1430 = 2:30 PM) from a timestamp column.
   Use this in both fact and dimension models to ensure consistent key formats. #}

{% macro generate_time_key(timestamp_column) %}
    date_part(hour, {{ timestamp_column }}) * 100 + date_part(minute, {{ timestamp_column }})
{% endmacro %}
