{% macro convert_utc_to_pst(timestamp_column) %}
    SELECT DATETIME(run_timestamp, "America/Los_Angeles")
{% endmacro %}