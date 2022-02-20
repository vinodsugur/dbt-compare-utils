{% test compare_table(model, to,column_map) %}
    {{ return(adapter.dispatch('test_compare_table', 'custom_dbt_utils')(model, to,column_map)) }}
{% endtest %}


{% macro snowflake__test_compare_table(model, to,column_map) %}

{{ log("Column Map: " ~ column_map, info=true) }}

{% if column_map is defined and column_map|length > 0 %}
  {{ log("Column Map: " ~ column_map|length, info=true) }}
  {%- set csv_columns = namespace(source_cols="",target_cols="") -%}
  {% for column in column_map %}
    {% set csv_columns.source_cols = csv_columns.source_cols ~  column['model_col']  ~ "," %}
    {% set csv_columns.target_cols = csv_columns.target_cols ~  column['to_col']  ~ "," %}
  {% endfor %}
  {%- set source_cols = csv_columns.source_cols[:-1] -%}
  {%- set target_cols = csv_columns.target_cols[:-1] -%}
  {{ log("columns: " ~ source_cols, info=true) }}
{% else %}

  {%- set columns = adapter.get_columns_in_relation(model) -%}

  {%- set csv_columns = namespace(cols="") -%}

  {% for column in columns %}
    {{ log("Column: " ~ column, info=true) }}
    {% set csv_columns.cols = csv_columns.cols ~  column['column']  ~ "," %}
  {% endfor %}
  {%- set source_cols = csv_columns.cols[:-1] -%}
  {%- set target_cols = csv_columns.cols[:-1] -%}
{% endif %}

{# This macros compares two tables #}

(
  select {{ csv_columns.cols }}
    from {{ model }}
  minus
  select {{ csv_columns.cols }}
    from {{ to }}
)
union
(
  select {{ csv_columns.cols }}
    from {{ to }}
  minus
  select {{ csv_columns.cols }}
    from {{ model }}
)
{% endmacro %}


{% macro bigquery__test_compare_table(model, to,column_map) %}

{%- set source_relation = adapter.get_relation(
      database=model.database,
      schema=model.schema,
      identifier=to) -%}

{{ log("Source Relation: " ~ source_relation, info=true) }}

{{ log("Column Map: " ~ column_map, info=true) }}


{% if column_map is defined and column_map|length > 0 %}
  {{ log("Column Map: " ~ column_map|length, info=true) }}
  {%- set csv_columns = namespace(source_cols="",target_cols="") -%}
  {% for column in column_map %}
    {% set csv_columns.source_cols = csv_columns.source_cols ~  column['model_col']  ~ "," %}
    {% set csv_columns.target_cols = csv_columns.target_cols ~  column['to_col']  ~ "," %}
  {% endfor %}
  {%- set source_cols = csv_columns.source_cols[:-1] -%}
  {%- set target_cols = csv_columns.target_cols[:-1] -%}
  {{ log("columns: " ~ source_cols, info=true) }}
{% else %}

  {%- set columns = adapter.get_columns_in_relation(model) -%}

  {%- set csv_columns = namespace(cols="") -%}

  {% for column in columns %}
    {{ log("Column: " ~ column, info=true) }}
    {% set csv_columns.cols = csv_columns.cols ~  column['column']  ~ "," %}
  {% endfor %}
  {%- set source_cols = csv_columns.cols[:-1] -%}
  {%- set target_cols = csv_columns.cols[:-1] -%}
{% endif %}


{# This macros compares two tables #}

(
  select {{ source_cols }}
    from {{ model }}
  except distinct
  select {{ target_cols }}
    from {{ source_relation }}
)
union all
(
  select {{ target_cols }}
    from {{ source_relation }}
  except distinct
  select {{ source_cols }}
    from {{ model }}
)
{% endmacro %}
