{% test compare_table(model, compare_model,column_map,column_compare,ignore_colmns) %}
    {{ return(adapter.dispatch('test_compare_table', 'custom_dbt_utils')(model, compare_model,column_map,column_compare,ignore_columns)) }}
{% endtest %}

{% macro compare_code(model, compare_model,column_map,column_compare,ignore_columns) %}

{% set source_cols = "" %}
{% set target_cols = "" %}

{# The below check is for comparision on column map #}
{% if column_map is defined and column_map|length > 0 %}
  {{ log("Column Map: " ~ column_map, info=true) }}
  {{ log("Column Map: " ~ column_map|length, info=true) }}
  {%- set csv_columns = namespace(source_cols="",target_cols="") -%}
  {% for column in column_map %}
    {% set csv_columns.source_cols = csv_columns.source_cols ~  column['model_column']  ~ "," %}
    {% set csv_columns.target_cols = csv_columns.target_cols ~  column['compare_model_column']  ~ "," %}
  {% endfor %}
  {%- set source_cols = csv_columns.source_cols[:-1] -%}
  {%- set target_cols = csv_columns.target_cols[:-1] -%}

{# The below check is for comparision is driven either by model, compare_model, all   #}
{% elif column_compare is defined and (column_compare == "model" or column_compare == "compare_model")  %}
  {%- set columns = adapter.get_columns_in_relation(compare_model) -%}
  {%- set csv_columns = namespace(cols="") -%}
  {% for column in columns %}
    {{ log("Column: " ~ column, info=true) }}
    {% if ignore_columns is not defined or (ignore_columns is defined and column.lower() not in ignore_columns|lower) %}
      {% set csv_columns.cols = csv_columns.cols ~  column['column']  ~ "," %}
    {% endif %}
  {% endfor %}
  {%- set source_cols = csv_columns.cols[:-1] -%}
  {%- set target_cols = csv_columns.cols[:-1] -%}

{% elif column_compare is defined and (column_compare == "all")  %}
  {%- set source_cols = "*" -%}
  {%- set target_cols = "*" -%}

{% endif %}

{{ log("Columns: " ~ source_cols, info=true) }}
{{ return([source_cols,target_cols]) }}

{% endmacro %}

{% macro snowflake__test_compare_table(model, compare_model,column_map,column_compare,ignore_columns) %}

{# This macros compares two tables #}

{% set columns_list = compare_code(model, compare_model,column_map,column_compare,ignore_columns) %}

{%- set source_cols = columns_list[0] -%}
{%- set target_cols = columns_list[1] -%}


(
  select {{ source_cols }}
    from {{ model }}
  minus
  select {{ target_cols }}
    from {{ compare_model }}
)
union
(
  select {{ target_cols }}
    from {{ compare_model }}
  minus
  select {{ source_cols }}
    from {{ model }}
)
{% endmacro %}


{% macro bigquery__test_compare_table(model, compare_model,column_map,column_compare,ignore_columns) %}

{% set columns_list = compare_code(model, compare_model,column_map,column_compare,ignore_columns) %}

{%- set source_cols = columns_list[0] -%}
{%- set target_cols = columns_list[1] -%}


{# This macros compares two tables #}

(
  select {{ source_cols }}
    from {{ model }}
  except distinct
  select {{ target_cols }}
    from {{ compare_model }}
)
union all
(
  select {{ target_cols }}
    from {{ compare_model }}
  except distinct
  select {{ source_cols }}
    from {{ model }}
)
{% endmacro %}
