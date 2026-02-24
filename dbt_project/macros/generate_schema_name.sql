{#
  Override de generate_schema_name para que dbt use el schema
  definido en dbt_project.yml directamente (sin prefijo del target).

  Ejemplo: models/bronze/* → local.bronze (no local.dev_bronze)
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema | trim }}
    {%- endif -%}
{%- endmacro %}
