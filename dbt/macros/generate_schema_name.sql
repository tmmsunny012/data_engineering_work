{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {# If no custom schema is specified, use the target schema #}
        {{ default_schema }}

    {%- else -%}
        {# Use ONLY the custom schema name, without prefix #}
        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
