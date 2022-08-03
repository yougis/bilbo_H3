{%- macro unnest(table_name, field, dict_subfields) -%}
{%- set col_names_result = run_query("SELECT json_object_keys(to_json((SELECT t FROM "+ table_name +" t LIMIT 1)))") -%}
{%- if execute -%}
    {%- set col_names = col_names_result.columns[0].values() -%}
{%- else -%}
    {% set col_names = [] -%}
{%- endif -%}
SELECT  
    row_number() OVER()::int AS id, 
    {%- for attribut in col_names -%}
    {%- if attribut != field -%}
    {%- if true %}
    {{attribut}}{% endif -%}{%- if not loop.last -%},{% endif %}
    {%- endif -%}
    {%- endfor -%}
    {%- for key, value in dict_subfields.items() %}
    {{key}}{%- if not loop.last -%},{%- endif -%}
    {%- endfor %}
FROM {{table_name}}
JOIN LATERAL json_to_recordset({{table_name}}.{{field}}::json) AS x({%- for key, value in dict_subfields.items() -%}{{'"'+key+'"'}} {{value}}{%- if not loop.last -%},{%- endif -%}{%- endfor -%}) ON true
{%- endmacro -%}

{{config(materialized="view", alias="view_"+table_name.split(".")[-1])}}