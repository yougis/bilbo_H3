{%- macro fonctions(attribut, sep, key, table_name, alias, as_statement=false) -%}
    {%- set args = attribut.split(sep) %}
    {%- if args[0] == "area" %}h3_cell_area({{alias[table_name.index(key)]}}.{{args[1]}}::h3index){% if as_statement %} AS {{args[2]}}{% endif -%}
    {%- elif args[0] == "array_agg" %}ARRAY_AGG(DISTINCT {{alias[table_name.index(key)]}}.{{args[1]}}){% if as_statement %} AS {{args[2]}}{% endif -%}
    {%- elif args[0] == "array_length_agg" %}ARRAY_LENGTH(ARRAY_AGG(DISTINCT {{alias[table_name.index(key)]}}.{{args[1]}}),1){% if as_statement %} AS {{args[2]}}{% endif -%}
    {%- elif args[0] == "sum_agg" %}SUM({{alias[table_name.index(key)]}}.{{args[1]}}){% if as_statement %} AS {{args[2]}}{% endif -%}
    {%- elif args[0] == "sum_area_agg" %}SUM(h3_cell_area({{alias[table_name.index(key)]}}.{{args[1]}}::h3index)){% if as_statement %} AS {{args[2]}}{% endif -%}
    {%- elif args[0] == "min_agg" %}MIN({{alias[table_name.index(key)]}}.{{args[1]}}){% if as_statement %} AS {{args[2]}}{% endif -%}
    {%- elif args[0] == "min_init_cap_agg" %}MIN(INITCAP({{alias[table_name.index(key)]}}.{{args[1]}})){% if as_statement %} AS {{args[2]}}{% endif -%}
    {%- elif args[0] == "geom_agg" %}h3_to_geo_boundary(h3_to_parent({{alias[table_name.index(key)]}}.{{args[1]}}::h3index,{{args[2]}}))::geometry{% if as_statement %} AS geometry{% endif -%}
    {%- elif args[0] == "id_esri" %}row_number() OVER(){% if as_statement %} AS id_esri{% endif %}
    {%- elif args[0] == "hex_id" %}(CASE WHEN h3_get_resolution({{alias[table_name.index(key)]}}.hex_id::h3index)>={{args[1]}} THEN h3_to_parent({{alias[table_name.index(key)]}}.hex_id::h3index,{{args[1]}}) ELSE children END)::text{% if as_statement %} AS hex_id{% endif -%}
    {%- elif args[0] == "hex_id_children" %}children::text{% if as_statement %} AS hex_id{% endif -%}
    {%- elif args[0] == "hex_id_parent" %}h3_to_parent({{alias[table_name.index(key)]}}.hex_id::h3index,{{args[1]}})::text{% if as_statement %} AS hex_id{% endif -%}
    {%- elif args[0] == "sum_area_adaptatif_agg" %}SUM(LEAST(h3_cell_area({{args[1]}}::h3index),{%- for i in range(2,args|length-1) -%}COALESCE(h3_cell_area({{args[i]}}::h3index),'+infinity'){%- if not loop.last -%},{%- endif -%}{%- endfor -%})){% if as_statement %} AS {{args[-1]}}{% endif -%}
    {% endif %}
{%- endmacro -%}