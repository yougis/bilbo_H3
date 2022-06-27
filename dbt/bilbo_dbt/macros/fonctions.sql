{%- macro fonctions(attribut, sep, key, table_name, alias, as_statement=false) -%}
    {%- set args = attribut.split(sep) %}
    {%- if args[0] == "area" %}h3_cell_area({{alias[table_name.index(key)]}}.{{args[1]}}::h3index){% if as_statement %} AS {{args[2]}}{% endif %}{% endif %}
    {%- if args[0] == "find_parent" %}h3_to_parent({{alias[table_name.index(key)]}}.{{args[1]}}::h3index,{{args[2]}})::text{% if as_statement %} AS hex_id_{{args[2]}}{% endif %}{% endif %}
    {%- if args[0] == "array_agg" %}ARRAY_AGG(DISTINCT {{alias[table_name.index(key)]}}.{{args[1]}}){% if as_statement %} AS {{args[2]}}{% endif %}{% endif %}
    {%- if args[0] == "array_length_agg" %}ARRAY_LENGTH(ARRAY_AGG(DISTINCT {{alias[table_name.index(key)]}}.{{args[1]}}),1){% if as_statement %} AS {{args[2]}}{% endif %}{% endif %}
    {%- if args[0] == "sum_agg" %}SUM({{alias[table_name.index(key)]}}.{{args[1]}}){% if as_statement %} AS {{args[2]}}{% endif %}{% endif %}
    {%- if args[0] == "min_agg" %}MIN({{alias[table_name.index(key)]}}.{{args[1]}}){% if as_statement %} AS {{args[2]}}{% endif %}{% endif %}
    {%- if args[0] == "min_init_cap_agg" %}MIN(INITCAP({{alias[table_name.index(key)]}}.{{args[1]}})){% if as_statement %} AS {{args[2]}}{% endif %}{% endif %}
    {%- if args[0] == "geom_agg" %}h3_to_geo_boundary(h3_to_parent(MIN({{alias[table_name.index(key)]}}.{{args[1]}})::h3index,{{args[2]}}))::geometry{% if as_statement %} AS geometry{% endif %}{% endif %}
    {%- if args[0] == "id_esri" %}row_number() OVER(){% if as_statement %} AS id_esri{% endif %}{% endif %}
{%- endmacro -%}
