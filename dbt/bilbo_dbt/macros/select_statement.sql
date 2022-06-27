{%- macro select_statement(dict_attributs, list_jointures, name_of_the_table, alias_of_the_table, granularite, set_alias=false, set_index=false, set_esri_requirements=false) -%}
{%- set ns = namespace() -%}
{%- set table_name = [] -%}
{%- set alias = [] -%}
{%- set joint_value = [] -%}
{%- set groupby = [] -%}
{%- set orderby = [] -%}

{%- if set_esri_requirements -%}
    {%- for key, value in dict_attributs.items() -%}
    {%- if loop.first -%}
            {%- do dict_attributs[key].append("id_esri" + var("sep")) -%}
        {%- endif -%}
    {%- endfor -%} 
{%- endif -%}

{#- Création des listes du nom des tables et des clés de jointure -#}
{%- for dict in list_jointures -%}
    {%- for key, value in dict.items() -%}
        {%- do table_name.append(key) -%}
        {%- do alias.append(key) -%}
        {%- do joint_value.append(value) -%}
    {%- endfor -%}
{%- endfor -%}

{%- for i in range((table_name|length)//2) -%}
     {%- for j in range(2) -%}
        {%- if var("sep") in table_name[2*i+j] -%}
            {%- set list = table_name[2*i+j].split(var("sep")) -%}
                {%- if list[0] == "dynamic_to_uniform" -%}
                    {%- do replace_item(table_name,["(SELECT * FROM ",schema,".dynamic_to_uniform()) AS ", var("dict_shortcut")["dynamic_to_uniform"]]|join(""),2*i+j) -%}
                    {%- do replace_item(alias,var("dict_shortcut")["dynamic_to_uniform"],2*i+j) -%}
                {%- endif -%}
        {%- endif -%}   
    {%- endfor -%}  
{%- endfor -%}

{%- if set_alias -%}({%- endif -%}SELECT
{#- Attributs -#}
{%- for key, value in dict_attributs.items() -%}
    {%- for attribut in value -%}
        {%- if attribut[0]=="!" or attribut[1]=="!" -%}
            {%- set attribut = attribut|replace("!","") -%}
            {%- set groupby = groupby.append(([alias[table_name.index(key)],attribut|replace("?","")]|join(".")).split(" AS ")[0]) -%}
        {%- endif -%}
        {%- if attribut[0]=="?" or attribut[1]=="?" -%}
            {%- set attribut = attribut|replace("?","") -%}
            {%- set orderby = orderby.append(([alias[table_name.index(key)],attribut|replace("!","")]|join(".")).split(" AS ")[0]) -%}
        {%- endif -%}
        
        {%- if var("sep") in attribut %}
    {{fonctions(attribut, var("sep"), key, table_name, alias, as_statement=true)}}
        {%- else %} 
    {{alias[table_name.index(key)]}}.{{attribut}}
        {%- endif -%}
        {%- if not loop.last -%}, {%- endif -%}
    {%- endfor -%} 
    {%- if not loop.last -%}, {%- endif -%}
{%- endfor -%} 

{#- Jointures -#}
{%- for i in range((table_name|length)//2) %}
    {%- if loop.first %}
    FROM {% if table_name[2*i][0] != "(" %}{{schema}}.{%- endif %}{{table_name[2*i]}} 
        JOIN {% if table_name[2*i+1][0] != "(" %}{{schema}}.{%- endif %}{{table_name[2*i+1]}} {{on_statement(2*i,2*i+1,var("sep"),table_name,alias,joint_value)}}
    {%- else %}
        {%- if table_name[2*i] not in table_name[:2*i] %}
            {%- set a = 2*i %}
            {%- set b = 2*i+1 %}
        {%- else %}
            {%- set a = 2*i+1 %}
            {%- set b = 2*i %}
        {%- endif %} 
        JOIN {% if table_name[a][0] != "(" %}{{schema}}.{%- endif %}{{table_name[a]}} {{on_statement(a,b,var("sep"),table_name,alias,joint_value)}}
    {%- endif %}
{%- endfor %}

{#- Group By -#}
{%- if groupby|length %}
    GROUP BY 
    {%- for i in range(groupby|length) %}
        {{groupby[i]}}
        {%- if not loop.last %}, {% endif %}
    {%- endfor %}
{%- endif %}

{#- Order By -#}
{%- if orderby|length %}
    ORDER BY 
    {%- for i in range(orderby|length) %}
        {{orderby[i]}}
        {%- if not loop.last %}, {% endif %}
    {%- endfor %}
{%- endif -%}{%- if set_alias -%}) AS {{alias_of_the_table}}{% else %}{%- endif -%}

{#- Index -#}
{%- set index_hook = ["CREATE INDEX IF NOT EXISTS ix_",schema,"_",name_of_the_table,"_hex_id_",granularite,
    " ON ",schema,".",name_of_the_table," USING btree
    (hex_id_",granularite,' COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;']|join("") -%}

{# ESRI #}
{%- set esri_hook = ["ALTER TABLE IF EXISTS ",schema,".",name_of_the_table,
    " ADD CONSTRAINT ",schema,"_",name_of_the_table,"_pkey PRIMARY KEY (id_esri);
ALTER TABLE IF EXISTS ",schema,".",name_of_the_table,"
    ADD CONSTRAINT enforce_srid_shape CHECK (st_srid(geometry) = 3163);"]|join("") -%}

{%- if set_index -%}
{{config({"post-hook": [index_hook]})}}
{%- endif -%}

{%- if set_esri_requirements -%}
{{config({"post-hook": [esri_hook]})}}
{%- endif -%}

{%- if set_esri_requirements and set_index -%}
{{config({"post-hook": [index_hook,esri_hook]})}}
{%- endif -%}
{%- endmacro -%}