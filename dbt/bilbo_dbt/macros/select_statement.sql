{%- macro select_statement(dict_attributs, list_jointures=[], name_of_the_table="my_tab", mode="tab", set_index=false, set_esri_requirements=false, display_attributs=true, display_jointures=true, display_sortby=true) -%}
{#- Macro permettant de générer une clause SELECT -#}

{#- Variables -#}
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
                {%- if list[0] == "jointure_adaptative" -%}
                    {%- do replace_item(table_name,["(SELECT * FROM bilbo.jointure_adaptative('", list[1], "','", list[2], "',", list[3], ",", list[4], ")) AS ", list[5]]|join(""),2*i+j) -%}
                    {%- do replace_item(alias,list[5],2*i+j) -%}
                {%- elif list[0] == "h3_to_children" -%}
                    {%- do replace_item(table_name,["h3_to_children(",list[1],".hex_id::h3index,",list[2],") AS children_",list[1].split(".")[-1]]|join(""),2*i+j) -%}
                    {%- do replace_item(alias,"children_"+list[1].split(".")[-1],2*i+j) -%}
                {%- endif -%}
        {%- endif -%}   
    {%- endfor -%}  
{%- endfor -%}

{%- if display_attributs -%}{%- if mode == "cte" -%}{{name_of_the_table}} AS ({%- elif mode == "alias" -%}({%- endif -%}SELECT{%- endif -%}
{#- Attributs -#}
{%- for key, value in dict_attributs.items() -%}
    {%- for attribut in value -%}
        {%- if attribut[0]=="!" or attribut[1]=="!" -%}
            {%- set attribut = attribut|replace("!","") -%}
            {%- do groupby.append(([alias[table_name.index(key)],attribut|replace("?","")]|join(".")).split(" AS ")[0]) -%}
        {%- endif -%}
        {%- if attribut[0]=="?" or attribut[1]=="?" -%}
            {%- set attribut = attribut|replace("?","") -%}
            {%- do orderby.append(([alias[table_name.index(key)],attribut|replace("!","")]|join(".")).split(" AS ")[0]) -%}
        {%- endif -%}
        {%- if display_attributs -%}    
        {%- if var("sep") in attribut %}
    {{fonctions(attribut, var("sep"), key, table_name, alias, as_statement=true)}}
        {%- else %} 
    {{alias[table_name.index(key)]}}.{{attribut}}
        {%- endif -%}
        {%- if display_attributs -%}{%- if not loop.last -%}, {%- endif -%}{%- endif -%}
       {%- endif -%}
    {%- endfor -%} 
    {%- if display_attributs -%}{%- if not loop.last -%}, {%- endif -%}{%- endif -%}
{%- endfor -%} 

{%- if display_jointures -%}
{#- Jointures -#}
{%- for i in range((table_name|length)//2) %}
    {%- set type_jointure = "JOIN" %}
    {%- if joint_value[2*i][0]=="!" -%}
        {%- set type_jointure = "LEFT JOIN" -%}
    {%- elif joint_value[2*i+1][0]=="!" -%}
        {%- set type_jointure = "RIGHT JOIN" -%}
    {%- endif -%}
    {%- if loop.first %}
    FROM {{table_name[2*i]}} 
        {{type_jointure}} {{table_name[2*i+1]}} {{on_statement(2*i,2*i+1,var("sep"),table_name,alias,joint_value)}}
    {%- else %}
        {%- if table_name[2*i] not in table_name[:2*i] %}
            {%- set a = 2*i %}
            {%- set b = 2*i+1 %}
        {%- else %}
            {%- set a = 2*i+1 %}
            {%- set b = 2*i %}
        {%- endif %} 
        {{type_jointure}} {{table_name[a]}} {{on_statement(a,b,var("sep"),table_name,alias,joint_value)}}
    {%- endif %}
{%- endfor %}
{%- endif -%}

{%- if display_sortby -%}
{#- Group By -#}
{%- if groupby|length %}
    GROUP BY 
    {%- for i in range(groupby|length) %}
        {%- set groupby_i = groupby[i] -%}
        {%- if var("sep") in groupby_i -%}
            {%- set attribut =  groupby_i.split(".") -%}
            {%- set groupby_i = fonctions(attribut[2], var("sep"), attribut[0]+"."+attribut[1], table_name, alias, as_statement=false) -%}
        {%- endif %}
        {{groupby_i}}
        {%- if not loop.last %}, {% endif %}
    {%- endfor %}
{%- endif %}

{#- Order By -#}
{%- if orderby|length %}
    ORDER BY 
    {%- for i in range(orderby|length) %}
        {%- set orderby_i = orderby[i] -%}
        {%- if var("sep") in orderby_i -%}
             {%- set attribut =  orderby_i.split(".") -%}
            {%- set orderby_i = fonctions(attribut[2], var("sep"), attribut[0]+"."+attribut[1], table_name, alias, as_statement=false) -%}
        {%- endif %}
        {{orderby_i}}
        {%- if not loop.last %}, {% endif %}
    {%- endfor %}
{%- endif -%}{%- if mode == "alias" -%}) AS {{name_of_the_table}}{% elif mode == "cte" %}){%- endif -%}
{%- endif -%}

{#- Post-hooks -#}
{#- Index -#}
{%- set index_hook = ["CREATE INDEX IF NOT EXISTS ix_",schema,"_",name_of_the_table,"_hex_id ON ",
    schema,".",name_of_the_table,' USING btree
    (hex_id COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;']|join("") -%}

{#- ESRI -#}
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