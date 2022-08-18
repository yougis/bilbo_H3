{%- macro dtm(list_tab, name_of_the_table='table', res=8, json_agg={}, set_index=false, set_esri_requirements=false) -%}
{#- Macro permettant de créer des DataMarts -#}
{{config(materialized="table", alias=name_of_the_table)}}

{#- Variables -#}
{%- set ns = namespace() -%}
{%- set ns.str = "" -%}
{%- set date_attributs = [] -%}
{%- set date_jointures = [] -%}
{%- set date_attributs_dim = [] -%}
{%- set date_jointures_dim = [] -%}
{%- set json_keys = [] -%}
{%- set json_values = [] -%}
{%- set groupby = [] -%}
{%- set groupby_sans_json_agg = [] -%}
{%- set dict_attributs = {} -%}
{%- set list_jointures = [] -%}
{%- set list_jointures_dim = [] -%}
{%- set list_statut = [] -%}
{%- set ix_ind = None -%}
{%- set ix_con = None -%}
{%- set type_t1 = "cte" -%}
{%- set cte = "t2" -%}
{%- set table_name = [] -%}
{%- set alias = [] -%}
{%- set joint_value = [] -%}

{%- for tab in list_tab -%}
    {%- do list_statut.append(tab["statut"]) -%}
{%- endfor -%}
{%- set ix_ind = list_statut.index('indicateur') -%}
{%- if 'context' in  list_statut -%}
    {%- set ix_con = indexes(list_statut,'context') -%}  
{%- endif -%}
{%- if 'dimension' in  list_statut -%}
    {%- set ix_dim = indexes(list_statut,'dimension') -%}
{%- endif -%}

{%- if json_agg=={} -%}
    {%- set cte = "t1"-%}
{%- endif -%}
{%- set dict_attributs_dim = {cte:["*"]} -%}

{%- if 'dimension' not in  list_statut and json_agg=={} -%}
    {%- set type_t1 = "table"-%}
{%- endif -%}

{#- Récupération des résolutions minimales et maximales de la table indicateur -#}
{%- set result_ind = run_query("SELECT MIN(h3_get_resolution(hex_id::h3index)), MAX(h3_get_resolution(hex_id::h3index)) FROM " + list_tab[ix_ind]["nom"]) -%}
{%- if execute -%}
    {%- set res_min_ind = result_ind.columns[0].values()[0] -%}
    {%- set res_max_ind = result_ind.columns[1].values()[0] -%}
{%- else -%}
    {% set res_min_ind = 0 -%}
    {% set res_max_ind = 0 -%}
{%- endif -%}

{%- if json_agg!={} -%}
    {%- for key, value in json_agg.items() -%}
        {%- do json_keys.append(key) -%}
    {%- endfor -%}
    {%- for key in json_agg[json_keys[0]] -%}
        {%- do json_values.append(json_agg[json_keys[0]][key]) -%}
    {%- endfor -%}
    {%- for tab in list_tab -%}
        {%- for attribut in tab["attributs"] -%}
            {%- set split = attribut.split(var("sep")) -%}
            {%- if "!" in attribut -%}
                {%- do groupby.append(split[split|length-1]|replace("!","")) -%}
            {% endif %}
        {%- endfor -%}
    {%- endfor %}
    {%- for i in range(groupby|length) -%}
        {%- if groupby[i] not in json_values -%}
            {%- do groupby_sans_json_agg.append(groupby[i]) -%}
        {%- endif -%}
    {%- endfor %}
{%- endif -%}

{%- for tab in list_tab -%}
    {%- set list_attributs = [] -%}
    {%- set list_attributs_dim = [] -%}
    {%- if tab["nom"] == list_tab[ix_ind]["nom"] -%}
        {%- if (res_min_dim == res) and (res_max_dim == res) -%}
            {%- do list_attributs.append('!hex_id') -%} 
        {%- elif res_min_ind >= res -%}
            {%- do list_attributs.append(['!hex_id_parent',var("sep"),res]|join("")) -%}  
        {%- elif res_max_ind <= res -%}
            {%- do list_attributs.append(['!hex_id_children',var("sep"),res]|join("")) -%}
            {%- do list_jointures.append({list_tab[ix_ind]["nom"]:'!', ['h3_to_children+',list_tab[ix_ind]["nom"],"+",res]|join(""):''}) -%}  
        {%- else -%}
            {%- do list_attributs.append(['!hex_id',var("sep"),res]|join("")) -%} 
            {%- do list_jointures.append({list_tab[ix_ind]["nom"]:'!', ['h3_to_children+',list_tab[ix_ind]["nom"],"+",res]|join(""):''}) -%}
        {%- endif -%}  
    {%- endif -%}   
    {%- if tab["statut"] != 'dimension' -%}
        {%- for attribut in tab["attributs"] -%}
            {%- if var("sep") in attribut -%}
                {%- set list = attribut.split(var("sep")) -%}
                {%- if list[0] == 'date' -%}
                    {%- do date_attributs.append(list[2]+" AS "+list[3])-%}
                    {%- do date_jointures.append({tab["nom"]:"!"+list[1],var("nom_tab_date"):"date_id"})-%}
                {%- elif list[0] == '!date' -%}
                    {%- do date_attributs.append("!"+list[2]+" AS "+list[3])-%}
                    {%- do date_jointures.append({tab["nom"]:"!"+list[1],var("nom_tab_date"):"date_id"})-%}
                {%- elif tab["nom"] == list_tab[ix_ind]["nom"] and 'geom' in list[0] -%}
                    {%- if (res_min_dim == res) and (res_max_dim == res) -%}
                        {%- do list_attributs.append(list[0]+"_parent"+var("sep")+list[1:]|join(var("sep"))) -%} 
                    {%- elif res_min_ind >= res -%}
                        {%- do list_attributs.append(list[0]+"_parent"+var("sep")+list[1:]|join(var("sep"))) -%} 
                    {%- elif res_max_ind <= res -%}
                        {%- do list_attributs.append(list[0]+"_children"+var("sep")+list[1:]|join(var("sep"))) -%} 
                    {%- else -%}
                        {%- do list_attributs.append(attribut) -%} 
                    {%- endif -%}  
                {%- elif "sum_area_adaptatif" in list[0] -%}
                    {%- for idx in ix_con -%}
                        {%- set ns.str = ns.str + list_tab[idx]["nom"] + ".hex_id+" -%}
                    {%- endfor -%}
                    {%- do date_attributs.append([list[0],"+",list_tab[ix_ind]["nom"],".hex_id+",ns.str,list[1]]|join(""))-%}
                {%- else -%}
                    {%- do list_attributs.append(attribut) -%}
                {%- endif -%}
            {%- else -%}
                {%- do list_attributs.append(attribut) -%}   
            {%- endif -%}   
        {%- endfor -%}  
        {%- do dict_attributs.update({tab["nom"]: list_attributs}) -%}
    {%- else -%}   
        {%- for attribut in tab["attributs"] -%}
            {%- if var("sep") in attribut -%}
                {%- set list = attribut.split(var("sep")) -%}
                {%- if list[0] == 'date' -%}
                    {%- do date_attributs_dim.append(list[2]+" AS "+list[3])-%}
                    {%- do date_jointures_dim.append({tab["nom"]:"!"+list[1],var("nom_tab_date"):"date_id"})-%}
                {%- elif list[0] == '!date' -%}
                    {%- do date_attributs_dim.append("!"+list[2]+" AS "+list[3])-%}
                    {%- do date_jointures_dim.append({tab["nom"]:"!"+list[1],var("nom_tab_date"):"date_id"})-%}
                {%- elif 'geom' in list[0] -%}
                    {%- if (res_min_dim == res) and (res_max_dim == res) -%}
                        {%- do list_attributs_dim.append(list[0]+"_parent"+var("sep")+list[1:]|join(var("sep"))) -%} 
                    {%- elif res_min_ind >= res -%}
                        {%- do list_attributs_dim.append(list[0]+"_parent"+var("sep")+list[1:]|join(var("sep"))) -%} 
                    {%- elif res_max_ind <= res -%}
                        {%- do list_attributs_dim.append(list[0]+"_children"+var("sep")+list[1:]|join(var("sep"))) -%} 
                    {%- else -%}
                        {%- do list_attributs_dim.append(attribut) -%} 
                    {%- endif -%}  
                {%- endif -%}  
            {%- else -%}
                {%- do list_attributs_dim.append(attribut) -%}
            {%- endif -%}
        {%- endfor -%} 
        {%- do dict_attributs_dim.update({tab["nom"]: list_attributs_dim}) -%}
    {%- endif -%}   
{%- endfor -%} 

{%- if ix_con != None -%}
    {%- for idx in ix_con -%}

        {%- set result_con = run_query("SELECT MIN(h3_get_resolution(hex_id::h3index)), MAX(h3_get_resolution(hex_id::h3index)) FROM " + list_tab[idx]["nom"]) -%}
        {%- if execute -%}
            {%- set res_min_con = result_con.columns[0].values()[0] -%}
            {%- set res_max_con = result_con.columns[1].values()[0] -%}
        {%- else -%}
            {% set res_min_con = 0 -%}
            {% set res_max_con = 0 -%}
        {%- endif -%}

        {%- set jointure_adaptative = ['jointure_adaptative+',list_tab[ix_ind]["nom"],"+",list_tab[idx]["nom"],"+",res_min_con,"+",res_max_con,"+link_tab_",list_tab[ix_ind]["nom"].split('.')[-1],"_",list_tab[idx]["nom"].split('.')[-1]]|join("") -%}
        {%- do list_jointures.append({list_tab[ix_ind]["nom"]:'!hex_id', jointure_adaptative:'hex_id_src'}) -%}
        {%- do list_jointures.append({list_tab[idx]["nom"]:'!hex_id', jointure_adaptative:'hex_id_tar'}) -%}
    {%- endfor -%} 
{%- endif -%}  

{%- if ix_dim != None -%}
    {%- for idx in ix_dim -%}

        {%- set result_dim = run_query("SELECT MIN(h3_get_resolution(hex_id::h3index)), MAX(h3_get_resolution(hex_id::h3index)) FROM " + list_tab[idx]["nom"]) -%}
        {%- if execute -%}
            {%- set res_min_dim = result_dim.columns[0].values()[0] -%}
            {%- set res_max_dim = result_dim.columns[1].values()[0] -%}
        {%- else -%}
            {% set res_min_dim = 0 -%}
            {% set res_max_dim = 0 -%}
        {%- endif -%}

        {%- if (res_min_dim == res) and (res_max_dim == res) -%}
            {%- do list_jointures_dim.append({cte:'!hex_id', list_tab[idx]["nom"]:'!hex_id'}) -%}
        {%- elif res_min_dim >= res -%}
            {%- do list_jointures_dim.append({cte:'!hex_id', list_tab[idx]["nom"]:['!hex_id_parent',var("sep"),res]|join("")}) -%}
        {%- elif res_max_dim <= res -%}
            {%- do list_jointures_dim.append({list_tab[idx]["nom"]:'', ['h3_to_children+',list_tab[idx]["nom"],"+",res]|join(""):''}) -%} 
            {%- do list_jointures_dim.append({cte:'hex_id', list_tab[idx]["nom"]:['!hex_id_children',var("sep")]|join("")}) -%}
        {%- else -%}
            {%- do list_jointures_dim.append({list_tab[idx]["nom"]:'', ['h3_to_children+',list_tab[idx]["nom"],"+",res]|join(""):''}) -%}
            {%- do list_jointures_dim.append({cte:'hex_id', list_tab[idx]["nom"]:['!hex_id',var("sep"),res]|join("")}) -%} 
        {%- endif -%}  
    {%- endfor -%} 
{%- endif -%}  

{%- if date_attributs -%}
    {%- do dict_attributs.update({var("nom_tab_date"): date_attributs}) -%}
    {%- for item in date_jointures -%}
        {%- do list_jointures.append(item) -%}
    {%- endfor -%} 
{%- endif -%}
{%- if date_attributs_dim -%}
    {%- do dict_attributs_dim.update({var("nom_tab_date"): date_attributs_dim}) -%}
    {%- for item in date_jointures_dim -%}
        {%- do list_jointures_dim.append(item) -%}
    {%- endfor -%} 
{%- endif -%}


{% if 'dimension' in list_statut or ('context' in  list_statut and json_agg!={}) -%}WITH {% endif -%}{{select_statement(dict_attributs=dict_attributs, list_jointures=list_jointures, name_of_the_table="t1", mode=type_t1)}}{%- if 'dimension' in  list_statut and 'context' in  list_statut and json_agg!={}-%},{%- endif %}
{%- if 'context' in list_statut and json_agg!={} %}

{% if 'dimension' in  list_statut -%}t2 AS ({%- endif -%}SELECT 
    hex_id, {{''}}
    {% for i in range(groupby_sans_json_agg|length) -%}
        {{groupby_sans_json_agg[i]}}{{", "}}
    {% endfor -%}
        JSON_AGG(json_build_object({%- for key in json_agg[json_keys[0]] -%}{{"'"+key+"'"}}, {{json_agg[json_keys[0]][key]}}{%- if not loop.last %}, {% endif -%}{%- endfor -%})) AS {{json_keys[0]}}
    FROM t1
    GROUP BY hex_id, {{''}}
    {%- for i in range(groupby_sans_json_agg|length) -%}
        {{groupby_sans_json_agg[i]}}{%- if not loop.last -%}{{", "}}{%- endif -%}
    {%- endfor %}  
    ORDER BY {{''}}
    {%- for i in range(groupby_sans_json_agg|length) -%}
        {{groupby_sans_json_agg[i]}}{%- if not loop.last -%}{{", "}}{%- endif -%}
    {%- endfor -%}{% if 'dimension' in  list_statut -%}){%- endif %}
{%- endif %}

{% if 'dimension' in  list_statut -%}
    {#- Attributs -#}
    {{select_statement(dict_attributs=dict_attributs_dim, list_jointures=list_jointures_dim, name_of_the_table="t3", mode="tab", display_jointures=false, display_sortby=false)}}

    {%- for dict in list_jointures_dim -%}
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
                    {%- if list[0] == "h3_to_children" -%}
                        {%- do replace_item(table_name,["h3_to_children(",list[1],".hex_id::h3index,",list[2],") AS children_",list[1].split(".")[-1]]|join(""),2*i+j) -%}
                        {%- do replace_item(alias,"children_"+list[1].split(".")[-1],2*i+j) -%}
                    {%- endif -%}
            {%- endif -%}   
        {%- endfor -%}  
    {%- endfor -%}

    {#- Jointures #}
    FROM 
    {%- for key, value in dict_attributs_dim.items() %}
        {{key}}{%- if not loop.last -%}, {%- endif -%}
    {%- endfor %}
    {%- for i in range((table_name|length)//2) %}
            {%- if table_name[2*i] not in table_name[:2*i] -%}
                {%- set a = 2*i -%}
                {%- set b = 2*i+1 -%}
            {%- else %}
                {%- set a = 2*i+1 -%}
                {%- set b = 2*i -%}
            {%- endif -%} 
            {%- set joint_value_a = joint_value[a]|replace("!","") -%}
            {%- set joint_value_b = joint_value[b]|replace("!","") -%}
            {%- if (joint_value_a=="") and (joint_value_b=="") %}
        JOIN {{table_name[b]}} {{on_statement(a,b,var("sep"),table_name,alias,joint_value)}}
            {%- endif -%} 
    {%- endfor %}
    WHERE {{''}}
    {%- for i in range((table_name|length)//2) %}
            {%- if table_name[2*i] not in table_name[:2*i] %}
                {%- set a = 2*i -%}
                {%- set b = 2*i+1 -%}
            {%- else %}
                {%- set a = 2*i+1 -%}
                {%- set b = 2*i -%}
            {%- endif -%} 
            {%- set joint_value_a = joint_value[a]|replace("!","") -%}
            {%- set joint_value_b = joint_value[b]|replace("!","") -%}
            {%- if not ((joint_value_a=="") and (joint_value_b=="")) -%}
        {{on_statement(a,b,var("sep"),table_name,alias,joint_value,where=true)}}{%- if not loop.last %}{{" AND "}}{%- endif -%} 
            {%- endif -%} 
    {%- endfor -%}

    {#- Group By #}
    {{select_statement(dict_attributs=dict_attributs_dim, list_jointures=list_jointures_dim, name_of_the_table="t3", mode="tab", display_attributs=false, display_jointures=false)}}
{%- endif -%}

{#- Post-hooks -#}
{#- Index -#}
{%- set index_hook = ["CREATE INDEX IF NOT EXISTS ix_",schema,"_",name_of_the_table,"_hex_id ON ",
    schema,".",name_of_the_table,' USING btree
    (hex_id COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;']|join("") -%}

{#- ESRI -#}
{%- set esri_hook = ["ALTER TABLE IF EXISTS ",schema,".",name_of_the_table,"
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