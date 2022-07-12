{%- macro dtm(list_tab,tab_mask,name_of_the_table,json_agg={},set_index=false,set_esri_requirements=false) -%}
{%- set ns = namespace() -%}
   
{%- set result_mask = run_query("SELECT h3_get_resolution(hex_id::h3index) FROM " + tab_mask + " LIMIT 1") -%}
{%- if execute -%}
    {%- set res_mask = result_mask.columns[0].values()[0] -%}
{%- else -%}
    {% set res_mask = 0 -%}
{%- endif -%}


{%- set date_attributs = [] -%}
{%- set date_jointures = [] -%}

{%- set statut0 = list_tab[0]["statut"] -%}
{%- set statut1 = list_tab[1]["statut"] -%}

{%- if (statut0 == "indicateur" and statut1 == "indicateur") -%}

{%- elif ((statut0 == "indicateur" and statut1 == "context") or (statut0 == "context" and statut1 == "indicateur")) -%}

    {%- if (statut0 == "indicateur" and statut1 == "context") -%}
        {%- set ix_ind = 0 -%}
        {%- set ix_con = 1 -%}
    {%- elif (statut0 == "context" and statut1 == "indicateur") -%}
        {%- set ix_ind = 1 -%}
        {%- set ix_con = 0 -%}
    {%- endif -%}

    {%- set tab_link = var("schema_link_table")+".link_"+list_tab[ix_ind]["nom"].split(".")[1]+"_"+list_tab[ix_con]["nom"].split(".")[1] -%}
    {%- set dict_attributs = {} -%}
    {%- set list_jointures = [] -%}

    {%- set result_ind = run_query("SELECT MIN(h3_get_resolution(hex_id::h3index)), MAX(h3_get_resolution(hex_id::h3index)) FROM " + list_tab[ix_ind]["nom"]) -%}
    {%- if execute -%}
        {%- set res_min_ind = result_ind.columns[0].values()[0] -%}
        {%- set res_max_ind = result_ind.columns[1].values()[0] -%}
    {%- else -%}
        {% set res_min_ind = 0 -%}
        {% set res_max_ind = 0 -%}
    {%- endif -%}

    {%- for tab in list_tab -%}
        {%- set list_attributs = [] -%}
        {%- if tab["nom"] == list_tab[ix_ind]["nom"] -%}
            {%- if res_max_ind < res_mask -%}
                {%- do list_attributs.append(['!hex_id_children',var("sep"),res_mask]|join("")) -%}
            {%- elif  res_min_ind >= res_mask -%}  
                {%- do list_attributs.append(['!hex_id_parent',var("sep"),res_mask]|join("")) -%}
            {%- else -%}   
                {%- do list_attributs.append(['!hex_id',var("sep"),res_mask]|join("")) -%}
            {%- endif -%}   
        {%- endif -%}   
        {%- for attribut in tab["attributs"] -%}
            {%- if var("sep") in attribut -%}
                {%- set list = attribut.split(var("sep")) -%}
                {%- if list[0]=="date" -%}
                    {%- do date_attributs.append(list[2]+" AS "+list[3])-%}
                    {%- do date_jointures.append({tab["nom"]:list[1],var("nom_tab_date"):"date_id"})-%}
                {%- else -%}
                    {%- do list_attributs.append(attribut) -%}
                {%- endif -%}
            {%- else -%}
                {%- do list_attributs.append(attribut) -%}   
            {%- endif -%}   
        {%- endfor -%}  
        {%- do dict_attributs.update({tab["nom"]: list_attributs}) -%}
    {%- endfor -%} 
    {%- if date_attributs -%}
        {%- do dict_attributs.update({var("nom_tab_date"): date_attributs}) -%}
        {%- for item in date_jointures -%}
            {%- do list_jointures.append(item) -%}
        {%- endfor -%} 
    {%- endif -%}  
    {%- if res_min_ind < res_mask -%}
        {%- do list_jointures.append({list_tab[ix_ind]["nom"]:"!", ["h3_to_children",var("sep"),list_tab[ix_ind]["nom"],var("sep"),res_mask]|join(""):""}) -%} 
    {%- endif -%}  
    {%- do list_jointures.append({list_tab[ix_ind]["nom"]:"hex_id",tab_link:"hex_id_src"}) -%}
    {%- do list_jointures.append({tab_link:"!hex_id_tar",list_tab[ix_con]["nom"]:"hex_id"}) -%}

    WITH {{select_statement(dict_attributs=dict_attributs, list_jointures=list_jointures, name_of_the_table="t1", mode="cte")}}{%- if json_agg|length -%},{%- endif -%}

    {%- if json_agg|length -%}
        {%- set keys = [] -%}
        {%- set list_attributs = [] -%}
        {%- set groupby = [] -%}
        {%- for key, value in json_agg.items() -%}
            {%- do keys.append(key) -%}
        {%- endfor -%}
        {%- for tab in list_tab -%}
            {%- for attribut in tab["attributs"] -%}
                {%- set split = attribut.split(var("sep")) -%}
                {%- do list_attributs.append(split[split|length-1]) -%}
                {%- if "!" in attribut -%}
                    {%- do groupby.append(split[split|length-1]) -%}
                {% endif %}
            {%- endfor -%}
        {%- endfor %}

    t2 AS ({{"SELECT hex_id, "}}
        {%- set list = [] -%}
        {%- for i in range(groupby|length) -%}
            {%- if groupby[i] not in json_agg[keys[0]] -%}
                {%- do list.append(groupby[i]) -%}
            {%- endif -%}
        {%- endfor %}
        {%- for i in range(list|length) -%}
            {{list[i]}}{{", "}}
        {%- endfor %}
        JSON_AGG(json_build_object({%- for key in json_agg[keys[0]] -%}{{key}}, {{json_agg[keys[0]][key]}}{%- if not loop.last %}, {% endif -%}{%- endfor -%})) AS {{keys[0]}}
        FROM t1
        {{"GROUP BY hex_id, "}}
        {%- set list = [] -%}
        {%- for i in range(groupby|length) -%}
            {%- if groupby[i] not in json_agg[keys[0]] -%}
                {%- do list.append(groupby[i]) -%}
            {%- endif -%}
        {%- endfor %}
        {%- for i in range(list|length) -%}
            {{list[i]}}{%- if not loop.last -%}{{", "}}{%- endif -%}
        {%- endfor %})

SELECT *, h3_to_geo_boundary(hex_id::h3index)::geometry AS geometry FROM t2
    {%- else -%}   
SELECT *, h3_to_geo_boundary(hex_id::h3index)::geometry AS geometry FROM t1
    {%- endif -%}  
{%- endif -%}

{#- Index -#}
{%- set index_hook = ["CREATE INDEX IF NOT EXISTS ix_",schema,"_",name_of_the_table,"_hex_id ON ",
    schema,".",name_of_the_table,' USING btree
    (hex_id COLLATE pg_catalog."default" ASC NULLS LAST)
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