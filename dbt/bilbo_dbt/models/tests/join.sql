{%- set dict_tab = [{"nom":"bilbo.faits_feux_13","statut":"indicateur","attributs":["date+begdate+!?year+annee", "array_agg+objectid+objectid", "array_length_agg+objectid+nb_feux", "!?sum_area_agg+hex_id+surface", "min_init_cap_agg+province+province", "min_init_cap_agg+commune+commune"]},{"nom":"bilbo.dm_mos2014_7_12_bis","statut":"context","attributs":["array_agg+l_2014_n1+classe"]}] -%}
{%- set tab_mask = "bilbo.dim_communes_8" -%}
{%- set name_of_the_table = "join" -%}
{%- set json_agg = {classe_n1:{"classe":"classe", "surface":"surface"}} -%}
{%- set set_index = false -%}
{%- set set_esri_requirements = false -%}

{{dtm(dict_tab,tab_mask,name_of_the_table, json_agg, set_index=false,set_esri_requirements=false)}}