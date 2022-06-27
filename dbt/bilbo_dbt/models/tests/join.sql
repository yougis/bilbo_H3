{%- set dict_attributs = {"faits_feux_13":["hex_id AS hex_id_8", "objectid", "area+hex_id+surface", "province", "commune"], "dim_date":["year AS annee"], "dm_mos2014_7_12_bis":[ "l_2014_n1 AS classe"]} -%}
{%- set list_jointures = [{"faits_feux_13":"begdate","dim_date":"date_id"},{"faits_feux_13":"hex_id","dynamic_to_uniform+":"hex_id_src"},{"dynamic_to_uniform+":"hex_id_tar","dm_mos2014_7_12_bis":"hex_id"}] -%}
{%- set name_of_the_table = "join" -%}
{%- set alias_of_the_table = "tab" -%}
{%- set granularite = 8 -%}
{%- set set_alias = false -%}
{%- set set_index = true -%}
{%- set set_esri_requirements = true -%}

{{select_statement(dict_attributs, list_jointures, name_of_the_table, alias_of_the_table, granularite, set_alias, set_index, set_esri_requirements)}}