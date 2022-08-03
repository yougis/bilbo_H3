{%- set list_tab = [{"nom":"bilbo.faits_feux_idp_6a13","statut":"indicateur","attributs":["!objectid", "!date+begdate+year+annee", "sum_area_adaptatif_agg+surface"]},{"nom":"bilbo.faits_mos_idp_6a13","statut":"context","attributs":["!l_2014_n3"]}, {"nom":"bilbo.dim_communes_8","statut":"dimension","attributs":["nom"]},{"nom":"bilbo.dim_provinces_8","statut":"dimension","attributs":["nom"]}] -%}
{%- set name_of_the_table = "join" -%}
{%- set res = 8 -%}
{%- set json_agg = {"classe_n3":{"idfeu":"objectid", "classe":"l_2014_n3", "surface":"surface"}} -%}
{%- set set_index = false -%}
{%- set set_esri_requirements = false -%}

{{dtm(list_tab, name_of_the_table, res, json_agg, set_index, set_esri_requirements)}}