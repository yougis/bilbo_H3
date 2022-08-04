{%- set list_tab = [{"nom":"bilbo.faits_feux_nc_6a13","statut":"indicateur","attributs":["!objectid", "!date+begdate+year+annee", "sum_area_adaptatif+surface", "!geom+hex_id+8+geometry"]},{"nom":"bilbo.faits_mos_nc_6a13_v2","statut":"context","attributs":["!l_2014_n3"]}, {"nom":"bilbo.dim_communes_8","statut":"dimension","attributs":["nom AS commune"]}, {"nom":"bilbo.dim_provinces_8","statut":"dimension","attributs":["nom AS province"]}] -%}
{%- set name_of_the_table = "dm_feux_mos_nc_annee_8_dbt" -%}
{%- set res = 8 -%}
{%- set json_agg = {"classe_n3":{"idfeu":"objectid", "classe":"l_2014_n3", "surface":"surface"}} -%}
{%- set set_index = true -%}

{{dtm(list_tab, name_of_the_table, res, json_agg, set_index)}}