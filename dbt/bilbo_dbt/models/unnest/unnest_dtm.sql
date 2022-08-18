{#- Modèle permettant de d-exploser un json en plusieurs lignes -#}

{{unnest("bilbo.dm_feux_mos_nc_annee_8_dbt", "classe_n3", {"idfeu":"int", "classe":"text", "surface":"double precision"})}}