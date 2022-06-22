--DROP VIEW IF EXISTS bilbo.dm_feux_annee_8;
--CREATE VIEW bilbo.dm_feux_annee_8 AS

WITH tab AS (SELECT feux.hex_id, date.year AS annee, feux.objectid, l_2014_n1 AS c1, h3_cell_area(feux.hex_id::h3index) AS surface, feux.province, feux.commune 
FROM bilbo.faits_feux_13 AS feux 
    JOIN bilbo.dim_date AS date ON feux.begdate=date.date_id 
    JOIN (SELECT * FROM bilbo.dynamic_to_uniform()) AS l ON feux.hex_id=l.hex_id_src
    JOIN (SELECT hex_id, l_2014_n1 FROM bilbo.dm_mos2014_7_12_bis) AS mos ON l.hex_id_tar=mos.hex_id)

SELECT t2.classe_n1, t1.* FROM
(SELECT h3_to_parent(tab.hex_id::h3index,8)::text AS hex_id_8, tab.annee, ARRAY_AGG(DISTINCT tab.objectid) AS objectid, array_length(ARRAY_AGG(DISTINCT tab.objectid),1) AS nombre, SUM(surface) AS surface, 
MIN(INITCAP(province)) AS province, MIN(INITCAP(commune)) AS commune, MIN(com.shape_area)/1000000 AS surface_commune, (h3_to_geo_boundary(h3_to_parent(MIN(tab.hex_id)::h3index,8))::geometry) AS geometry 
FROM tab
JOIN bilbo.dim_communes_8 AS com ON h3_to_parent(tab.hex_id::h3index,8)::text=com.hex_id

GROUP BY hex_id_8, annee
ORDER BY annee, hex_id_8) AS t1

JOIN (SELECT hex_id_8, annee, JSON_AGG(json_build_object('classe',subquery.c1,'surface',subquery.surface)) AS classe_n1
      FROM (SELECT h3_to_parent(tab.hex_id::h3index,8)::text AS hex_id_8, tab.annee, c1, SUM(surface) AS surface 
            FROM tab 
            GROUP BY hex_id_8, annee, c1) AS subquery 
      GROUP BY hex_id_8, annee) AS t2
      ON t1.hex_id_8=t2.hex_id_8 AND t1.annee=t2.annee