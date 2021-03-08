/*
 * SCRIPTS DE TESTS DES DONNEES GEOSTANDARDISEE
 * vien en complementdu notebook 'Outils' dans C:\Users\martin.schoreisz\git\PlaMADE\PlaMADE\src
 */

--test bdtopo juin 2019 3D
SELECT * FROM public.bdtopo_juin2019_charente LIMIT 1
--La version mise a disposition des opérateurs est en 2D !!!!!!

--pour faire le menage : 
TRUNCATE TABLE geostandardise_src.allure_national CONTINUE IDENTITY RESTRICT;
TRUNCATE TABLE geostandardise_src.route_national CONTINUE IDENTITY RESTRICT;
TRUNCATE TABLE geostandardise_src.rvt_national CONTINUE IDENTITY RESTRICT;
TRUNCATE TABLE geostandardise_src.trafic_national CONTINUE IDENTITY RESTRICT;
TRUNCATE TABLE geostandardise_src.troncon_national CONTINUE IDENTITY RESTRICT;
TRUNCATE TABLE geostandardise_src.vts_national CONTINUE IDENTITY RESTRICT;

--pour les test on a besoin d'une bdd tres permissive en geometrie
ALTER TABLE geostandardise_src.troncon_national ALTER COLUMN geom TYPE geometry

--tester l'import dans la table troncon
SELECT * FROM geostandardise_src.troncon_national WHERE codedept='084' LIMIT 1

--tests sur les SRID par departement
SELECT DISTINCT codedept, st_srid(geom) 
 FROM geostandardise_src.troncon_national
 
--test sur des doublons de géométrie
WITH dbl_dpt AS (
SELECT codedept, geom, count(geom) nbgeom
 FROM geostandardise_src.troncon_national
 GROUP BY codedept,geom
 HAVING count(geom) > 1)
SELECT codedept, count(nbgeom)
 FROM dbl_dpt
 GROUP BY codedept
 ORDER BY codedept
 
--test sur la présence de plusieurs idsource pour une geométrie
WITH check_geom AS (
 SELECT codedept, idsource, geom, count(geom) over(PARTITION BY geom) nb_geom, count(idsource) OVER (PARTITION BY idsource) nb_idsource
 FROM geostandardise_src.troncon_national
 ORDER BY idsource)
SELECT codedept, count(nb_geom) nb_geom_dbl, count(nb_idsource)
 FROM check_geom
 WHERE nb_geom > 1 OR nb_idsource >1
 GROUP  BY codedept
 
--sur un departement : 
WITH check_geom AS (
 SELECT codedept, idsource, geom, count(geom) over(PARTITION BY geom) nb_geom, count(idsource) OVER (PARTITION BY idsource) nb_idsource
 FROM geostandardise_src.troncon_national
 WHERE codedept='001'
 ORDER BY idsource)
SELECT * from check_geom WHERE nb_geom > 1 OR nb_idsource >1
 

 
 