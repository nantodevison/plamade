/*
 * SCRIPTS DE TESTS DES DONNEES GEOSTANDARDISEE
 * vien en complementdu notebook 'Outils' dans C:\Users\martin.schoreisz\git\PlaMADE\PlaMADE\src
 */

--test bdtopo juin 2019 3D
SELECT * FROM public.bdtopo_juin2019_charente LIMIT 1
--!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! La version mise a disposition des opérateurs est en 2D !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

--pour faire le menage : 
TRUNCATE TABLE geostandardise_src.troncon_national CONTINUE IDENTITY CASCADE ;
TRUNCATE TABLE geostandardise_src.allure_national CONTINUE IDENTITY CASCADE;
TRUNCATE TABLE geostandardise_src.route_national CONTINUE IDENTITY CASCADE;
TRUNCATE TABLE geostandardise_src.rvt_national CONTINUE IDENTITY CASCADE;
TRUNCATE TABLE geostandardise_src.trafic_national CONTINUE IDENTITY CASCADE;
TRUNCATE TABLE geostandardise_src.vts_national CONTINUE IDENTITY CASCADE;

--mise a jour de l'identifiant unique apres import par departement
ALTER TABLE geostandardise_src.troncon_national ADD COLUMN id_uniq_w SERIAL ;
ALTER TABLE geostandardise_src.allure_national ADD COLUMN id_uniq_w SERIAL ;
ALTER TABLE geostandardise_src.route_national ADD COLUMN id_uniq_w SERIAL ;
ALTER TABLE geostandardise_src.rvt_national ADD COLUMN id_uniq_w SERIAL ;
ALTER TABLE geostandardise_src.trafic_national ADD COLUMN id_uniq_w SERIAL ;
ALTER TABLE geostandardise_src.vts_national ADD COLUMN id_uniq_w SERIAL ;

--pour les test on a besoin d'une bdd tres permissive en geometrie
ALTER TABLE geostandardise_src.troncon_national ALTER COLUMN geom TYPE geometry

--tester l'import dans la table troncon
SELECT * FROM geostandardise_src.troncon_national WHERE codedept='035' LIMIT 1

SELECT * FROM geostandardise_src.trafic_national WHERE codedept='048' LIMIT 1

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

--recherche des idtroncon dupliques (109 valeurs, toutes dans le 39)
SELECT idtroncon
 FROM (SELECT codedept, idtroncon, idsource, geom, count(idtroncon) over(PARTITION BY idtroncon) nb_idtroncon
       FROM geostandardise_src.troncon_national
       ORDER BY idtroncon) t 
 WHERE t.nb_idtroncon>=2 AND id_troncon IS NOT null
--pour la suite du taf je supprime arbitrairement des données en doubons dans le 39, mais c'est a surveiller (la requete à supprimer original + doublons, attention, il faudrait remlpacer le 
-- idtroncon de la clause where par ogcfid)
DELETE FROM geostandardise_src.troncon_national WHERE id_uniq_w IN (SELECT id_uniq_w,idtroncon, nb_idtroncon
 FROM (SELECT id_uniq_w, codedept, idtroncon, idsource, geom, ROW_NUMBER() over(PARTITION BY idtroncon) nb_idtroncon
       FROM geostandardise_src.troncon_national
       ORDER BY idtroncon) t 
 WHERE t.nb_idtroncon>=2 AND t.idtroncon IS NOT null)
--idem pour les tables trafic, allure, rvt, vts
DELETE FROM geostandardise_src.trafic_national WHERE id_uniq_w IN (SELECT id_uniq_w
 FROM (SELECT id_uniq_w, codedept, idtroncon, ROW_NUMBER() over(PARTITION BY idtroncon) nb_idtroncon
       FROM geostandardise_src.trafic_national
       ORDER BY idtroncon) t 
 WHERE t.nb_idtroncon>=2 AND t.idtroncon IS NOT null)
 
--test sur les valeurs null de idtroncon 
select DISTINCT (codedept) FROM geostandardise_src.troncon_national WHERE idtroncon IS NULL
-- toute dans le 48 (114643)
SELECT count(*) FROM geostandardise_src.troncon_national WHERE idtroncon IS NULL
--et comme par hasard on a 114643 valeurs d'idtroncon vide dans les tables trafic, rvt, allure
SELECT count(*) FROM geostandardise_src.trafic_national WHERE idtroncon IS NULL
SELECT count(*) FROM geostandardise_src.rvt_national WHERE idtroncon IS NULL
SELECT count(*) FROM geostandardise_src.allure_national WHERE idtroncon IS NULL
SELECT count(*) FROM geostandardise_src.vts_national WHERE idtroncon IS null
-- besoin de tests : on supprime
DELETE FROM geostandardise_src.troncon_national WHERE idtroncon IS NULL
DELETE FROM geostandardise_src.trafic_national WHERE idtroncon IS NULL
DELETE FROM geostandardise_src.rvt_national  WHERE idtroncon IS NULL
DELETE FROM geostandardise_src.allure_national  WHERE idtroncon IS NULL


--verif des cles etrangeres 
--creer un index sur trable fille pour optimiser jointure
CREATE INDEX trafic_national_idtroncon_idx ON geostandardise_src.trafic_national (idtroncon);
--recherche des valeurs : on retombe sur les valeurs du 039 supprimées plus haut
SELECT *
 FROM geostandardise_src.trafic_national tn LEFT JOIN geostandardise_src.troncon_national tronc ON tn.idtroncon=tronc.idtroncon
 WHERE tronc.idtroncon IS NULL
 
--creer les vues avec les infos principale
CREATE OR REPLACE VIEW geostandardise_src.vue_trafic AS 
SELECT tn.idtroncon, tn. codedept, tn. idroute, tn. nomrueg, tn. nomrued, tn. refsource, tn. millsource, tn. idsource, tn. cbs_gitt, tn. geom, tn. listgest
       , tn2."comment", tn2.source_trafic, tn2. pcentpl, tn2. tmjavlt, tn2. tmjaplt, tn2. debitsatac, tn2. saturation 
  FROM geostandardise_src.troncon_national tn JOIN (SELECT tn2.idtroncon, tn2."comment", e.nom source_trafic, tn2. pcentpl, tn2. tmjavlt, tn2. tmjaplt, tn2. debitsatac, tn2. saturation 
                                                            FROM geostandardise_src.trafic_national tn2 LEFT JOIN geostandardise_src.enum_source_trafic e 
                                                            ON (tn2.sourcevl=e.code) ) tn2 ON tn.idtroncon=tn2.idtroncon
  
select * from geostandardise_src.vue_trafic where codedept in ('029', '022', '035', '056')
                                                            
/*===================================
 * TABLES d''enumeration 
 ==================================== */
CREATE TABLE geostandardise_src.enum_source_trafic (code varchar(2) PRIMARY KEY, nom varchar) ;
INSERT INTO geostandardise_src.enum_source_trafic VALUES ('00', 'non renseigne'), ('01', 'permanent siredo'), ('02', 'permanent linearise'), ('03', 'tournant'), ('04', 'tournant linearise'), 
 ('05', 'ponctuel'), ('06', 'ponctuel linearise'), ('07', 'model trafic'), ('08', 'classement sonore'), ('09', 'forfaitaire'), ('10', 'estimation ou calcul Cerema'), ('99', 'Autre') ;

/* =====================================================================================================================================================
 * traitements régionaux
 ===================================================================================================================================================== */

/* ----------------
 * Bretagne
 ------------------ */
 
 --vérif sur les x/y : on est bien sur du 4326, donc on les passe en 2154
 SELECT st_y((st_dumpPoints(geom)).geom) FROM geostandardise_src.troncon_national WHERE codedept='056' LIMIT 1

--mise a jour geom : 
UPDATE geostandardise_src.troncon_national
 SET geom = st_transform(st_setSrid(geom, 4326),2154)
 WHERE codedept IN ('056', '029', '022', '035')
 
--rechreche des donnees de trafic : 
SELECT * FROM geostandardise_src.trafic_national WHERE codedept IN ('056', '029', '022', '035')

DELETE FROM geostandardise_src.route_national  WHERE codedept IS NULL --IN ('056', '029', '022', '035')


/* -----------------------
 * IdF
 ------------------------*/
 
--pour export et tests attributs
SELECT pk, geom, id, prec_plani, prec_alti, nature, numero, nom_voie_g, nom_voie_d, importance, cl_admin, largeur, nom_iti, nb_voies, pos_sol, sens, alias_g, alias_d, inseecom_g, inseecom_d, codevoie_g, codevoie_d, codepost_g, codepost_d, typ_adres, bornedeb_g, bornedeb_d, bornefin_g, bornefin_d, etat, z_ini, z_fin, cat, modelcete, gi, agglo, s_trafic, an_trafic, s_vites, s_pl, tmja, q_vl_d, q_vl_e, q_vl_n, q_pl_d, q_pl_e, q_pl_n, v_vl_d, v_vl_e, v_vl_n, v_pl_d, v_pl_e, v_pl_n, allure, s_allure, enrobe, s_enrobe, bd_topo, "admin", vpkwd, vpkwe, vpkwn, vlkwd, vlkwe, vlkwn, mt, me, mn, pt, pe, pn, flownr, stronr, vu, rq
FROM donnees_sources.bruitparif
LIMIT 100

SELECT DISTINCT bd_topo 
 FROM donnees_sources.bruitparif
 
--test sur les vitesses
SELECT * 
FROM donnees_sources.bruitparif
WHERE v_pl_d != vlkwd AND v_pl_d !=0 AND vlkwd!=0

 
 