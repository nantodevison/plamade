/*  =====================================================================================
Scripts d'identification des découpages de vitesse
L'objetif estd'identifier les découpages de vitesse etde pouvoir proposer une géométrie type 
ponctuelle qui permette de tracer ces modifications sur la BdTopo des échéances suivantes
L'exemple est pris sur les données Orhane
 ==================================================================================== */
 
/* 
 * Préparation des données
 */
 
--ajouter l'extension pgrouting (https://pgrouting.org/)
CREATE EXTENSION pgrouting 
 
--creer les données de test : tables, colonnes, cle primaire, index
drop table if exists test_identifier_vts.troncon_aura ;
CREATE TABLE test_identifier_vts.troncon_aura AS 
 SELECT tronc.idroute,tronc.idsource, tronc.idtroncon, st_force2d(geom) geom, v.idvitesse, v.vitessevl, traf.tmjavlt
  FROM geostandardise_src.troncon_national tronc
         JOIN geostandardise_src.vts_national v ON v.idtroncon=tronc.idtroncon
         JOIN geostandardise_src.trafic_national traf ON traf.idtroncon=tronc.idtroncon
 WHERE tronc.codedept in ('001','069','038','026','069','042','063','003','073','074','007') ;
ALTER TABLE test_identifier_vts.troncon_aura ADD PRIMARY KEY (idtroncon) ;
CREATE INDEX troncon_aura_geom_idx ON test_identifier_vts.troncon_aura USING gist(geom) ;
ALTER TABLE test_identifier_vts.troncon_aura ADD COLUMN id serial ;
ALTER TABLE test_identifier_vts.troncon_aura ADD COLUMN "source" integer ; 
ALTER TABLE test_identifier_vts.troncon_aura ADD COLUMN "target" integer ;

--creer la table de topologie (https://docs.pgrouting.org/3.1/en/pgr_createTopology.htmlhttps://docs.pgrouting.org/3.1/en/pgr_createTopology.html)
SELECT  pgr_createTopology('test_identifier_vts.troncon_aura', 0.01, 'geom');

--populate table des vertex avec le nombre d'objets
SELECT  pgr_analyzeGraph('test_identifier_vts.troncon_aura',0.001, 'geom', 'id')

/* 
Requete de localisation
Etape : 
1. isoler les noeud à 2 vertex
2. ramener les lignes de chaquue cote du noeud
3. trouver les doublons d'idsource
4. identifier le noeu de références
5. virer les lignes identiques (exemple : decoupe a un departement)
6. trouver celles qui des trafic, idsource équivalent mais vitesses différentes
*/

--isoler les noeud à 2 lignes (3,5s sur PV portable bureautique i5 8thgen SSD 8Go, pour les 11 dept ci-dessus)
--CREATE TABLE test_identifier_vts.lignes_doublons_vts AS -- ne sert que temporairement pour des verifs de lignes découpées
--DROP TABLE IF EXISTS test_identifier_vts.decoupe_vitesse ;
--CREATE TABLE test_identifier_vts.decoupe_vitesse AS 
WITH 
noeud_2_lgn as(
 SELECT id, the_geom
  FROM test_identifier_vts.troncon_aura_vertices_pgr vp
  WHERE cnt=2),
lgn_noeud_2_lgn as( --trouver les lignes correspondantes
 SELECT t.*
  FROM test_identifier_vts.troncon_aura t JOIN noeud_2_lgn n ON t."source"=n.id
 UNION
 SELECT t.*
  FROM test_identifier_vts.troncon_aura t JOIN noeud_2_lgn n ON t."target"=n.id),
doublons_idsource AS ( --isoler les doublons d'idsource, car sinon on a aussi tout les noeuds BdTopo natifs avec seulement 2 lignes du a une rupture attributaire 
 SELECT t.*
  FROM (SELECT *, count(idsource) OVER (PARTITION BY idsource) nb_idsource
         FROM lgn_noeud_2_lgn) t 
  WHERE t.nb_idsource=2
  ORDER BY t.idsource) ,
noeud_ref as( --trouver le noeud commun aux deux lignes, car il va servir de géométrie ensuite
SELECT *, CASE WHEN LEAD("source") OVER w ="target" OR LEAD("target") OVER w ="target" THEN "target"
               WHEN LEAD("target") OVER w ="source" OR lEAD("source") OVER w = "source" THEN "source"
               WHEN LAG("source") OVER w ="target" OR LAG("target") OVER w ="target" THEN "target"
               WHEN LAG("target") OVER w ="source" OR LAG("source") OVER w = "source" THEN "source"
               END noeud_ref,
               LEAD(vitessevl) OVER w vitessevl2,
               LEAD(tmjavlt) OVER w tmjavlt2,
               LEAD(idsource) OVER w idsource2,
               LEAD(idtroncon) OVER w idtroncon2
 FROM doublons_idsource 
 WINDOW w AS (PARTITION BY idsource) 
 ORDER BY idsource)
SELECT n.idsource, n.idsource2, n.idtroncon, n.idtroncon2, n.vitessevl, n.vitessevl2, n.tmjavlt, n.tmjavlt2, n.noeud_ref, n2.the_geom::geometry('POINT', 2154) geom
 FROM noeud_ref n JOIN noeud_2_lgn n2 ON n.noeud_ref=n2.id
 WHERE vitessevl!=vitessevl2 AND idsource2 IS NOT NULL
 
--Visu dispo dans le projet "decoupe_vitesse" du schema qgis de la base locale plaMADE,
 --fichiers a plat ici : 
 --C:\Users\XXXX.XXXX\Box\Reprise PlaMADE-Projet Sword\Documents