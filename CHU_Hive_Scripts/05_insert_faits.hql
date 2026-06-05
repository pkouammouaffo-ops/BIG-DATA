-- ============================================================
--  CHU — Script 5 : Alimentation des tables de faits
-- ============================================================
USE chu_dw;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- FACT_SATISFACTION (partitionné par annee)
INSERT OVERWRITE TABLE fact_satisfaction
PARTITION (annee)
SELECT
    t.date_id,
    e.etab_sk,
    g.geo_sk,
    q.question_sk,
    s.note                  AS note_satisfaction,
    s.nb_repondants         AS nb_avis,
    s.annee
FROM stg_satisfaction s
JOIN dim_temps        t ON t.annee = s.annee AND t.jour = 1 AND t.mois = 1
JOIN dim_etablissement e ON e.nom_etablissement = s.etab_id
JOIN dim_geo          g ON g.region IS NOT NULL
JOIN dim_question     q ON q.libelle_question = s.question_id;

-- FACT_DECES (partitionné par annee)
INSERT OVERWRITE TABLE fact_deces
PARTITION (annee)
SELECT
    t.date_id,
    g.geo_sk,
    d.nb_deces,
    d.annee
FROM stg_deces d
JOIN dim_temps  t ON t.annee = d.annee AND t.jour = 1 AND t.mois = 1
JOIN dim_geo    g ON g.code_insee = d.code_insee;

