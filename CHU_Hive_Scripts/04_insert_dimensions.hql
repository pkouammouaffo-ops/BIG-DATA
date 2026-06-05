-- ============================================================
--  CHU — Script 4 : Alimentation des dimensions
--  (après Talend : données PostgreSQL déjà dans HDFS)
-- ============================================================
USE chu_dw;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- DIM_ETABLISSEMENT depuis staging CSV
INSERT OVERWRITE TABLE dim_etablissement
SELECT
    ROW_NUMBER() OVER (ORDER BY finess) AS etab_sk,
    nom,
    type,
    region,
    departement
FROM stg_etablissement
WHERE nom IS NOT NULL;

-- DIM_GEO depuis staging décès (ou référentiel INSEE séparé)
INSERT OVERWRITE TABLE dim_geo
SELECT
    ROW_NUMBER() OVER (ORDER BY code_insee) AS geo_sk,
    code_insee,
    NULL AS commune,
    SUBSTR(code_insee, 1, 2) AS departement,
    region
FROM stg_deces
GROUP BY code_insee, region;

-- DIM_QUESTION depuis staging satisfaction
INSERT OVERWRITE TABLE dim_question
SELECT
    ROW_NUMBER() OVER (ORDER BY question_id) AS question_sk,
    question_id AS libelle_question,
    NULL AS dimension,
    NULL AS ordre
FROM stg_satisfaction
GROUP BY question_id;

