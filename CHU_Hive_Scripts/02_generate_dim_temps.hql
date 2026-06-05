-- ============================================================
--  CHU — Script 2 : Génération de DIM_TEMPS (2015 → 2025)
-- ============================================================
USE chu_dw;

INSERT OVERWRITE TABLE dim_temps
SELECT
    CAST(DATE_FORMAT(d, 'yyyyMMdd') AS INT)   AS date_id,
    DAY(d)                                     AS jour,
    MONTH(d)                                   AS mois,
    CEIL(MONTH(d) / 3.0)                       AS trimestre,
    YEAR(d)                                    AS annee
FROM (
    SELECT DATE_ADD('2015-01-01', pos) AS d
    FROM (
        SELECT posexplode(split(space(DATEDIFF('2025-12-31','2015-01-01')), ' '))
    ) t
) dates;

-- Vérification rapide
SELECT MIN(date_id), MAX(date_id), COUNT(*) AS nb_jours FROM dim_temps;
