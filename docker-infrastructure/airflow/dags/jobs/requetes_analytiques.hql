-- =============================================================================
-- LIVRABLE 2 -- 8 REQUETES ANALYTIQUES HiveQL
-- Data Warehouse CHU -- Base dwh_chu
-- Schema reel: sk_*, duree_minutes, cout_euros (T4/T5 v2)
-- =============================================================================
-- Execution : docker exec chu-hive-server beeline -u "jdbc:hive2://localhost:10000" -f /tmp/requetes_analytiques.hql
-- DBeaver   : jdbc:hive2://localhost:10000/dwh_chu
-- =============================================================================

USE dwh_chu;

-- -----------------------------------------------------------------------------
-- REQUETE 1 -- Volume de consultations par mois et par annee
-- Tables : fait_consultation  |  1 027 157 lignes
-- -----------------------------------------------------------------------------
SELECT
    annee,
    mois,
    COUNT(*)                             AS nb_consultations,
    ROUND(AVG(duree_minutes), 1)         AS duree_moy_minutes,
    ROUND(SUM(cout_euros), 2)            AS cout_total_euros,
    COUNT(DISTINCT sk_patient)           AS nb_patients_uniques
FROM fait_consultation
GROUP BY annee, mois
ORDER BY annee, mois;


-- -----------------------------------------------------------------------------
-- REQUETE 2 -- Repartition des consultations par type
-- Tables : fait_consultation
-- Remarque : type_consultation = motif de la consultation (champ reel Silver)
-- -----------------------------------------------------------------------------
SELECT
    type_consultation,
    COUNT(*)                                              AS nb_consultations,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)   AS pct_total,
    ROUND(AVG(cout_euros), 2)                            AS cout_moyen,
    ROUND(AVG(duree_minutes), 1)                         AS duree_moy_minutes,
    COUNT(DISTINCT sk_patient)                           AS nb_patients_uniques
FROM fait_consultation
GROUP BY type_consultation
ORDER BY nb_consultations DESC
LIMIT 20;


-- -----------------------------------------------------------------------------
-- REQUETE 3 -- Top 10 des diagnostics les plus frequents
-- Tables : fait_consultation JOIN dim_diagnostic (sk_diagnostic)
-- -----------------------------------------------------------------------------
SELECT
    d.code_cim10,
    d.libelle_diagnostic,
    d.chapitre_cim10,
    d.libelle_chapitre,
    COUNT(*)                             AS nb_consultations,
    ROUND(AVG(f.duree_minutes), 1)       AS duree_moy_min
FROM fait_consultation f
JOIN dim_diagnostic d ON f.sk_diagnostic = d.sk_diagnostic
WHERE f.sk_diagnostic <> -1
GROUP BY d.code_cim10, d.libelle_diagnostic, d.chapitre_cim10, d.libelle_chapitre
ORDER BY nb_consultations DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- REQUETE 4 -- Analyse des hospitalisations par annee et mois
-- Tables : fait_hospitalisation  |  1 890 lignes
-- Remarque : dim_etablissement vide (CSV non charges), analyse temporelle seule
-- -----------------------------------------------------------------------------
SELECT
    annee,
    mois,
    COUNT(*)                             AS nb_sejours,
    ROUND(AVG(duree_sejour_jours), 1)    AS duree_moy_jours,
    MAX(duree_sejour_jours)              AS duree_max_jours,
    ROUND(SUM(cout_sejour_euros), 2)     AS cout_total,
    ROUND(AVG(cout_sejour_euros), 2)     AS cout_moyen_sejour
FROM fait_hospitalisation
GROUP BY annee, mois
ORDER BY annee, mois;


-- -----------------------------------------------------------------------------
-- REQUETE 5 -- Profil demographique des patients par tranche d'age et sexe
-- Tables : dim_patient  |  99 987 lignes (SCD Type 2)
-- Remarque : requete sur dim_patient (les FK patient ne matchent pas en Silver)
-- -----------------------------------------------------------------------------
SELECT
    tranche_age,
    sexe,
    COUNT(*) AS nb_patients,
    SUM(CASE WHEN est_courant = TRUE THEN 1 ELSE 0 END) AS nb_patients_actifs
FROM dim_patient
WHERE tranche_age IS NOT NULL
GROUP BY tranche_age, sexe
ORDER BY
    CASE tranche_age
        WHEN '<18'   THEN 1
        WHEN '18-39' THEN 2
        WHEN '40-64' THEN 3
        WHEN '65-79' THEN 4
        WHEN '80+'   THEN 5
        ELSE 6
    END,
    sexe;


-- -----------------------------------------------------------------------------
-- REQUETE 6 -- Evolution annuelle des diagnostics par chapitre CIM-10
-- Tables : fait_consultation JOIN dim_diagnostic (sk_diagnostic)
-- Remarque : requete dim_temps non applicable (sk_temps_entree=-1 hospitalisation)
--            Ici on analyse par annee (partition) + chapitre diagnostique
-- -----------------------------------------------------------------------------
SELECT
    f.annee,
    d.libelle_chapitre,
    COUNT(*)                             AS nb_consultations,
    COUNT(DISTINCT f.sk_diagnostic)      AS nb_diagnostics_distincts,
    ROUND(AVG(f.duree_minutes), 1)       AS duree_moy_min
FROM fait_consultation f
JOIN dim_diagnostic d ON f.sk_diagnostic = d.sk_diagnostic
WHERE f.sk_diagnostic <> -1
GROUP BY f.annee, d.libelle_chapitre
ORDER BY f.annee, nb_consultations DESC;


-- -----------------------------------------------------------------------------
-- REQUETE 7 -- Activite par departement -- vue territoriale
-- Tables : dim_patient  |  repartition geographique des patients enregistres
-- Remarque : JOIN fait_consultation non applicable (id format mismatch)
-- -----------------------------------------------------------------------------
SELECT
    p.departement,
    g.region,
    COUNT(*) AS nb_patients,
    SUM(CASE WHEN p.est_courant = TRUE THEN 1 ELSE 0 END) AS nb_actifs
FROM dim_patient p
LEFT JOIN dim_geographie g ON p.code_postal = g.code_postal
WHERE p.departement IS NOT NULL
GROUP BY p.departement, g.region
ORDER BY nb_patients DESC
LIMIT 20;


-- -----------------------------------------------------------------------------
-- REQUETE 8 -- Performance des medecins : volume, specialite, categorie
-- Tables : fait_consultation JOIN dim_professionnel (sk_professionnel)
-- Remarque : sk_professionnel = -1 si aucune correspondance (format ID != hash)
-- -----------------------------------------------------------------------------
SELECT
    pr.specialite,
    pr.categorie_pro,
    pr.mode_exercice,
    COUNT(DISTINCT f.sk_professionnel)   AS nb_medecins,
    COUNT(*)                             AS nb_consultations,
    ROUND(COUNT(*) * 1.0
          / COUNT(DISTINCT f.sk_professionnel), 0) AS consult_par_medecin,
    ROUND(AVG(f.duree_minutes), 1)       AS duree_moy_min
FROM fait_consultation f
JOIN dim_professionnel pr ON f.sk_professionnel = pr.sk_professionnel
WHERE f.sk_professionnel <> -1
  AND pr.est_courant = TRUE
GROUP BY pr.specialite, pr.categorie_pro, pr.mode_exercice
ORDER BY nb_consultations DESC
LIMIT 20;
