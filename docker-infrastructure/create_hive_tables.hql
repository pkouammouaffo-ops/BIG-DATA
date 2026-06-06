-- ============================================================
-- CHU DATA WAREHOUSE - CRÉATION SCHÉMA HIVE (via beeline)
-- Base : dwh_chu | HDFS : hdfs://chu-namenode:9000/data/gold
-- ============================================================

CREATE DATABASE IF NOT EXISTS dwh_chu
  COMMENT 'Data Warehouse CHU - Modèle en constellation - Architecture Médaillon';

USE dwh_chu;

-- ─────────────────────────────────────────────────────────────
-- DIMENSION 1 : DIM_TEMPS
-- ─────────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS dwh_chu.dim_temps (
    temps_sk          INT     COMMENT 'Clé de substitution (YYYYMMDD)',
    date_complete     DATE    COMMENT 'Date complète ex: 2024-06-01',
    jour              INT     COMMENT 'Numéro du jour dans le mois (1-31)',
    mois              INT     COMMENT 'Numéro du mois (1-12)',
    trimestre         INT     COMMENT 'Trimestre (1-4)',
    annee             INT     COMMENT 'Année (ex: 2024)',
    semaine_annee     INT     COMMENT 'Numéro de semaine ISO (1-53)',
    jour_semaine      INT     COMMENT 'Jour dans la semaine (1=Lundi ... 7=Dimanche)',
    libelle_jour      STRING  COMMENT 'Nom du jour ex: Lundi',
    libelle_mois      STRING  COMMENT 'Nom du mois ex: Janvier',
    libelle_trimestre STRING  COMMENT 'Libellé trimestre ex: T1 2024',
    est_weekend       BOOLEAN COMMENT 'True si samedi ou dimanche',
    est_jour_ferie    BOOLEAN COMMENT 'True si jour férié en France',
    libelle_jour_ferie STRING  COMMENT 'Nom du jour férié si applicable',
    est_vacances      BOOLEAN COMMENT 'True si en période de vacances scolaires',
    saison            STRING  COMMENT 'Saison : Hiver / Printemps / Été / Automne'
)
COMMENT 'Dimension temporelle - Calendrier 2010 à 2030'
STORED AS PARQUET
LOCATION 'hdfs://chu-namenode:9000/data/gold/dimensions/dim_temps'
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'scd_type'='SCD Type 1');

-- ─────────────────────────────────────────────────────────────
-- DIMENSION 2 : DIM_PATIENT
-- ─────────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS dwh_chu.dim_patient (
    patient_sk              INT     COMMENT 'Clé de substitution (séquentielle)',
    id_patient_hash         STRING  COMMENT 'SHA-256 de id_patient original (RGPD)',
    date_naissance          DATE    COMMENT 'Date de naissance du patient',
    annee_naissance         INT     COMMENT 'Année de naissance (pour calcul âge)',
    sexe                    STRING  COMMENT 'Sexe : M / F / I',
    code_postal             STRING  COMMENT 'Code postal de résidence (5 chiffres)',
    ville                   STRING  COMMENT 'Ville de résidence',
    departement             STRING  COMMENT 'Numéro de département (01-976)',
    region                  STRING  COMMENT 'Région administrative',
    tranche_age             STRING  COMMENT 'Tranche: <18 / 18-39 / 40-64 / 65-79 / 80+',
    mutuelle_adherent       BOOLEAN COMMENT 'True si patient a une mutuelle',
    nom_mutuelle            STRING  COMMENT 'Nom de la mutuelle si applicable',
    date_debut_validite     DATE    COMMENT 'Date début de validité (SCD Type 2)',
    date_fin_validite       DATE    COMMENT 'Date fin de validité (NULL si courant)',
    est_courant             BOOLEAN COMMENT 'True si version courante du patient'
)
COMMENT 'Dimension patient pseudonymisée (SHA-256) - SCD Type 2'
STORED AS PARQUET
LOCATION 'hdfs://chu-namenode:9000/data/gold/dimensions/dim_patient'
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'scd_type'='SCD Type 2', 'rgpd_compliant'='true');

-- ─────────────────────────────────────────────────────────────
-- DIMENSION 3 : DIM_ETABLISSEMENT
-- ─────────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS dwh_chu.dim_etablissement (
    etablissement_sk    INT     COMMENT 'Clé de substitution',
    finess              STRING  COMMENT 'Numéro FINESS (9 chiffres)',
    raison_sociale      STRING  COMMENT 'Dénomination officielle',
    categorie_code      STRING  COMMENT 'Code catégorie FINESS',
    categorie_libelle   STRING  COMMENT 'Libellé catégorie',
    statut_juridique    STRING  COMMENT 'Public / ESPIC / Privé lucratif',
    capacite_lits       INT     COMMENT 'Nombre de lits autorisés',
    adresse             STRING  COMMENT 'Adresse postale complète',
    code_postal         STRING  COMMENT 'Code postal (5 chiffres)',
    ville               STRING  COMMENT 'Ville',
    departement         STRING  COMMENT 'Numéro de département',
    region              STRING  COMMENT 'Région administrative',
    telephone           STRING  COMMENT 'Numéro de téléphone',
    activites           STRING  COMMENT 'Activités autorisées',
    latitude            DOUBLE  COMMENT 'Latitude GPS (WGS84)',
    longitude           DOUBLE  COMMENT 'Longitude GPS (WGS84)',
    est_actif           BOOLEAN COMMENT 'True si établissement toujours actif'
)
COMMENT 'Dimension établissements de santé - Référentiel FINESS'
STORED AS PARQUET
LOCATION 'hdfs://chu-namenode:9000/data/gold/dimensions/dim_etablissement'
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'scd_type'='SCD Type 1', 'source'='FINESS');

-- ─────────────────────────────────────────────────────────────
-- DIMENSION 4 : DIM_DIAGNOSTIC
-- ─────────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS dwh_chu.dim_diagnostic (
    diagnostic_sk       INT     COMMENT 'Clé de substitution',
    code_cim10          STRING  COMMENT 'Code CIM-10 ex: J18.9',
    libelle_court       STRING  COMMENT 'Libellé court',
    libelle_long        STRING  COMMENT 'Libellé complet',
    chapitre_code       STRING  COMMENT 'Code chapitre CIM-10',
    chapitre_libelle    STRING  COMMENT 'Libellé chapitre',
    groupe_code         STRING  COMMENT 'Code groupe',
    groupe_libelle      STRING  COMMENT 'Libellé groupe',
    type_diagnostique   STRING  COMMENT 'DP=Principal / DR=Relié / DAS=Associé',
    severite            INT     COMMENT 'Niveau de sévérité 1-5',
    est_chronique       BOOLEAN COMMENT 'True si maladie chronique',
    est_infectieux      BOOLEAN COMMENT 'True si maladie infectieuse',
    est_invalidant      BOOLEAN COMMENT 'True si reconnue comme invalidante'
)
COMMENT 'Dimension diagnostics - Nomenclature CIM-10'
STORED AS PARQUET
LOCATION 'hdfs://chu-namenode:9000/data/gold/dimensions/dim_diagnostic'
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'scd_type'='SCD Type 1', 'nomenclature'='CIM-10');

-- ─────────────────────────────────────────────────────────────
-- DIMENSION 5 : DIM_PROFESSIONNEL
-- ─────────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS dwh_chu.dim_professionnel (
    professionnel_sk        INT     COMMENT 'Clé de substitution',
    identifiant_rpps        STRING  COMMENT 'Identifiant RPPS (11 chiffres)',
    identifiant_adeli       STRING  COMMENT 'Identifiant ADELI si applicable',
    profession_code         STRING  COMMENT 'Code profession ex: 10 = Médecin',
    profession_libelle      STRING  COMMENT 'Libellé profession',
    specialite_code         STRING  COMMENT 'Code spécialité médicale',
    specialite_libelle      STRING  COMMENT 'Libellé spécialité',
    mode_exercice           STRING  COMMENT 'LIBERAL / SALARIE / BENEVOLE',
    secteur_activite        STRING  COMMENT 'MCO / SSR / HAD / PSY / AMBULATOIRE',
    departement_exercice    STRING  COMMENT 'Département exercice',
    region_exercice         STRING  COMMENT 'Région exercice',
    annee_premiere_install  INT     COMMENT 'Année de première installation',
    date_debut_validite     DATE    COMMENT 'Date début validité (SCD Type 2)',
    date_fin_validite       DATE    COMMENT 'Date fin validité (NULL si courant)',
    est_courant             BOOLEAN COMMENT 'True si enregistrement courant'
)
COMMENT 'Dimension professionnels de santé - Référentiels RPPS et ADELI'
STORED AS PARQUET
LOCATION 'hdfs://chu-namenode:9000/data/gold/dimensions/dim_professionnel'
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'scd_type'='SCD Type 2', 'source'='RPPS / ADELI');

-- ─────────────────────────────────────────────────────────────
-- DIMENSION 6 : DIM_GEOGRAPHIE
-- ─────────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS dwh_chu.dim_geographie (
    geographie_sk       INT     COMMENT 'Clé de substitution',
    code_postal         STRING  COMMENT 'Code postal (5 chiffres)',
    code_commune_insee  STRING  COMMENT 'Code INSEE commune',
    nom_commune         STRING  COMMENT 'Nom de la commune',
    code_departement    STRING  COMMENT 'Code département (01-976)',
    nom_departement     STRING  COMMENT 'Nom du département',
    code_region         STRING  COMMENT 'Code région INSEE',
    nom_region          STRING  COMMENT 'Nom de la région',
    nom_region_court    STRING  COMMENT 'Nom court de la région',
    zone_urbaine        STRING  COMMENT 'RURALE / SEMI-URBAINE / URBAINE / METROPOLE',
    population_2024     INT     COMMENT 'Population estimée 2024',
    densite_hab_km2     DOUBLE  COMMENT 'Densité de population',
    latitude            DOUBLE  COMMENT 'Latitude centroïde commune',
    longitude           DOUBLE  COMMENT 'Longitude centroïde commune',
    dom_tom             BOOLEAN COMMENT 'True si DOM-TOM'
)
COMMENT 'Dimension géographique France - Code postal vers région'
STORED AS PARQUET
LOCATION 'hdfs://chu-namenode:9000/data/gold/dimensions/dim_geographie'
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'scd_type'='SCD Type 1', 'source'='La Poste + INSEE');

-- ─────────────────────────────────────────────────────────────
-- DIMENSION 7 : DIM_QUESTION
-- ─────────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS dwh_chu.dim_question (
    question_sk         INT     COMMENT 'Clé de substitution (1-7)',
    code_question       STRING  COMMENT 'Code court ex: Q1 / Q2 / GLOBAL',
    libelle_court       STRING  COMMENT 'Intitulé court',
    libelle_long        STRING  COMMENT 'Intitulé complet',
    domaine             STRING  COMMENT 'ACCUEIL / SOINS / CHAMBRE / ALIMENTATION / GLOBAL',
    est_indicateur_iqss BOOLEAN COMMENT 'True si question utilisée dans les IQSS HAS',
    ponderation         DOUBLE  COMMENT 'Poids dans le score global ESATIS'
)
COMMENT 'Dimension questions ESATIS - 7 dimensions de satisfaction patient'
STORED AS PARQUET
LOCATION 'hdfs://chu-namenode:9000/data/gold/dimensions/dim_question'
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'scd_type'='SCD Type 1', 'source'='HAS ESATIS');

-- ─────────────────────────────────────────────────────────────
-- FAIT 1 : FAIT_CONSULTATION
-- ─────────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS dwh_chu.fait_consultation (
    consultation_sk      BIGINT  COMMENT 'Clé de substitution du fait',
    temps_sk             INT     COMMENT 'FK dim_temps (YYYYMMDD)',
    patient_sk           INT     COMMENT 'FK dim_patient',
    professionnel_sk     INT     COMMENT 'FK dim_professionnel',
    etablissement_sk     INT     COMMENT 'FK dim_etablissement',
    diagnostic_sk        INT     COMMENT 'FK dim_diagnostic',
    geographie_sk        INT     COMMENT 'FK dim_geographie',
    duree_consultation   INT     COMMENT 'Durée en minutes',
    cout_acte            DOUBLE  COMMENT 'Coût de acte en euros',
    montant_rembourse    DOUBLE  COMMENT 'Part remboursée AM',
    montant_reste_charge DOUBLE  COMMENT 'Reste à charge patient',
    type_consultation    STRING  COMMENT 'CABINET / TELECONSULTATION / DOMICILE / URGENCE',
    mode_consultation    STRING  COMMENT 'PROGRAMMEE / NON_PROGRAMMEE',
    secteur_tarif        STRING  COMMENT 'Secteur tarifaire : S1 / S2 / S3',
    nb_diagnostics_assoc INT     COMMENT 'Nombre de diagnostics associés',
    est_premiere_consult BOOLEAN COMMENT 'True si première consultation'
)
COMMENT 'Fait consultations - 1 ligne = 1 acte médical'
PARTITIONED BY (annee INT, mois INT)
STORED AS PARQUET
LOCATION 'hdfs://chu-namenode:9000/data/gold/faits/fait_consultation'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- ─────────────────────────────────────────────────────────────
-- FAIT 2 : FAIT_HOSPITALISATION
-- ─────────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS dwh_chu.fait_hospitalisation (
    hospitalisation_sk  BIGINT  COMMENT 'Clé de substitution du fait',
    temps_entree_sk     INT     COMMENT 'FK dim_temps date entrée',
    temps_sortie_sk     INT     COMMENT 'FK dim_temps date sortie',
    patient_sk          INT     COMMENT 'FK dim_patient',
    etablissement_sk    INT     COMMENT 'FK dim_etablissement',
    diagnostic_sk       INT     COMMENT 'FK dim_diagnostic',
    geographie_sk       INT     COMMENT 'FK dim_geographie',
    duree_sejour        INT     COMMENT 'Durée du séjour en jours',
    mode_entree         STRING  COMMENT '1=Domicile / 2=Mutation / 3=Transfert / 7=Urgences',
    mode_sortie         STRING  COMMENT '1=Domicile / 2=Mutation / 3=Transfert / 9=Décès',
    type_sejour         STRING  COMMENT 'MCO / SSR / HAD / PSY',
    ghm_code            STRING  COMMENT 'Code GHM',
    ghm_libelle         STRING  COMMENT 'Libellé GHM',
    type_ghm            STRING  COMMENT 'C=Chirurgie / M=Médecine / Z=Séance / K=Actes',
    salle_code          STRING  COMMENT 'Code salle / unité',
    cout_sejour         DOUBLE  COMMENT 'Coût total estimé du séjour',
    est_deces           BOOLEAN COMMENT 'True si décès pendant le séjour'
)
COMMENT 'Fait hospitalisations - 1 ligne = 1 séjour hospitalier'
PARTITIONED BY (annee INT, mois INT)
STORED AS PARQUET
LOCATION 'hdfs://chu-namenode:9000/data/gold/faits/fait_hospitalisation'
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'source'='ATIH PMSI');

-- ─────────────────────────────────────────────────────────────
-- FAIT 3 : FAIT_DECES
-- ─────────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS dwh_chu.fait_deces (
    deces_sk               BIGINT  COMMENT 'Clé de substitution du fait',
    temps_deces_sk         INT     COMMENT 'FK dim_temps date de décès',
    patient_sk             INT     COMMENT 'FK dim_patient',
    diagnostic_sk          INT     COMMENT 'FK dim_diagnostic cause principale',
    geographie_deces_sk    INT     COMMENT 'FK dim_geographie lieu de décès',
    geographie_domicile_sk INT     COMMENT 'FK dim_geographie lieu de résidence',
    age_au_deces           INT     COMMENT 'Âge au moment du décès',
    tranche_age_deces      STRING  COMMENT '<1 / 1-17 / 18-39 / 40-64 / 65-79 / 80+',
    sexe                   STRING  COMMENT 'M / F / I',
    lieu_deces             STRING  COMMENT 'HOPITAL / DOMICILE / EHPAD / RUE / AUTRE',
    cause_initiale_cim10   STRING  COMMENT 'Code CIM-10 cause initiale',
    cause_directe_cim10    STRING  COMMENT 'Code CIM-10 cause directe',
    est_mort_subite        BOOLEAN COMMENT 'True si décès subit ou accidentel',
    est_deces_maternel     BOOLEAN COMMENT 'True si décès maternel'
)
COMMENT 'Fait décès - 1 ligne = 1 décès (source INSEE)'
PARTITIONED BY (annee INT, mois INT)
STORED AS PARQUET
LOCATION 'hdfs://chu-namenode:9000/data/gold/faits/fait_deces'
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'source'='INSEE');

-- ─────────────────────────────────────────────────────────────
-- FAIT 4 : FAIT_SATISFACTION
-- ─────────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS dwh_chu.fait_satisfaction (
    satisfaction_sk     BIGINT  COMMENT 'Clé de substitution du fait',
    temps_sk            INT     COMMENT 'FK dim_temps période enquête',
    etablissement_sk    INT     COMMENT 'FK dim_etablissement',
    question_sk         INT     COMMENT 'FK dim_question',
    score_brut          DOUBLE  COMMENT 'Score brut ESATIS (0.0 à 100.0)',
    score_classe        STRING  COMMENT 'A (>80) / B (60-80) / C (<60)',
    rang_national       INT     COMMENT 'Rang national',
    rang_regional       INT     COMMENT 'Rang régional',
    nb_repondants       INT     COMMENT 'Nombre de répondants',
    taux_participation  DOUBLE  COMMENT 'Taux de participation',
    millesime           INT     COMMENT 'Millésime ESATIS ex: 2017',
    type_enquete        STRING  COMMENT 'ESATIS48H / ESATISCA / HPP / RCP / DPA',
    secteur_sejour      STRING  COMMENT 'MCO / SSR / HAD',
    est_iqss            BOOLEAN COMMENT 'True si indicateur IQSS HAS'
)
COMMENT 'Fait satisfaction patients - Scores ESATIS'
PARTITIONED BY (annee INT)
STORED AS PARQUET
LOCATION 'hdfs://chu-namenode:9000/data/gold/faits/fait_satisfaction'
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'source'='HAS ESATIS / IQSS');

-- ─────────────────────────────────────────────────────────────
-- VÉRIFICATION FINALE
-- ─────────────────────────────────────────────────────────────
SHOW TABLES IN dwh_chu;
