-- ============================================================
--  CHU — Schéma en constellation
--  Script 1 : Création des tables Hive
--  Base cible : chu_dw
-- ============================================================

CREATE DATABASE IF NOT EXISTS chu_dw
  COMMENT 'Entrepôt de données CHU - Cloud Healthcare Unit'
  LOCATION '/user/hive/warehouse/chu_dw.db';

USE chu_dw;


-- ============================================================
-- DIMENSIONS
-- ============================================================

-- ---- DIM_TEMPS (générée) ------------------------------------
CREATE TABLE IF NOT EXISTS dim_temps (
    date_id     INT        COMMENT 'Clé surrogate YYYYMMDD',
    jour        INT        COMMENT 'Jour du mois (1-31)',
    mois        INT        COMMENT 'Mois (1-12)',
    trimestre   INT        COMMENT 'Trimestre (1-4)',
    annee       INT        COMMENT 'Année'
)
COMMENT 'Dimension temps — générée'
STORED AS ORC
TBLPROPERTIES ('transactional'='false');


-- ---- DIM_PATIENT (src : PostgreSQL) -------------------------
CREATE TABLE IF NOT EXISTS dim_patient (
    patient_sk      INT        COMMENT 'Clé surrogate',
    patient_id_pseudo STRING   COMMENT 'Identifiant pseudonymisé SHA-256 (RGPD)',
    sexe            STRING     COMMENT 'Sexe',
    age             INT        COMMENT 'Âge',
    tranche_age     STRING     COMMENT 'Tranche d\'âge (dérivé)',
    ville           STRING     COMMENT 'Ville'
)
COMMENT 'Dimension patient — source PostgreSQL'
STORED AS ORC
TBLPROPERTIES ('transactional'='false');


-- ---- DIM_PROFESSIONNEL (src : PostgreSQL) -------------------
CREATE TABLE IF NOT EXISTS dim_professionnel (
    prof_sk         INT        COMMENT 'Clé surrogate',
    prof_id_source  STRING     COMMENT 'Identifiant source',
    profession      STRING     COMMENT 'Profession',
    specialite      STRING     COMMENT 'Spécialité'
)
COMMENT 'Dimension professionnel de santé — source PostgreSQL'
STORED AS ORC
TBLPROPERTIES ('transactional'='false');


-- ---- DIM_DIAGNOSTIC (src : PostgreSQL) ----------------------
CREATE TABLE IF NOT EXISTS dim_diagnostic (
    diag_sk     INT        COMMENT 'Clé surrogate',
    code_diag   STRING     COMMENT 'Code diagnostic source',
    libelle     STRING     COMMENT 'Libellé diagnostic',
    code_cim10  STRING     COMMENT 'Code CIM-10 (référentiel externe)',
    categorie   STRING     COMMENT 'Catégorie CIM-10'
)
COMMENT 'Dimension diagnostic — source PostgreSQL + référentiel CIM-10'
STORED AS ORC
TBLPROPERTIES ('transactional'='false');


-- ---- DIM_ETABLISSEMENT (src : CSV) --------------------------
CREATE TABLE IF NOT EXISTS dim_etablissement (
    etab_sk             INT     COMMENT 'Clé surrogate',
    nom_etablissement   STRING  COMMENT 'Nom de l\'établissement',
    type_etablissement  STRING  COMMENT 'Type (CHU, CH, Clinique...)',
    region              STRING  COMMENT 'Région',
    departement         STRING  COMMENT 'Département'
)
COMMENT 'Dimension établissement — source CSV'
STORED AS ORC
TBLPROPERTIES ('transactional'='false');


-- ---- DIM_GEO (src : FTP) ------------------------------------
CREATE TABLE IF NOT EXISTS dim_geo (
    geo_sk      INT     COMMENT 'Clé surrogate',
    code_insee  STRING  COMMENT 'Code INSEE commune',
    commune     STRING  COMMENT 'Commune',
    departement STRING  COMMENT 'Département',
    region      STRING  COMMENT 'Région'
)
COMMENT 'Dimension géographique — source FTP INSEE'
STORED AS ORC
TBLPROPERTIES ('transactional'='false');


-- ---- DIM_QUESTION (src : FTP) --------------------------------
CREATE TABLE IF NOT EXISTS dim_question (
    question_sk      INT     COMMENT 'Clé surrogate',
    libelle_question STRING  COMMENT 'Libellé de la question de satisfaction',
    dimension        STRING  COMMENT 'Dimension évaluée',
    ordre            INT     COMMENT 'Ordre d\'affichage'
)
COMMENT 'Dimension question satisfaction — source FTP'
STORED AS ORC
TBLPROPERTIES ('transactional'='false');


-- ============================================================
-- TABLES DE FAITS
-- ============================================================

-- ---- FACT_CONSULTATION (src : PostgreSQL) -------------------
-- Besoins couverts : 1, 2, 6
CREATE TABLE IF NOT EXISTS fact_consultation (
    date_id         INT     COMMENT 'FK -> dim_temps',
    patient_sk      INT     COMMENT 'FK -> dim_patient',
    etab_sk         INT     COMMENT 'FK -> dim_etablissement',
    diag_sk         INT     COMMENT 'FK -> dim_diagnostic',
    prof_sk         INT     COMMENT 'FK -> dim_professionnel',
    nb_consultation INT     COMMENT 'Nombre de consultations',
    duree_minutes   INT     COMMENT 'Durée en minutes'
)
COMMENT 'Fait consultation — source PostgreSQL'
PARTITIONED BY (annee INT, mois INT)
CLUSTERED BY (patient_sk) INTO 16 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='false');


-- ---- FACT_HOSPITALISATION (src : CSV) -----------------------
-- Besoins couverts : 3, 4, 5
CREATE TABLE IF NOT EXISTS fact_hospitalisation (
    date_id             INT     COMMENT 'FK -> dim_temps',
    patient_sk          INT     COMMENT 'FK -> dim_patient',
    etab_sk             INT     COMMENT 'FK -> dim_etablissement',
    diag_sk             INT     COMMENT 'FK -> dim_diagnostic',
    nb_hospitalisation  INT     COMMENT 'Nombre d\'hospitalisations',
    duree_sejour        INT     COMMENT 'Durée du séjour en jours'
)
COMMENT 'Fait hospitalisation — source CSV'
PARTITIONED BY (annee INT, mois INT)
CLUSTERED BY (patient_sk) INTO 16 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='false');


-- ---- FACT_SATISFACTION (src : FTP) --------------------------
-- Besoin couvert : 8
CREATE TABLE IF NOT EXISTS fact_satisfaction (
    date_id          INT     COMMENT 'FK -> dim_temps',
    etab_sk          INT     COMMENT 'FK -> dim_etablissement',
    geo_sk           INT     COMMENT 'FK -> dim_geo',
    question_sk      INT     COMMENT 'FK -> dim_question',
    note_satisfaction DECIMAL(4,2) COMMENT 'Note moyenne (AVG — non additive)',
    nb_avis          INT     COMMENT 'Nombre d\'avis'
)
COMMENT 'Fait satisfaction patient — source FTP'
PARTITIONED BY (annee INT)
CLUSTERED BY (etab_sk) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='false');


-- ---- FACT_DECES (src : FTP INSEE) ---------------------------
-- Besoin couvert : 7
CREATE TABLE IF NOT EXISTS fact_deces (
    date_id     INT     COMMENT 'FK -> dim_temps',
    geo_sk      INT     COMMENT 'FK -> dim_geo',
    nb_deces    INT     COMMENT 'Nombre de décès'
)
COMMENT 'Fait décès — source FTP répertoire INSEE'
PARTITIONED BY (annee INT)
CLUSTERED BY (geo_sk) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='false');

-- ============================================================
-- FIN DU SCRIPT 1
-- ============================================================
