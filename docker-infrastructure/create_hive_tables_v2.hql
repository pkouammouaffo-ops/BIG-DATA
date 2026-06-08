-- ============================================================
-- CHU DWH - DDL Hive g??n??r?? automatiquement depuis Parquet Gold
-- ============================================================

CREATE DATABASE IF NOT EXISTS dwh_chu;
USE dwh_chu;

DROP TABLE IF EXISTS dwh_chu.dim_temps;
CREATE EXTERNAL TABLE dwh_chu.dim_temps (
    id_temps INT,
    date_complete DATE,
    annee INT,
    trimestre INT,
    mois INT,
    semaine_annee INT,
    jour_annee INT,
    jour_mois INT,
    jour_semaine INT,
    nom_jour STRING,
    nom_mois STRING,
    est_week_end BOOLEAN,
    est_jour_ferie BOOLEAN,
    saison STRING
)
STORED AS PARQUET
LOCATION 'hdfs://chu-namenode:9000/data/gold/dimensions/dim_temps'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- dim_temps: 7306 lignes

DROP TABLE IF EXISTS dwh_chu.dim_patient;
CREATE EXTERNAL TABLE dwh_chu.dim_patient (
    sk_patient BIGINT,
    id_patient_hash STRING,
    sexe STRING,
    tranche_age STRING,
    code_postal STRING,
    departement STRING,
    est_courant BOOLEAN,
    date_debut_validite DATE,
    date_fin_validite DATE
)
STORED AS PARQUET
LOCATION 'hdfs://chu-namenode:9000/data/gold/dimensions/dim_patient'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- dim_patient: 99987 lignes

DROP TABLE IF EXISTS dwh_chu.dim_etablissement;
CREATE EXTERNAL TABLE dwh_chu.dim_etablissement (
    sk_etablissement BIGINT,
    id_etablissement_src STRING,
    nom_etablissement STRING,
    categorie STRING,
    departement STRING
)
STORED AS PARQUET
LOCATION 'hdfs://chu-namenode:9000/data/gold/dimensions/dim_etablissement'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- dim_etablissement: 0 lignes

DROP TABLE IF EXISTS dwh_chu.dim_diagnostic;
CREATE EXTERNAL TABLE dwh_chu.dim_diagnostic (
    sk_diagnostic BIGINT,
    code_cim10 STRING,
    libelle_diagnostic STRING,
    chapitre_cim10 STRING,
    libelle_chapitre STRING
)
STORED AS PARQUET
LOCATION 'hdfs://chu-namenode:9000/data/gold/dimensions/dim_diagnostic'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- dim_diagnostic: 15490 lignes

DROP TABLE IF EXISTS dwh_chu.dim_professionnel;
CREATE EXTERNAL TABLE dwh_chu.dim_professionnel (
    sk_professionnel BIGINT,
    id_professionnel_hash STRING,
    specialite STRING,
    mode_exercice STRING,
    categorie_pro STRING,
    est_courant BOOLEAN,
    date_debut_validite DATE,
    date_fin_validite DATE
)
STORED AS PARQUET
LOCATION 'hdfs://chu-namenode:9000/data/gold/dimensions/dim_professionnel'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- dim_professionnel: 1048575 lignes

DROP TABLE IF EXISTS dwh_chu.dim_geographie;
CREATE EXTERNAL TABLE dwh_chu.dim_geographie (
    sk_geographie BIGINT,
    code_postal STRING,
    departement STRING,
    region STRING
)
STORED AS PARQUET
LOCATION 'hdfs://chu-namenode:9000/data/gold/dimensions/dim_geographie'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- dim_geographie: 555 lignes

DROP TABLE IF EXISTS dwh_chu.dim_question;
CREATE EXTERNAL TABLE dwh_chu.dim_question (
    sk_question INT,
    code_question STRING,
    libelle_court STRING,
    libelle_long STRING
)
STORED AS PARQUET
LOCATION 'hdfs://chu-namenode:9000/data/gold/dimensions/dim_question'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- dim_question: 7 lignes

DROP TABLE IF EXISTS dwh_chu.fait_consultation;
CREATE EXTERNAL TABLE dwh_chu.fait_consultation (
    sk_consultation BIGINT,
    sk_temps INT,
    sk_patient BIGINT,
    sk_professionnel BIGINT,
    sk_diagnostic BIGINT,
    duree_minutes INT,
    cout_euros FLOAT,
    type_consultation STRING
)
PARTITIONED BY (
    annee INT,
    mois INT
)
STORED AS PARQUET
LOCATION 'hdfs://chu-namenode:9000/data/gold/faits/fait_consultation'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- fait_consultation: 1027157 lignes

DROP TABLE IF EXISTS dwh_chu.fait_hospitalisation;
CREATE EXTERNAL TABLE dwh_chu.fait_hospitalisation (
    sk_hospitalisation BIGINT,
    sk_temps_entree BIGINT,
    duree_sejour_jours INT,
    cout_sejour_euros FLOAT
)
PARTITIONED BY (
    annee INT,
    mois INT
)
STORED AS PARQUET
LOCATION 'hdfs://chu-namenode:9000/data/gold/faits/fait_hospitalisation'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- fait_hospitalisation: 1890 lignes

DROP TABLE IF EXISTS dwh_chu.fait_deces;
CREATE EXTERNAL TABLE dwh_chu.fait_deces (
    sk_deces BIGINT,
    sk_temps BIGINT,
    departement BIGINT,
    annee INT
)
STORED AS PARQUET
LOCATION 'hdfs://chu-namenode:9000/data/gold/faits/fait_deces'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- fait_deces: 0 lignes

DROP TABLE IF EXISTS dwh_chu.fait_satisfaction;
CREATE EXTERNAL TABLE dwh_chu.fait_satisfaction (
    sk_satisfaction BIGINT,
    sk_etablissement BIGINT,
    sk_question BIGINT,
    sk_temps BIGINT,
    note_moyenne FLOAT,
    nb_repondants INT,
    taux_reponse FLOAT
)
STORED AS PARQUET
LOCATION 'hdfs://chu-namenode:9000/data/gold/faits/fait_satisfaction'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- fait_satisfaction: 0 lignes

