-- ============================================================
--  CHU — Script 3 : Tables externes (staging depuis HDFS)
--  À utiliser après dépôt des fichiers sur HDFS via Talend
-- ============================================================
USE chu_dw;

-- Table externe pour le CSV établissements (staging)
CREATE EXTERNAL TABLE IF NOT EXISTS stg_etablissement (
    finess          STRING,
    nom             STRING,
    type            STRING,
    adresse         STRING,
    code_postal     STRING,
    commune         STRING,
    departement     STRING,
    region          STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/chu/staging/etablissements/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Table externe pour les fichiers satisfaction (FTP)
CREATE EXTERNAL TABLE IF NOT EXISTS stg_satisfaction (
    annee           INT,
    etab_id         STRING,
    question_id     STRING,
    note            DECIMAL(4,2),
    nb_repondants   INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/chu/staging/satisfaction/';

-- Table externe pour le répertoire décès INSEE (FTP)
CREATE EXTERNAL TABLE IF NOT EXISTS stg_deces (
    annee           INT,
    code_insee      STRING,
    region          STRING,
    nb_deces        INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/chu/staging/deces/';

