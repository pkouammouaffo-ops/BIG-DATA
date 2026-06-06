#!/usr/bin/env python3
# =============================================================================
# LIVRABLE 2 - Script 01 : CRÉATION DU SCHÉMA HIVE - DATA WAREHOUSE CHU
# =============================================================================
# Projet    : CHU Data Warehouse - Architecture Médaillon
# Auteur    : Groupe 6 BigData CESI
# Date      : Juin 2026
# Version   : 1.0
#
# DESCRIPTION :
#   Ce script crée la base de données Hive "dwh_chu" et définit les tables
#   externes du modèle dimensionnel en constellation (11 tables) :
#     - 7 tables de dimensions (DIM_TEMPS, DIM_PATIENT, DIM_ETABLISSEMENT,
#       DIM_DIAGNOSTIC, DIM_PROFESSIONNEL, DIM_GEOGRAPHIE, DIM_QUESTION)
#     - 4 tables de faits (FAIT_CONSULTATION, FAIT_HOSPITALISATION,
#       FAIT_DECES, FAIT_SATISFACTION)
#
#   Les tables sont de type EXTERNAL et pointent vers les dossiers HDFS Gold.
#   Le format de stockage est Apache Parquet avec compression Snappy.
#
# PRÉREQUIS :
#   - Script 00_setup_hdfs.py exécuté avec succès (dossiers HDFS créés)
#   - Conteneur chu-hive-server démarré
#   - Metastore Hive opérationnel (PostgreSQL metastore_db accessible)
#
# EXÉCUTION :
#   docker exec chu-spark-master spark-submit \
#     --master spark://chu-spark-master:7077 \
#     --conf spark.sql.warehouse.dir=hdfs://chu-namenode:9000/warehouse \
#     --conf spark.hadoop.hive.metastore.uris=thrift://chu-hive-server:9083 \
#     /opt/airflow/dags/jobs/01_create_hive_schema.py
# =============================================================================

import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION DU LOGGER
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("01_create_hive_schema")

# ─────────────────────────────────────────────────────────────────────────────
# PARAMÈTRES GLOBAUX
# ─────────────────────────────────────────────────────────────────────────────
HDFS_NAMENODE  = "hdfs://chu-namenode:9000"
HIVE_METASTORE = "thrift://chu-hive-server:9083"
DATABASE_NAME  = "dwh_chu"
GOLD_BASE_PATH = f"{HDFS_NAMENODE}/data/gold"

# ─────────────────────────────────────────────────────────────────────────────
# DÉFINITIONS DDL DES TABLES (format dict pour faciliter la maintenance)
# ─────────────────────────────────────────────────────────────────────────────

#
# Chaque table est décrite par :
#   name        : Nom de la table dans Hive
#   hdfs_path   : Dossier HDFS Gold pointé par la table EXTERNAL
#   scd_type    : Type de Slowly Changing Dimension (1, 2, ou N/A pour les faits)
#   comment     : Description métier de la table
#   ddl         : Instruction CREATE TABLE HiveQL complète
#

HIVE_TABLES_DDL = [

    # ─────────────────────────────────────────────────────────────────────────
    # DIMENSION 1 : DIM_TEMPS
    # SCD Type 1 - Calendrier fixe 2010-2030
    # ─────────────────────────────────────────────────────────────────────────
    {
        "name": "dim_temps",
        "hdfs_path": f"{GOLD_BASE_PATH}/dimensions/dim_temps",
        "scd_type": "SCD Type 1",
        "comment": "Calendrier complet 2010-2030 avec hiérarchie temporelle complète",
        "ddl": f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.dim_temps (
                temps_sk          INT          COMMENT 'Clé de substitution (YYYYMMDD)',
                date_complete     DATE         COMMENT 'Date complète ex: 2024-06-01',
                jour              INT          COMMENT 'Numéro du jour dans le mois (1-31)',
                mois              INT          COMMENT 'Numéro du mois (1-12)',
                trimestre         INT          COMMENT 'Trimestre (1-4)',
                annee             INT          COMMENT 'Année (ex: 2024)',
                semaine_annee     INT          COMMENT 'Numéro de semaine ISO (1-53)',
                jour_semaine      INT          COMMENT 'Jour dans la semaine (1=Lundi ... 7=Dimanche)',
                libelle_jour      STRING       COMMENT 'Nom du jour ex: Lundi',
                libelle_mois      STRING       COMMENT 'Nom du mois ex: Janvier',
                libelle_trimestre STRING       COMMENT 'Libellé trimestre ex: T1 2024',
                est_weekend       BOOLEAN      COMMENT 'True si samedi ou dimanche',
                est_jour_ferie    BOOLEAN      COMMENT 'True si jour férié en France',
                libelle_jour_ferie STRING      COMMENT 'Nom du jour férié si applicable',
                est_vacances      BOOLEAN      COMMENT 'True si en période de vacances scolaires',
                saison            STRING       COMMENT 'Saison : Hiver / Printemps / Été / Automne'
            )
            COMMENT 'Dimension temporelle - Calendrier 2010 à 2030'
            STORED AS PARQUET
            LOCATION '{GOLD_BASE_PATH}/dimensions/dim_temps'
            TBLPROPERTIES (
                'parquet.compression'='SNAPPY',
                'scd_type'='SCD Type 1',
                'created_by'='livrable2_chu',
                'created_date'='{datetime.now().strftime("%Y-%m-%d")}'
            )
        """
    },

    # ─────────────────────────────────────────────────────────────────────────
    # DIMENSION 2 : DIM_PATIENT
    # SCD Type 2 - Historisation des changements de situation
    # ─────────────────────────────────────────────────────────────────────────
    {
        "name": "dim_patient",
        "hdfs_path": f"{GOLD_BASE_PATH}/dimensions/dim_patient",
        "scd_type": "SCD Type 2",
        "comment": "Patients pseudonymisés avec historique (RGPD conforme)",
        "ddl": f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.dim_patient (
                patient_sk              INT     COMMENT 'Clé de substitution (séquentielle)',
                id_patient_hash         STRING  COMMENT 'SHA-256 de id_patient original (RGPD)',
                date_naissance          DATE    COMMENT 'Date de naissance du patient',
                annee_naissance         INT     COMMENT 'Année de naissance (pour calcul âge)',
                sexe                    STRING  COMMENT 'Sexe : M / F / I (indéterminé)',
                code_postal             STRING  COMMENT 'Code postal de résidence (5 chiffres)',
                ville                   STRING  COMMENT 'Ville de résidence (nom normalisé)',
                departement             STRING  COMMENT 'Numéro de département (01-976)',
                region                  STRING  COMMENT 'Région administrative (ex: Île-de-France)',
                tranche_age             STRING  COMMENT 'Tranche: <18 / 18-39 / 40-64 / 65-79 / 80+',
                mutuelle_adherent       BOOLEAN COMMENT 'True si patient a une mutuelle enregistrée',
                nom_mutuelle            STRING  COMMENT 'Nom de la mutuelle si applicable',
                date_debut_validite     DATE    COMMENT 'Date début de validité (SCD Type 2)',
                date_fin_validite       DATE    COMMENT 'Date fin de validité (NULL si courant)',
                est_courant             BOOLEAN COMMENT 'True si c est la version courante du patient'
            )
            COMMENT 'Dimension patient pseudonymisée (SHA-256) - SCD Type 2'
            STORED AS PARQUET
            LOCATION '{GOLD_BASE_PATH}/dimensions/dim_patient'
            TBLPROPERTIES (
                'parquet.compression'='SNAPPY',
                'scd_type'='SCD Type 2',
                'rgpd_compliant'='true',
                'pseudonymisation'='SHA-256',
                'created_by'='livrable2_chu',
                'created_date'='{datetime.now().strftime("%Y-%m-%d")}'
            )
        """
    },

    # ─────────────────────────────────────────────────────────────────────────
    # DIMENSION 3 : DIM_ETABLISSEMENT
    # SCD Type 1 - Établissements de santé (FINESS)
    # ─────────────────────────────────────────────────────────────────────────
    {
        "name": "dim_etablissement",
        "hdfs_path": f"{GOLD_BASE_PATH}/dimensions/dim_etablissement",
        "scd_type": "SCD Type 1",
        "comment": "Établissements de santé FINESS avec enrichissement géographique",
        "ddl": f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.dim_etablissement (
                etablissement_sk    INT     COMMENT 'Clé de substitution',
                finess              STRING  COMMENT 'Numéro FINESS (9 chiffres)',
                raison_sociale      STRING  COMMENT 'Dénomination officielle',
                categorie_code      STRING  COMMENT 'Code catégorie FINESS ex: 355 = CHU',
                categorie_libelle   STRING  COMMENT 'Libellé catégorie ex: Centre Hospitalier Universitaire',
                statut_juridique    STRING  COMMENT 'Public / ESPIC / Privé lucratif',
                capacite_lits       INT     COMMENT 'Nombre de lits autorisés',
                adresse             STRING  COMMENT 'Adresse postale complète',
                code_postal         STRING  COMMENT 'Code postal (5 chiffres)',
                ville               STRING  COMMENT 'Ville',
                departement         STRING  COMMENT 'Numéro de département',
                region              STRING  COMMENT 'Région administrative',
                telephone           STRING  COMMENT 'Numéro de téléphone (format +33)',
                activites           STRING  COMMENT 'Activités autorisées (liste séparée par ;)',
                latitude            DOUBLE  COMMENT 'Latitude GPS (WGS84)',
                longitude           DOUBLE  COMMENT 'Longitude GPS (WGS84)',
                est_actif           BOOLEAN COMMENT 'True si établissement toujours actif'
            )
            COMMENT 'Dimension établissements de santé - Référentiel FINESS'
            STORED AS PARQUET
            LOCATION '{GOLD_BASE_PATH}/dimensions/dim_etablissement'
            TBLPROPERTIES (
                'parquet.compression'='SNAPPY',
                'scd_type'='SCD Type 1',
                'source'='FINESS',
                'created_by'='livrable2_chu',
                'created_date'='{datetime.now().strftime("%Y-%m-%d")}'
            )
        """
    },

    # ─────────────────────────────────────────────────────────────────────────
    # DIMENSION 4 : DIM_DIAGNOSTIC
    # SCD Type 1 - Nomenclature CIM-10
    # ─────────────────────────────────────────────────────────────────────────
    {
        "name": "dim_diagnostic",
        "hdfs_path": f"{GOLD_BASE_PATH}/dimensions/dim_diagnostic",
        "scd_type": "SCD Type 1",
        "comment": "Nomenclature diagnostics CIM-10 avec hiérarchie chapitre/groupe/code",
        "ddl": f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.dim_diagnostic (
                diagnostic_sk       INT     COMMENT 'Clé de substitution',
                code_cim10          STRING  COMMENT 'Code CIM-10 ex: J18.9',
                libelle_court       STRING  COMMENT 'Libellé court (50 car. max)',
                libelle_long        STRING  COMMENT 'Libellé complet',
                chapitre_code       STRING  COMMENT 'Code chapitre CIM-10 ex: X',
                chapitre_libelle    STRING  COMMENT 'Libellé chapitre ex: Maladies respiratoires',
                groupe_code         STRING  COMMENT 'Code groupe ex: J10-J18',
                groupe_libelle      STRING  COMMENT 'Libellé groupe',
                type_diagnostique   STRING  COMMENT 'DP=Principal / DR=Relié / DAS=Associé',
                severite            INT     COMMENT 'Niveau de sévérité 1-5 (5=critique)',
                est_chronique       BOOLEAN COMMENT 'True si maladie chronique longue durée',
                est_infectieux      BOOLEAN COMMENT 'True si maladie infectieuse/contagieuse',
                est_invalidant      BOOLEAN COMMENT 'True si reconnue comme invalidante'
            )
            COMMENT 'Dimension diagnostics - Nomenclature CIM-10 (Classification Internationale Maladies)'
            STORED AS PARQUET
            LOCATION '{GOLD_BASE_PATH}/dimensions/dim_diagnostic'
            TBLPROPERTIES (
                'parquet.compression'='SNAPPY',
                'scd_type'='SCD Type 1',
                'nomenclature'='CIM-10 (OMS)',
                'created_by'='livrable2_chu',
                'created_date'='{datetime.now().strftime("%Y-%m-%d")}'
            )
        """
    },

    # ─────────────────────────────────────────────────────────────────────────
    # DIMENSION 5 : DIM_PROFESSIONNEL
    # SCD Type 2 - Professionnels de santé (RPPS/ADELI)
    # ─────────────────────────────────────────────────────────────────────────
    {
        "name": "dim_professionnel",
        "hdfs_path": f"{GOLD_BASE_PATH}/dimensions/dim_professionnel",
        "scd_type": "SCD Type 2",
        "comment": "Professionnels de santé RPPS/ADELI avec historique de spécialité",
        "ddl": f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.dim_professionnel (
                professionnel_sk        INT     COMMENT 'Clé de substitution',
                identifiant_rpps        STRING  COMMENT 'Identifiant RPPS (11 chiffres)',
                identifiant_adeli       STRING  COMMENT 'Identifiant ADELI si applicable (9 chiffres)',
                profession_code         STRING  COMMENT 'Code profession ex: 10 = Médecin',
                profession_libelle      STRING  COMMENT 'Libellé profession ex: Médecin',
                specialite_code         STRING  COMMENT 'Code spécialité médicale',
                specialite_libelle      STRING  COMMENT 'Libellé spécialité ex: Cardiologie',
                mode_exercice           STRING  COMMENT 'LIBERAL / SALARIE / BENEVOLE',
                secteur_activite        STRING  COMMENT 'MCO / SSR / HAD / PSY / AMBULATOIRE',
                departement_exercice    STRING  COMMENT 'Département d exercice',
                region_exercice         STRING  COMMENT 'Région d exercice',
                annee_premiere_install  INT     COMMENT 'Année de première installation',
                date_debut_validite     DATE    COMMENT 'Date début validité (SCD Type 2)',
                date_fin_validite       DATE    COMMENT 'Date fin validité (NULL si courant)',
                est_courant             BOOLEAN COMMENT 'True si enregistrement courant actif'
            )
            COMMENT 'Dimension professionnels de santé - Référentiels RPPS et ADELI'
            STORED AS PARQUET
            LOCATION '{GOLD_BASE_PATH}/dimensions/dim_professionnel'
            TBLPROPERTIES (
                'parquet.compression'='SNAPPY',
                'scd_type'='SCD Type 2',
                'source'='RPPS / ADELI',
                'created_by'='livrable2_chu',
                'created_date'='{datetime.now().strftime("%Y-%m-%d")}'
            )
        """
    },

    # ─────────────────────────────────────────────────────────────────────────
    # DIMENSION 6 : DIM_GEOGRAPHIE
    # SCD Type 1 - Référentiel géographique France
    # ─────────────────────────────────────────────────────────────────────────
    {
        "name": "dim_geographie",
        "hdfs_path": f"{GOLD_BASE_PATH}/dimensions/dim_geographie",
        "scd_type": "SCD Type 1",
        "comment": "Référentiel géographique France : code postal → commune → département → région",
        "ddl": f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.dim_geographie (
                geographie_sk       INT     COMMENT 'Clé de substitution',
                code_postal         STRING  COMMENT 'Code postal (5 chiffres)',
                code_commune_insee  STRING  COMMENT 'Code INSEE commune (5 chiffres)',
                nom_commune         STRING  COMMENT 'Nom de la commune',
                code_departement    STRING  COMMENT 'Code département (01-976)',
                nom_departement     STRING  COMMENT 'Nom du département',
                code_region         STRING  COMMENT 'Code région INSEE (11, 24, 27, ...)',
                nom_region          STRING  COMMENT 'Nom de la région ex: Île-de-France',
                nom_region_court    STRING  COMMENT 'Nom court ex: IdF / PACA / BFC',
                zone_urbaine        STRING  COMMENT 'RURALE / SEMI-URBAINE / URBAINE / METROPOLE',
                population_2024     INT     COMMENT 'Population estimée 2024 de la commune',
                densite_hab_km2     DOUBLE  COMMENT 'Densité de population (hab/km²)',
                latitude            DOUBLE  COMMENT 'Latitude du centroïde de la commune',
                longitude           DOUBLE  COMMENT 'Longitude du centroïde de la commune',
                dom_tom             BOOLEAN COMMENT 'True si DOM-TOM (code postal 97xxx/98xxx)'
            )
            COMMENT 'Dimension géographique France - Code postal vers région (référentiel La Poste + INSEE)'
            STORED AS PARQUET
            LOCATION '{GOLD_BASE_PATH}/dimensions/dim_geographie'
            TBLPROPERTIES (
                'parquet.compression'='SNAPPY',
                'scd_type'='SCD Type 1',
                'source'='La Poste + INSEE',
                'created_by'='livrable2_chu',
                'created_date'='{datetime.now().strftime("%Y-%m-%d")}'
            )
        """
    },

    # ─────────────────────────────────────────────────────────────────────────
    # DIMENSION 7 : DIM_QUESTION
    # SCD Type 1 - Questions ESATIS satisfaction
    # ─────────────────────────────────────────────────────────────────────────
    {
        "name": "dim_question",
        "hdfs_path": f"{GOLD_BASE_PATH}/dimensions/dim_question",
        "scd_type": "SCD Type 1",
        "comment": "7 questions du questionnaire ESATIS de satisfaction patient",
        "ddl": f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.dim_question (
                question_sk         INT     COMMENT 'Clé de substitution (1-7)',
                code_question       STRING  COMMENT 'Code court ex: Q1 / Q2 / GLOBAL',
                libelle_court       STRING  COMMENT 'Intitulé court (50 car. max)',
                libelle_long        STRING  COMMENT 'Intitulé complet de la question',
                domaine             STRING  COMMENT 'Domaine : ACCUEIL / SOINS / CHAMBRE / ALIMENTATION / GLOBAL',
                est_indicateur_iqss BOOLEAN COMMENT 'True si question utilisée dans les IQSS HAS',
                ponderation         DOUBLE  COMMENT 'Poids dans le score global ESATIS (somme = 1.0)'
            )
            COMMENT 'Dimension questions ESATIS - 7 dimensions de satisfaction patient'
            STORED AS PARQUET
            LOCATION '{GOLD_BASE_PATH}/dimensions/dim_question'
            TBLPROPERTIES (
                'parquet.compression'='SNAPPY',
                'scd_type'='SCD Type 1',
                'source'='HAS ESATIS',
                'created_by'='livrable2_chu',
                'created_date'='{datetime.now().strftime("%Y-%m-%d")}'
            )
        """
    },

    # ─────────────────────────────────────────────────────────────────────────
    # FAIT 1 : FAIT_CONSULTATION
    # Granularité : 1 ligne = 1 acte médical (consultation)
    # ─────────────────────────────────────────────────────────────────────────
    {
        "name": "fait_consultation",
        "hdfs_path": f"{GOLD_BASE_PATH}/faits/fait_consultation",
        "scd_type": "Fait",
        "comment": "Actes médicaux - 1 ligne par consultation",
        "ddl": f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.fait_consultation (
                consultation_sk     BIGINT  COMMENT 'Clé de substitution du fait',
                temps_sk            INT     COMMENT 'FK → dim_temps (YYYYMMDD)',
                patient_sk          INT     COMMENT 'FK → dim_patient',
                professionnel_sk    INT     COMMENT 'FK → dim_professionnel',
                etablissement_sk    INT     COMMENT 'FK → dim_etablissement',
                diagnostic_sk       INT     COMMENT 'FK → dim_diagnostic (diagnostic principal)',
                geographie_sk       INT     COMMENT 'FK → dim_geographie (lieu de résidence patient)',
                duree_consultation  INT     COMMENT 'Durée en minutes (0-60+)',
                cout_acte           DOUBLE  COMMENT 'Coût de l acte en euros',
                montant_rembourse   DOUBLE  COMMENT 'Part remboursée par Assurance Maladie',
                montant_reste_charge DOUBLE COMMENT 'Reste à charge patient',
                type_consultation   STRING  COMMENT 'CABINET / TELECONSULTATION / DOMICILE / URGENCE',
                mode_consultation   STRING  COMMENT 'PROGRAMMEE / NON_PROGRAMMEE',
                secteur_tarif       STRING  COMMENT 'Secteur tarifaire : S1 / S2 / S3',
                nb_diagnostics_assoc INT    COMMENT 'Nombre de diagnostics associés (hors DP)',
                est_premiere_consult BOOLEAN COMMENT 'True si première consultation de ce patient pour ce médecin'
            )
            COMMENT 'Fait consultations - 1 ligne = 1 acte médical'
            PARTITIONED BY (annee INT COMMENT 'Partition par année', mois INT COMMENT 'Partition par mois')
            STORED AS PARQUET
            LOCATION '{GOLD_BASE_PATH}/faits/fait_consultation'
            TBLPROPERTIES (
                'parquet.compression'='SNAPPY',
                'granularity'='1 ligne = 1 consultation',
                'volume_estime'='5M lignes/an',
                'created_by'='livrable2_chu',
                'created_date'='{datetime.now().strftime("%Y-%m-%d")}'
            )
        """
    },

    # ─────────────────────────────────────────────────────────────────────────
    # FAIT 2 : FAIT_HOSPITALISATION
    # Granularité : 1 ligne = 1 séjour hospitalier (RSS/RUM)
    # ─────────────────────────────────────────────────────────────────────────
    {
        "name": "fait_hospitalisation",
        "hdfs_path": f"{GOLD_BASE_PATH}/faits/fait_hospitalisation",
        "scd_type": "Fait",
        "comment": "Séjours hospitaliers ATIH - 1 ligne par séjour (RSS)",
        "ddl": f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.fait_hospitalisation (
                hospitalisation_sk  BIGINT  COMMENT 'Clé de substitution du fait',
                temps_entree_sk     INT     COMMENT 'FK → dim_temps (date d entrée)',
                temps_sortie_sk     INT     COMMENT 'FK → dim_temps (date de sortie)',
                patient_sk          INT     COMMENT 'FK → dim_patient',
                etablissement_sk    INT     COMMENT 'FK → dim_etablissement',
                diagnostic_sk       INT     COMMENT 'FK → dim_diagnostic (diagnostic principal)',
                geographie_sk       INT     COMMENT 'FK → dim_geographie (lieu de résidence patient)',
                duree_sejour        INT     COMMENT 'Durée du séjour en jours',
                mode_entree         STRING  COMMENT '1=Domicile / 2=Mutation / 3=Transfert / 7=Urgences',
                mode_sortie         STRING  COMMENT '1=Domicile / 2=Mutation / 3=Transfert / 9=Décès',
                type_sejour         STRING  COMMENT 'MCO / SSR / HAD / PSY',
                ghm_code            STRING  COMMENT 'Code GHM (Groupe Homogène de Malades)',
                ghm_libelle         STRING  COMMENT 'Libellé GHM',
                type_ghm            STRING  COMMENT 'C=Chirurgie / M=Médecine / Z=Séance / K=Actes',
                salle_code          STRING  COMMENT 'Code salle / unité d hospitalisation',
                cout_sejour         DOUBLE  COMMENT 'Coût total estimé du séjour (tarif GHS)',
                est_deces           BOOLEAN COMMENT 'True si le patient est décédé pendant ce séjour'
            )
            COMMENT 'Fait hospitalisations - 1 ligne = 1 séjour hospitalier (RSS ATIH)'
            PARTITIONED BY (annee INT COMMENT 'Partition par année', mois INT COMMENT 'Partition par mois')
            STORED AS PARQUET
            LOCATION '{GOLD_BASE_PATH}/faits/fait_hospitalisation'
            TBLPROPERTIES (
                'parquet.compression'='SNAPPY',
                'granularity'='1 ligne = 1 séjour RSS',
                'volume_estime'='2M lignes/an',
                'source'='ATIH PMSI',
                'created_by'='livrable2_chu',
                'created_date'='{datetime.now().strftime("%Y-%m-%d")}'
            )
        """
    },

    # ─────────────────────────────────────────────────────────────────────────
    # FAIT 3 : FAIT_DECES
    # Granularité : 1 ligne = 1 décès enregistré par l'INSEE
    # ─────────────────────────────────────────────────────────────────────────
    {
        "name": "fait_deces",
        "hdfs_path": f"{GOLD_BASE_PATH}/faits/fait_deces",
        "scd_type": "Fait",
        "comment": "Décès en France INSEE - 1 ligne par décès",
        "ddl": f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.fait_deces (
                deces_sk            BIGINT  COMMENT 'Clé de substitution du fait',
                temps_deces_sk      INT     COMMENT 'FK → dim_temps (date de décès)',
                patient_sk          INT     COMMENT 'FK → dim_patient (clé dégradée si non connu SIH)',
                diagnostic_sk       INT     COMMENT 'FK → dim_diagnostic (cause principale de décès)',
                geographie_deces_sk INT     COMMENT 'FK → dim_geographie (lieu de décès)',
                geographie_domicile_sk INT  COMMENT 'FK → dim_geographie (lieu de résidence)',
                age_au_deces        INT     COMMENT 'Âge au moment du décès (années révolues)',
                tranche_age_deces   STRING  COMMENT '<1 / 1-17 / 18-39 / 40-64 / 65-79 / 80+',
                sexe                STRING  COMMENT 'M / F / I',
                lieu_deces          STRING  COMMENT 'HOPITAL / DOMICILE / EHPAD / RUE / AUTRE',
                cause_initiale_cim10 STRING COMMENT 'Code CIM-10 cause initiale de décès',
                cause_directe_cim10 STRING  COMMENT 'Code CIM-10 cause directe de décès',
                est_mort_subite     BOOLEAN COMMENT 'True si décès subit ou accidentel',
                est_deces_maternel  BOOLEAN COMMENT 'True si décès maternel'
            )
            COMMENT 'Fait décès - 1 ligne = 1 décès (source INSEE fichiers deces.csv)'
            PARTITIONED BY (annee INT COMMENT 'Partition par année', mois INT COMMENT 'Partition par mois')
            STORED AS PARQUET
            LOCATION '{GOLD_BASE_PATH}/faits/fait_deces'
            TBLPROPERTIES (
                'parquet.compression'='SNAPPY',
                'granularity'='1 ligne = 1 décès INSEE',
                'volume_estime'='300K lignes/an',
                'source'='INSEE fichiers état civil',
                'created_by'='livrable2_chu',
                'created_date'='{datetime.now().strftime("%Y-%m-%d")}'
            )
        """
    },

    # ─────────────────────────────────────────────────────────────────────────
    # FAIT 4 : FAIT_SATISFACTION
    # Granularité : 1 ligne = 1 score par établissement × question × année
    # ─────────────────────────────────────────────────────────────────────────
    {
        "name": "fait_satisfaction",
        "hdfs_path": f"{GOLD_BASE_PATH}/faits/fait_satisfaction",
        "scd_type": "Fait",
        "comment": "Scores ESATIS satisfaction patient - 1 ligne par établissement × question × période",
        "ddl": f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.fait_satisfaction (
                satisfaction_sk     BIGINT  COMMENT 'Clé de substitution du fait',
                temps_sk            INT     COMMENT 'FK → dim_temps (période de l enquête)',
                etablissement_sk    INT     COMMENT 'FK → dim_etablissement',
                question_sk         INT     COMMENT 'FK → dim_question',
                score_brut          DOUBLE  COMMENT 'Score brut ESATIS (0.0 à 100.0)',
                score_classe        STRING  COMMENT 'Classe : A (>80) / B (60-80) / C (<60)',
                rang_national       INT     COMMENT 'Rang de l établissement au niveau national',
                rang_regional       INT     COMMENT 'Rang au niveau régional',
                nb_repondants       INT     COMMENT 'Nombre de patients ayant répondu à cette question',
                taux_participation  DOUBLE  COMMENT 'Taux de participation à l enquête (0.0 à 1.0)',
                millesime           INT     COMMENT 'Millésime ESATIS ex: 2017, 2019, 2020',
                type_enquete        STRING  COMMENT 'ESATIS48H / ESATISCA / HPP / RCP / DPA',
                secteur_sejour      STRING  COMMENT 'MCO / SSR / HAD',
                est_iqss            BOOLEAN COMMENT 'True si cet indicateur fait partie des IQSS HAS'
            )
            COMMENT 'Fait satisfaction patients - Scores ESATIS par établissement et question (HAS 2013-2020)'
            PARTITIONED BY (annee INT COMMENT 'Partition par année millésime')
            STORED AS PARQUET
            LOCATION '{GOLD_BASE_PATH}/faits/fait_satisfaction'
            TBLPROPERTIES (
                'parquet.compression'='SNAPPY',
                'granularity'='1 ligne = 1 score établissement × question × période',
                'volume_estime'='150K lignes',
                'source'='HAS ESATIS / IQSS',
                'created_by'='livrable2_chu',
                'created_date'='{datetime.now().strftime("%Y-%m-%d")}'
            )
        """
    },
]

# DDL pour les vues analytiques utiles dans Hive
HIVE_VIEWS_DDL = [

    # Vue : Consultations avec toutes les dimensions (dénormalisée)
    f"""
        CREATE OR REPLACE VIEW {DATABASE_NAME}.v_consultations_completes AS
        SELECT
            fc.consultation_sk,
            t.date_complete          AS date_consultation,
            t.annee                  AS annee,
            t.mois                   AS mois,
            t.trimestre              AS trimestre,
            t.libelle_mois           AS libelle_mois,
            p.id_patient_hash        AS patient_id,
            p.sexe                   AS patient_sexe,
            p.tranche_age            AS patient_tranche_age,
            p.region                 AS patient_region,
            e.finess                 AS finess_etab,
            e.raison_sociale         AS etablissement,
            e.categorie_libelle      AS type_etablissement,
            e.region                 AS region_etab,
            pr.profession_libelle    AS profession_medecin,
            pr.specialite_libelle    AS specialite_medecin,
            d.code_cim10             AS code_diagnostic,
            d.libelle_court          AS diagnostic,
            d.chapitre_libelle       AS chapitre_diagnostic,
            fc.duree_consultation    AS duree_minutes,
            fc.cout_acte             AS cout_euros,
            fc.montant_rembourse     AS rembourse_euros,
            fc.type_consultation     AS type_consultation,
            fc.mode_consultation     AS mode_consultation
        FROM {DATABASE_NAME}.fait_consultation fc
        JOIN {DATABASE_NAME}.dim_temps t
            ON fc.temps_sk = t.temps_sk
        JOIN {DATABASE_NAME}.dim_patient p
            ON fc.patient_sk = p.patient_sk AND p.est_courant = TRUE
        JOIN {DATABASE_NAME}.dim_etablissement e
            ON fc.etablissement_sk = e.etablissement_sk
        JOIN {DATABASE_NAME}.dim_professionnel pr
            ON fc.professionnel_sk = pr.professionnel_sk AND pr.est_courant = TRUE
        JOIN {DATABASE_NAME}.dim_diagnostic d
            ON fc.diagnostic_sk = d.diagnostic_sk
    """,

    # Vue : Score satisfaction moyen par établissement
    f"""
        CREATE OR REPLACE VIEW {DATABASE_NAME}.v_satisfaction_etablissement AS
        SELECT
            e.finess                    AS finess,
            e.raison_sociale            AS etablissement,
            e.region                    AS region,
            e.statut_juridique          AS statut_juridique,
            fs.millesime                AS annee,
            ROUND(AVG(fs.score_brut), 2) AS score_moyen_global,
            MAX(CASE WHEN q.domaine='ACCUEIL' THEN fs.score_brut END) AS score_accueil,
            MAX(CASE WHEN q.domaine='SOINS' THEN fs.score_brut END)   AS score_soins,
            MAX(CASE WHEN q.domaine='CHAMBRE' THEN fs.score_brut END) AS score_chambre,
            SUM(fs.nb_repondants)       AS total_repondants
        FROM {DATABASE_NAME}.fait_satisfaction fs
        JOIN {DATABASE_NAME}.dim_etablissement e
            ON fs.etablissement_sk = e.etablissement_sk
        JOIN {DATABASE_NAME}.dim_question q
            ON fs.question_sk = q.question_sk
        GROUP BY e.finess, e.raison_sociale, e.region, e.statut_juridique, fs.millesime
    """,

    # Vue : Mortalité par région et pathologie
    f"""
        CREATE OR REPLACE VIEW {DATABASE_NAME}.v_mortalite_region_pathologie AS
        SELECT
            g.nom_region                AS region,
            g.nom_departement           AS departement,
            d.chapitre_libelle          AS chapitre_cim10,
            d.libelle_court             AS pathologie,
            t.annee                     AS annee,
            fd.tranche_age_deces        AS tranche_age,
            fd.sexe                     AS sexe,
            COUNT(*)                    AS nombre_deces,
            ROUND(AVG(fd.age_au_deces), 1) AS age_moyen_deces,
            SUM(CASE WHEN fd.lieu_deces='HOPITAL' THEN 1 ELSE 0 END) AS deces_a_hopital,
            SUM(CASE WHEN fd.lieu_deces='DOMICILE' THEN 1 ELSE 0 END) AS deces_domicile
        FROM {DATABASE_NAME}.fait_deces fd
        JOIN {DATABASE_NAME}.dim_geographie g
            ON fd.geographie_deces_sk = g.geographie_sk
        JOIN {DATABASE_NAME}.dim_diagnostic d
            ON fd.diagnostic_sk = d.diagnostic_sk
        JOIN {DATABASE_NAME}.dim_temps t
            ON fd.temps_deces_sk = t.temps_sk
        GROUP BY g.nom_region, g.nom_departement, d.chapitre_libelle,
                 d.libelle_court, t.annee, fd.tranche_age_deces, fd.sexe
    """,
]


# ─────────────────────────────────────────────────────────────────────────────
# FONCTIONS PRINCIPALES
# ─────────────────────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """
    Crée une session Spark avec support Hive (connexion au Metastore).
    """
    log.info("Initialisation de la session Spark avec support Hive...")
    spark = (
        SparkSession.builder
        .appName("CHU_01_Create_Hive_Schema")
        .master("spark://chu-spark-master:7077")
        .config("spark.sql.warehouse.dir", f"{HDFS_NAMENODE}/warehouse")
        .config("spark.hadoop.hive.metastore.uris", HIVE_METASTORE)
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
        .config("spark.executor.instances", "1")
        .config("spark.executor.memory", "512m")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info(f"Session Spark+Hive créée - App ID : {spark.sparkContext.applicationId}")
    return spark


def create_database(spark: SparkSession) -> bool:
    """
    Crée la base de données Hive dwh_chu si elle n'existe pas.
    """
    try:
        spark.sql(f"""
            CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}
            COMMENT 'Data Warehouse CHU - Modèle en constellation - Architecture Médaillon'
            LOCATION '{HDFS_NAMENODE}/warehouse/{DATABASE_NAME}.db'
        """)
        log.info(f"  [OK] Base de données '{DATABASE_NAME}' créée/vérifiée")

        # Utiliser la base par défaut
        spark.sql(f"USE {DATABASE_NAME}")
        log.info(f"  [OK] Contexte défini sur '{DATABASE_NAME}'")
        return True
    except Exception as e:
        log.error(f"  [ERREUR] Création base de données : {e}")
        return False


def create_tables(spark: SparkSession) -> tuple:
    """
    Crée toutes les tables Hive (dimensions + faits) en EXTERNAL Parquet.
    Retourne (nb_succès, nb_erreurs).
    """
    successes = 0
    errors = 0

    for table_def in HIVE_TABLES_DDL:
        table_name = table_def["name"]
        try:
            # Nettoyage du DDL (retrait des espaces superflus)
            ddl = " ".join(table_def["ddl"].split())

            spark.sql(ddl)
            log.info(f"  [OK] Table '{DATABASE_NAME}.{table_name}' créée ({table_def['scd_type']})")
            successes += 1
        except Exception as e:
            if "already exists" in str(e).lower():
                log.info(f"  [EXISTE] Table '{DATABASE_NAME}.{table_name}' déjà présente")
                successes += 1
            else:
                log.error(f"  [ERREUR] Table '{table_name}' : {e}")
                errors += 1

    return successes, errors


def create_views(spark: SparkSession) -> tuple:
    """
    Crée les vues analytiques Hive.
    """
    successes = 0
    errors = 0

    view_names = [
        "v_consultations_completes",
        "v_satisfaction_etablissement",
        "v_mortalite_region_pathologie"
    ]

    for i, ddl in enumerate(HIVE_VIEWS_DDL):
        view_name = view_names[i]
        try:
            ddl_clean = " ".join(ddl.split())
            spark.sql(ddl_clean)
            log.info(f"  [OK] Vue '{DATABASE_NAME}.{view_name}' créée")
            successes += 1
        except Exception as e:
            log.error(f"  [ERREUR] Vue '{view_name}' : {e}")
            errors += 1

    return successes, errors


def verify_schema(spark: SparkSession):
    """
    Vérifie le schéma créé en listant les tables et leur structure.
    """
    log.info("\n" + "="*60)
    log.info("VÉRIFICATION DU SCHÉMA HIVE")
    log.info("="*60)

    try:
        # Lister les bases de données
        databases_df = spark.sql("SHOW DATABASES")
        log.info(f"\nBases de données disponibles :")
        for row in databases_df.collect():
            log.info(f"  - {row[0]}")

        # Lister les tables de dwh_chu
        tables_df = spark.sql(f"SHOW TABLES IN {DATABASE_NAME}")
        tables = tables_df.collect()
        log.info(f"\nTables dans '{DATABASE_NAME}' ({len(tables)} objets) :")

        dims = [t for t in tables if "dim_" in t[1]]
        faits = [t for t in tables if "fait_" in t[1]]
        views = [t for t in tables if "v_" in t[1]]

        log.info(f"\n  Dimensions ({len(dims)}) :")
        for t in dims:
            # Compter le nombre de colonnes via DESCRIBE
            cols = spark.sql(f"DESCRIBE {DATABASE_NAME}.{t[1]}").count()
            log.info(f"    ✅ {t[1]} ({cols} colonnes)")

        log.info(f"\n  Faits ({len(faits)}) :")
        for t in faits:
            cols = spark.sql(f"DESCRIBE {DATABASE_NAME}.{t[1]}").count()
            log.info(f"    ✅ {t[1]} ({cols} colonnes)")

        log.info(f"\n  Vues ({len(views)}) :")
        for t in views:
            log.info(f"    ✅ {t[1]}")

    except Exception as e:
        log.error(f"Erreur lors de la vérification : {e}")


# ─────────────────────────────────────────────────────────────────────────────
# SCRIPT PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def main():
    start_time = datetime.now()

    log.info("=" * 70)
    log.info("  CHU DATA WAREHOUSE - CRÉATION SCHÉMA HIVE")
    log.info(f"  Base de données : {DATABASE_NAME}")
    log.info(f"  Metastore       : {HIVE_METASTORE}")
    log.info(f"  Démarrage       : {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 70)

    spark = None

    try:
        # ── ÉTAPE 1 : Session Spark + Hive ───────────────────────────────
        log.info("\n[ÉTAPE 1/5] Connexion Spark avec support Hive...")
        spark = create_spark_session()

        # ── ÉTAPE 2 : Création de la base de données ──────────────────────
        log.info(f"\n[ÉTAPE 2/5] Création de la base '{DATABASE_NAME}'...")
        ok = create_database(spark)
        if not ok:
            raise RuntimeError("Impossible de créer la base de données Hive.")

        # ── ÉTAPE 3 : Création des tables EXTERNAL ────────────────────────
        log.info(f"\n[ÉTAPE 3/5] Création des {len(HIVE_TABLES_DDL)} tables (7 dims + 4 faits)...")
        log.info("-" * 60)
        t_ok, t_err = create_tables(spark)

        # ── ÉTAPE 4 : Création des vues analytiques ───────────────────────
        log.info(f"\n[ÉTAPE 4/5] Création des {len(HIVE_VIEWS_DDL)} vues analytiques...")
        v_ok, v_err = create_views(spark)

        # ── ÉTAPE 5 : Vérification du schéma ─────────────────────────────
        log.info("\n[ÉTAPE 5/5] Vérification du schéma complet...")
        verify_schema(spark)

        # ── RÉSUMÉ ────────────────────────────────────────────────────────
        duration = (datetime.now() - start_time).total_seconds()
        total_errors = t_err + v_err

        log.info("\n" + "=" * 70)
        log.info("  RÉSUMÉ - CRÉATION SCHÉMA HIVE")
        log.info("=" * 70)
        log.info(f"  Tables créées  : {t_ok}/{len(HIVE_TABLES_DDL)}")
        log.info(f"  Vues créées    : {v_ok}/{len(HIVE_VIEWS_DDL)}")
        log.info(f"  Erreurs        : {total_errors}")
        log.info(f"  Durée          : {duration:.1f}s")
        log.info(f"  Statut         : {'✅ SUCCÈS' if total_errors == 0 else '⚠️ PARTIEL'}")
        log.info("=" * 70)
        log.info("\nProchaine étape : Exécuter E1_extract_postgres.py")
        log.info("  Commande : spark-submit /opt/airflow/dags/jobs/E1_extract_postgres.py")

        if total_errors > 0:
            sys.exit(1)

    except Exception as e:
        log.error(f"Erreur critique : {e}", exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            log.info("\nSession Spark fermée.")


if __name__ == "__main__":
    main()
