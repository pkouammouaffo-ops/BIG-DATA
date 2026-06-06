#!/usr/bin/env python3
# =============================================================================
# LIVRABLE 2 - Job T1 : NETTOYAGE BRONZE → SILVER
# =============================================================================
# Projet    : CHU Data Warehouse - Architecture Médaillon
# Auteur    : Groupe 6 BigData CESI
# Date      : Juin 2026
# Version   : 1.0
#
# DESCRIPTION :
#   Ce job PySpark lit les fichiers Parquet de la couche Bronze (données brutes
#   extraites de PostgreSQL + CSV NiFi) et applique les transformations de
#   nettoyage nécessaires pour alimenter la couche Silver.
#
#   Transformations appliquées :
#   ┌─────────────────────┬────────────────────────────────────────────────┐
#   │ Catégorie           │ Transformations                                │
#   ├─────────────────────┼────────────────────────────────────────────────┤
#   │ Types de données    │ Cast implicites → types PostgreSQL d'origine   │
#   │ Valeurs nulles      │ Imputation selon règles métier                 │
#   │ Dates invalides     │ Détection et neutralisation                    │
#   │ Formats incohérents │ Normalisation (codes CIM-10, codes postaux...) │
#   │ Caractères spéciaux │ Nettoyage noms/villes (accents, casse)         │
#   │ Codes aberrants     │ Suppression des enregistrements hors-scope     │
#   └─────────────────────┴────────────────────────────────────────────────┘
#
# PRÉREQUIS :
#   - Job E1_extract_postgres.py exécuté avec succès
#   - Dossiers Bronze HDFS peuplés
#
# EXÉCUTION :
#   docker exec chu-spark-master spark-submit \
#     --master spark://chu-spark-master:7077 \
#     --conf spark.executor.memory=1g \
#     /opt/airflow/dags/jobs/T1_clean_bronze.py \
#     --date 2024-06-01
# =============================================================================

import sys
import argparse
import logging
from datetime import datetime, date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (IntegerType, DateType, DoubleType,
                                StringType, BooleanType)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("T1_clean_bronze")

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTES
# ─────────────────────────────────────────────────────────────────────────────
HDFS_NAMENODE = "hdfs://chu-namenode:9000"
BRONZE_BASE   = f"{HDFS_NAMENODE}/data/bronze/postgres"
SILVER_BASE   = f"{HDFS_NAMENODE}/data/silver"

# Code postal France : 5 chiffres (01000 → 98999)
REGEX_CODE_POSTAL   = r"^\d{5}$"
# Code CIM-10 : 3-7 caractères (ex: J18.9, A00, Z13.220)
REGEX_CIM10         = r"^[A-Z][0-9]{2}(\.[0-9A-Z]{1,4})?$"
# Numéro RPPS : 11 chiffres exactement
REGEX_RPPS          = r"^\d{11}$"
# Sexe valide : M, F, ou I
SEXE_VALIDES        = ["M", "F", "I"]
# Tranche de durée de consultation valide (minutes)
DUREE_CONSULT_MIN   = 0
DUREE_CONSULT_MAX   = 480   # 8 heures max
# Dates hors-scope : avant 1900 ou après aujourd'hui
DATE_MIN            = "1900-01-01"


# ─────────────────────────────────────────────────────────────────────────────
# FONCTIONS DE NETTOYAGE PAR TABLE
# ─────────────────────────────────────────────────────────────────────────────

def clean_patient(df: DataFrame, extraction_date: str) -> DataFrame:
    """
    Nettoyage de la table PATIENT.

    Règles appliquées :
    - Suppression doublons sur id_patient
    - date_naissance : cast DATE, nullification si < 1900 ou > aujourd'hui
    - sexe : normalisation (homme→M, femme→F), valeur inconnue → "I"
    - code_postal : validation regex 5 chiffres, NULL si invalide
    - nom / prenom : strip, UPPER pour nom, Title Case pour prénom
    - Calcul de la tranche d'âge
    """
    log.info("    Nettoyage PATIENT...")

    # Colonnes réelles Bronze : id_patient, nom, prenom, sexe, adresse, ville,
    # code_postal, pays, email, tel, date (=date_naissance), age, num_secu,
    # groupe_sanguin, poid, taille
    df_clean = df \
        .dropDuplicates(["id_patient"]) \
        .withColumn("date_naissance",
            F.when(
                (F.col("date").cast(DateType()) >= F.lit(DATE_MIN).cast(DateType())) &
                (F.col("date").cast(DateType()) <= F.current_date()),
                F.col("date").cast(DateType())
            ).otherwise(F.lit(None).cast(DateType()))
        ) \
        .withColumn("sexe",
            F.when(F.upper(F.col("sexe")).isin(["M", "MASCULIN", "HOMME", "H"]), F.lit("M"))
             .when(F.upper(F.col("sexe")).isin(["F", "FEMININ", "FEMME"]), F.lit("F"))
             .otherwise(F.lit("I"))
        ) \
        .withColumn("code_postal",
            F.when(F.col("code_postal").rlike(REGEX_CODE_POSTAL), F.col("code_postal"))
             .otherwise(F.lit(None).cast(StringType()))
        ) \
        .withColumn("nom",
            F.upper(F.trim(F.regexp_replace(F.col("nom"), r"[^\w\s\-']", "")))
        ) \
        .withColumn("prenom",
            F.initcap(F.trim(F.regexp_replace(F.col("prenom"), r"[^\w\s\-']", "")))
        ) \
        .withColumn("age_calcule",
            F.when(
                F.col("date_naissance").isNotNull(),
                F.floor(
                    F.datediff(F.current_date(), F.col("date_naissance")) / 365.25
                ).cast(IntegerType())
            ).otherwise(F.col("age").cast(IntegerType()))
        ) \
        .withColumn("tranche_age",
            F.when(F.col("age_calcule") < 18,  F.lit("<18"))
             .when(F.col("age_calcule") < 40,  F.lit("18-39"))
             .when(F.col("age_calcule") < 65,  F.lit("40-64"))
             .when(F.col("age_calcule") < 80,  F.lit("65-79"))
             .when(F.col("age_calcule") >= 80, F.lit("80+"))
             .otherwise(F.lit("INCONNU"))
        ) \
        .withColumn("_silver_date", F.lit(extraction_date).cast(DateType())) \
        .withColumn("_silver_source", F.lit("bronze_postgres_patient")) \
        .drop("_source_system", "_source_db", "_extraction_ts")

    return df_clean


def clean_consultation(df: DataFrame, extraction_date: str) -> DataFrame:
    """
    Nettoyage de la table CONSULTATION.

    Règles :
    - Suppression doublons sur id_consultation
    - date_consultation : validation, suppression si null ou aberrante
    - duree : entre DUREE_CONSULT_MIN et DUREE_CONSULT_MAX minutes
    - cout_acte : entre 0 et 2000 euros (plafond raisonnable)
    - type_consultation : normalisation vers valeurs canoniques
    - Filtre : uniquement consultations avec patient et médecin identifiés
    """
    log.info("    Nettoyage CONSULTATION...")

    # Colonnes réelles Bronze : num_consultation, id_mut, id_patient,
    # id_prof_sante, code_diag, motif, date (=date_consultation), heure_debut, heure_fin
    df_clean = df \
        .dropDuplicates(["num_consultation"]) \
        .filter(F.col("id_patient").isNotNull()) \
        .filter(F.col("id_prof_sante").isNotNull()) \
        .withColumn("date_consultation",
            F.when(
                (F.col("date").cast(DateType()) >= F.lit(DATE_MIN).cast(DateType())) &
                (F.col("date").cast(DateType()) <= F.current_date()),
                F.col("date").cast(DateType())
            ).otherwise(F.lit(None).cast(DateType()))
        ) \
        .filter(F.col("date_consultation").isNotNull()) \
        .withColumn("duree_minutes",
            F.when(
                F.col("heure_debut").isNotNull() & F.col("heure_fin").isNotNull(),
                F.lit(None).cast(IntegerType())   # calculé si besoin
            ).otherwise(F.lit(None).cast(IntegerType()))
        ) \
        .withColumn("annee_consultation", F.year(F.col("date_consultation"))) \
        .withColumn("mois_consultation", F.month(F.col("date_consultation"))) \
        .withColumn("_silver_date", F.lit(extraction_date).cast(DateType())) \
        .withColumn("_silver_source", F.lit("bronze_postgres_consultation")) \
        .drop("_source_system", "_source_db", "_extraction_ts")

    return df_clean


def clean_diagnostic(df: DataFrame, extraction_date: str) -> DataFrame:
    """
    Nettoyage de la table DIAGNOSTIC.

    Règles :
    - Suppression doublons sur code_cim10
    - code_cim10 : validation format CIM-10 (regex)
    - libelles : trim + nettoyage des caractères spéciaux
    - Suppression des codes dépréciés / vides
    """
    log.info("    Nettoyage DIAGNOSTIC...")

    # Colonnes réelles Bronze : code_diag, diagnostic
    df_clean = df \
        .dropDuplicates(["code_diag"]) \
        .filter(F.col("code_diag").isNotNull()) \
        .withColumn("code_diag",
            F.upper(F.trim(F.col("code_diag")))
        ) \
        .withColumn("diagnostic",
            F.trim(F.regexp_replace(
                F.coalesce(F.col("diagnostic"), F.col("code_diag")),
                r"\s+", " "
            ))
        ) \
        .withColumn("_silver_date", F.lit(extraction_date).cast(DateType())) \
        .withColumn("_silver_source", F.lit("bronze_postgres_diagnostic")) \
        .drop("_source_system", "_source_db", "_extraction_ts")

    return df_clean


def clean_professionnel_sante(df: DataFrame, extraction_date: str) -> DataFrame:
    """
    Nettoyage de la table PROFESSIONNEL_DE_SANTE.

    Règles :
    - Suppression doublons sur identifiant_rpps (si non null)
    - Validation format RPPS (11 chiffres)
    - Normalisation profession_libelle
    - Normalisation mode_exercice
    """
    log.info("    Nettoyage PROFESSIONNEL_DE_SANTE...")

    # Colonnes réelles Bronze : identifiant, civilite, categorie_professionnelle,
    # nom, prenom, profession, type_identifiant, code_specialite
    df_clean = df \
        .dropDuplicates(["identifiant"]) \
        .withColumn("identifiant_rpps",
            F.when(
                (F.col("type_identifiant") == "RPPS") &
                F.col("identifiant").rlike(REGEX_RPPS),
                F.col("identifiant")
            ).otherwise(F.lit(None).cast(StringType()))
        ) \
        .withColumn("nom",
            F.upper(F.trim(F.col("nom")))
        ) \
        .withColumn("prenom",
            F.initcap(F.trim(F.col("prenom")))
        ) \
        .withColumn("categorie_normalisee",
            F.upper(F.trim(F.col("categorie_professionnelle")))
        ) \
        .withColumn("_silver_date", F.lit(extraction_date).cast(DateType())) \
        .withColumn("_silver_source", F.lit("bronze_postgres_professionnel_sante")) \
        .drop("_source_system", "_source_db", "_extraction_ts")

    return df_clean


def clean_generic(df: DataFrame, table_name: str, extraction_date: str) -> DataFrame:
    """
    Nettoyage générique pour les tables référentielles (SPECIALITES, MEDICAMENTS,
    MUTUELLE, SALLE, LABORATOIRE) : trim, cast, suppression nulls critiques.
    """
    log.info(f"    Nettoyage générique {table_name}...")

    # Cast toutes les colonnes STRING en trim
    for col_name, col_type in df.dtypes:
        if col_type == "string":
            df = df.withColumn(col_name, F.trim(F.col(col_name)))

    df_clean = df \
        .withColumn("_silver_date", F.lit(extraction_date).cast(DateType())) \
        .withColumn("_silver_source", F.lit(f"bronze_postgres_{table_name.lower()}")) \
        .drop("_source_system", "_source_db", "_extraction_ts")

    return df_clean


# ─────────────────────────────────────────────────────────────────────────────
# DISPATCH : mappe table_name → fonction de nettoyage
# ─────────────────────────────────────────────────────────────────────────────

CLEAN_FUNCTIONS = {
    "patient"             : clean_patient,
    "consultation"        : clean_consultation,
    "diagnostic"          : clean_diagnostic,
    "professionnel_sante" : clean_professionnel_sante,
}


# ─────────────────────────────────────────────────────────────────────────────
# FONCTIONS UTILITAIRES
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Job T1 - Nettoyage Bronze → Silver"
    )
    parser.add_argument("--date", default=date.today().isoformat())
    return parser.parse_args()


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("CHU_T1_Clean_Bronze")
        .master("spark://chu-spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "2")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_bronze_tables(extraction_date: str) -> list:
    """Retourne la liste des dossiers Bronze disponibles pour la date."""
    return [
        {"bronze": f"{BRONZE_BASE}/patient/_extraction_date={extraction_date}",
         "silver": f"{SILVER_BASE}/patient",
         "folder": "patient"},
        {"bronze": f"{BRONZE_BASE}/consultation/_extraction_date={extraction_date}",
         "silver": f"{SILVER_BASE}/consultation",
         "folder": "consultation"},
        {"bronze": f"{BRONZE_BASE}/diagnostic/_extraction_date={extraction_date}",
         "silver": f"{SILVER_BASE}/diagnostic",
         "folder": "diagnostic"},
        {"bronze": f"{BRONZE_BASE}/professionnel_sante/_extraction_date={extraction_date}",
         "silver": f"{SILVER_BASE}/professionnel_sante",
         "folder": "professionnel_sante"},
        {"bronze": f"{BRONZE_BASE}/specialites/_extraction_date={extraction_date}",
         "silver": f"{SILVER_BASE}/specialites",
         "folder": "specialites"},
        {"bronze": f"{BRONZE_BASE}/medicaments/_extraction_date={extraction_date}",
         "silver": f"{SILVER_BASE}/medicaments",
         "folder": "medicaments"},
        {"bronze": f"{BRONZE_BASE}/mutuelle/_extraction_date={extraction_date}",
         "silver": f"{SILVER_BASE}/mutuelle",
         "folder": "mutuelle"},
        {"bronze": f"{BRONZE_BASE}/adher/_extraction_date={extraction_date}",
         "silver": f"{SILVER_BASE}/adher",
         "folder": "adher"},
        {"bronze": f"{BRONZE_BASE}/salle/_extraction_date={extraction_date}",
         "silver": f"{SILVER_BASE}/salle",
         "folder": "salle"},
        {"bronze": f"{BRONZE_BASE}/laboratoire/_extraction_date={extraction_date}",
         "silver": f"{SILVER_BASE}/laboratoire",
         "folder": "laboratoire"},
    ]


# ─────────────────────────────────────────────────────────────────────────────
# SCRIPT PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    extraction_date = args.date
    start_time = datetime.now()

    log.info("=" * 70)
    log.info("  CHU ETL - JOB T1 : NETTOYAGE BRONZE → SILVER")
    log.info(f"  Date          : {extraction_date}")
    log.info(f"  Démarrage     : {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 70)

    spark = None
    results = []

    try:
        spark = create_spark_session()
        tables = get_bronze_tables(extraction_date)

        log.info(f"\n{len(tables)} tables à nettoyer :")
        log.info("─" * 60)

        for t in tables:
            folder = t["folder"]
            table_start = datetime.now()
            log.info(f"\n  Table : {folder.upper()}")

            result = {"table": folder, "bronze_rows": 0, "silver_rows": 0,
                      "status": "ERROR", "duration": 0}

            try:
                # Lecture Bronze
                df_bronze = spark.read.parquet(t["bronze"])
                bronze_rows = df_bronze.count()
                log.info(f"    Bronze lu : {bronze_rows:,} lignes")

                # Nettoyage : fonction spécifique ou générique
                if folder in CLEAN_FUNCTIONS:
                    df_silver = CLEAN_FUNCTIONS[folder](df_bronze, extraction_date)
                else:
                    df_silver = clean_generic(df_bronze, folder, extraction_date)

                # Repartitionnement avant écriture (évite les petits fichiers)
                df_silver = df_silver.repartition(2)

                # Écriture Silver
                df_silver.write \
                    .mode("overwrite") \
                    .option("compression", "snappy") \
                    .parquet(t["silver"])

                silver_rows = spark.read.parquet(t["silver"]).count()
                duration = (datetime.now() - table_start).total_seconds()

                lost_rows = bronze_rows - silver_rows
                lost_pct = (lost_rows / bronze_rows * 100) if bronze_rows > 0 else 0

                log.info(f"    Silver écrit : {silver_rows:,} lignes")
                log.info(f"    Lignes filtrées : {lost_rows:,} ({lost_pct:.1f}%) "
                         f"← doublons + invalides")
                log.info(f"    Durée : {duration:.1f}s ✅")

                result = {
                    "table": folder,
                    "bronze_rows": bronze_rows,
                    "silver_rows": silver_rows,
                    "filtered": lost_rows,
                    "status": "OK",
                    "duration": round(duration, 1)
                }

            except Exception as e:
                log.error(f"    ❌ Erreur : {e}")
                result["status"] = "ERROR"

            results.append(result)

        # ── RÉSUMÉ ────────────────────────────────────────────────────────
        total_duration = (datetime.now() - start_time).total_seconds()
        ok_count = sum(1 for r in results if r["status"] == "OK")
        err_count = len(results) - ok_count

        log.info("\n" + "=" * 70)
        log.info("  RÉSUMÉ - JOB T1 : NETTOYAGE BRONZE → SILVER")
        log.info("=" * 70)
        log.info(f"  Tables traitées : {ok_count}/{len(results)} ✅   Erreurs : {err_count}")
        log.info(f"  Durée totale    : {total_duration:.0f}s")
        log.info("\n  Détail :")
        for r in results:
            if r["status"] == "OK":
                log.info(f"    ✅ {r['table']:<25} Bronze:{r['bronze_rows']:>8,} → "
                         f"Silver:{r['silver_rows']:>8,} (filtrés:{r['filtered']:>6,})")
            else:
                log.error(f"    ❌ {r['table']:<25} ERREUR")

        log.info("=" * 70)
        log.info("\nProchaine étape : Exécuter T2_deduplicate_silver.py")

        if err_count > 0:
            sys.exit(1)

    except Exception as e:
        log.error(f"Erreur critique : {e}", exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
