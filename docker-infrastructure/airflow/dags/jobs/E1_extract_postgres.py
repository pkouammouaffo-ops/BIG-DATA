#!/usr/bin/env python3
# =============================================================================
# LIVRABLE 2 - Job E1 : EXTRACTION POSTGRESQL → BRONZE HDFS
# =============================================================================
# Projet    : CHU Data Warehouse - Architecture Médaillon
# Auteur    : Groupe 6 BigData CESI
# Date      : Juin 2026
# Version   : 1.0
#
# DESCRIPTION :
#   Ce job PySpark extrait les 11 tables du Système d'Information Hospitalier
#   (SIH) depuis PostgreSQL (chu-postgres:5432/sih_data) et les charge dans
#   la couche Bronze du Data Lake HDFS au format Parquet.
#
#   Tables extraites :
#     1. PATIENT              (données démographiques)
#     2. CONSULTATION         (actes médicaux)
#     3. DIAGNOSTIC           (codes CIM-10)
#     4. PROFESSIONNEL_DE_SANTE (médecins, infirmiers)
#     5. SPECIALITES          (référentiel spécialités)
#     6. PRESCRIPTION         (prescriptions médicamenteuses)
#     7. MEDICAMENTS          (référentiel médicaments)
#     8. MUTUELLE             (référentiel mutuelles)
#     9. ADHER                (adhésions mutuelles patients)
#    10. SALLE                (salles hospitalières)
#    11. LABORATOIRE          (laboratoires pharmaceutiques)
#
# STRATÉGIE D'EXTRACTION :
#   - Mode "full load" : remplacement complet à chaque exécution
#   - Partitionnement JDBC pour les grosses tables (PATIENT, CONSULTATION)
#   - Ajout de colonnes de traçabilité (extraction_date, source_system)
#   - Écriture Parquet partitionnée par date d'extraction
#
# PRÉREQUIS :
#   - Script 00_setup_hdfs.py exécuté (dossiers HDFS créés)
#   - Données restaurées dans chu-postgres (sih_data)
#   - Driver PostgreSQL JDBC disponible sur le Worker Spark
#
# EXÉCUTION :
#   docker exec chu-spark-master spark-submit \
#     --master spark://chu-spark-master:7077 \
#     --jars /opt/spark/jars/postgresql-42.7.3.jar \
#     --conf spark.executor.memory=1g \
#     --conf spark.executor.cores=1 \
#     /opt/airflow/dags/jobs/E1_extract_postgres.py \
#     --date 2024-06-01
# =============================================================================

import sys
import argparse
import logging
from datetime import datetime, date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# ─────────────────────────────────────────────────────────────────────────────
# LOGGER
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("E1_extract_postgres")

# ─────────────────────────────────────────────────────────────────────────────
# PARAMÈTRES DE CONNEXION POSTGRESQL
# ─────────────────────────────────────────────────────────────────────────────
POSTGRES_HOST     = "chu-postgres"
POSTGRES_PORT     = "5432"
POSTGRES_DB       = "sih_data"
POSTGRES_USER     = "postgres"
POSTGRES_PASSWORD = "chu_postgres_2026"
POSTGRES_SCHEMA   = "public"

# URL JDBC de connexion PostgreSQL
JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    f"?currentSchema={POSTGRES_SCHEMA}"
)

# Propriétés JDBC communes
JDBC_PROPERTIES = {
    "user"                : POSTGRES_USER,
    "password"            : POSTGRES_PASSWORD,
    "driver"              : "org.postgresql.Driver",
    "fetchsize"           : "10000",          # Lignes lues par batch
    "socketTimeout"       : "60",             # Timeout socket en secondes
    "connectTimeout"      : "30",             # Timeout connexion en secondes
    "ApplicationName"     : "CHU_ETL_E1",     # Visible dans pg_stat_activity
}

# ─────────────────────────────────────────────────────────────────────────────
# HDFS
# ─────────────────────────────────────────────────────────────────────────────
HDFS_NAMENODE  = "hdfs://chu-namenode:9000"
BRONZE_BASE    = f"{HDFS_NAMENODE}/data/bronze/postgres"

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION DES TABLES À EXTRAIRE
# ─────────────────────────────────────────────────────────────────────────────
#
# Chaque table est configurée avec :
#   table_name      : Nom de la table PostgreSQL (ex: "PATIENT")
#   hdfs_folder     : Sous-dossier HDFS Bronze de destination
#   partition_col   : Colonne INT pour le partitionnement parallèle JDBC
#                     (None = extraction séquentielle monopasse)
#   num_partitions  : Nombre de partitions parallèles JDBC
#   lower_bound     : Valeur minimale de partition_col (pour JDBC parallèle)
#   upper_bound     : Valeur maximale de partition_col (pour JDBC parallèle)
#   row_count_hint  : Ordre de grandeur attendu (pour validation)
#   description     : Description métier
#
# REMARQUE : Les noms de tables PostgreSQL sont en minuscule mixte (ex: "Patient").
# Le champ jdbc_table utilise des guillemets SQL pour respecter la casse exacte.
# Le champ table_name (sans guillemets) sert uniquement aux logs et aux métadonnées.
TABLE_CONFIGS = [
    {
        "table_name"      : "Patient",
        "jdbc_table"      : 'public."Patient"',
        "hdfs_folder"     : "patient",
        "partition_col"   : "Id_patient",    # INTEGER NOT NULL, range 1-100000
        "num_partitions"  : 4,
        "lower_bound"     : 1,
        "upper_bound"     : 100_000,
        "row_count_hint"  : 100_000,
        "description"     : "Données démographiques des patients du CHU",
    },
    {
        "table_name"      : "Consultation",
        "jdbc_table"      : 'public."Consultation"',
        "hdfs_folder"     : "consultation",
        "partition_col"   : "Num_consultation",  # INTEGER NOT NULL, seq 1059020001+
        "num_partitions"  : 8,
        "lower_bound"     : 1_059_020_001,
        "upper_bound"     : 1_059_920_001,
        "row_count_hint"  : 1_027_000,
        "description"     : "Actes de consultation médicale",
    },
    {
        "table_name"      : "Diagnostic",
        "jdbc_table"      : 'public."Diagnostic"',
        "hdfs_folder"     : "diagnostic",
        "partition_col"   : None,
        "num_partitions"  : 1,
        "lower_bound"     : None,
        "upper_bound"     : None,
        "row_count_hint"  : 15_490,
        "description"     : "Codes diagnostics CIM-10",
    },
    {
        "table_name"      : "Professionnel_de_sante",
        "jdbc_table"      : 'public."Professionnel_de_sante"',
        "hdfs_folder"     : "professionnel_sante",
        "partition_col"   : None,             # PK = VARCHAR (Identifiant RPPS)
        "num_partitions"  : 1,
        "lower_bound"     : None,
        "upper_bound"     : None,
        "row_count_hint"  : 1_048_575,
        "description"     : "Médecins et professionnels de santé",
    },
    {
        "table_name"      : "Specialites",
        "jdbc_table"      : 'public."Specialites"',
        "hdfs_folder"     : "specialites",
        "partition_col"   : None,
        "num_partitions"  : 1,
        "lower_bound"     : None,
        "upper_bound"     : None,
        "row_count_hint"  : 93,
        "description"     : "Référentiel des spécialités médicales",
    },
    {
        "table_name"      : "Prescription",
        "jdbc_table"      : 'public."Prescription"',
        "hdfs_folder"     : "prescription",
        "partition_col"   : "Num_consultation",  # FK integer, même plage que Consultation
        "num_partitions"  : 4,
        "lower_bound"     : 1_059_020_001,
        "upper_bound"     : 1_059_920_001,
        "row_count_hint"  : 1_003_845,
        "description"     : "Prescriptions médicamenteuses",
    },
    {
        "table_name"      : "Medicaments",
        "jdbc_table"      : 'public."Medicaments"',
        "hdfs_folder"     : "medicaments",
        "partition_col"   : None,
        "num_partitions"  : 1,
        "lower_bound"     : None,
        "upper_bound"     : None,
        "row_count_hint"  : 15_455,
        "description"     : "Référentiel des médicaments (Code CIS)",
    },
    {
        "table_name"      : "Mutuelle",
        "jdbc_table"      : 'public."Mutuelle"',
        "hdfs_folder"     : "mutuelle",
        "partition_col"   : None,
        "num_partitions"  : 1,
        "lower_bound"     : None,
        "upper_bound"     : None,
        "row_count_hint"  : 254,
        "description"     : "Référentiel des mutuelles santé",
    },
    {
        "table_name"      : "Adher",
        "jdbc_table"      : 'public."Adher"',
        "hdfs_folder"     : "adher",
        "partition_col"   : "Id_patient",   # INTEGER, range 1-100000
        "num_partitions"  : 4,
        "lower_bound"     : 1,
        "upper_bound"     : 100_000,
        "row_count_hint"  : 96_671,
        "description"     : "Adhésions des patients aux mutuelles",
    },
    {
        "table_name"      : "Salle",
        "jdbc_table"      : 'public."Salle"',
        "hdfs_folder"     : "salle",
        "partition_col"   : None,             # PK = VARCHAR (Id_salle)
        "num_partitions"  : 1,
        "lower_bound"     : None,
        "upper_bound"     : None,
        "row_count_hint"  : 201_735,
        "description"     : "Affectations salles / consultations",
    },
    {
        "table_name"      : "Laboratoire",
        "jdbc_table"      : 'public."Laboratoire"',
        "hdfs_folder"     : "laboratoire",
        "partition_col"   : None,
        "num_partitions"  : 1,
        "lower_bound"     : None,
        "upper_bound"     : None,
        "row_count_hint"  : 677,
        "description"     : "Laboratoires pharmaceutiques",
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# FONCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    """Analyse les arguments de la ligne de commande."""
    parser = argparse.ArgumentParser(
        description="Job E1 - Extraction PostgreSQL SIH vers Bronze HDFS"
    )
    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        help="Date d'extraction (format YYYY-MM-DD). Défaut: aujourd'hui."
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        help="Liste de tables à extraire (défaut: toutes). Ex: PATIENT CONSULTATION"
    )
    parser.add_argument(
        "--mode",
        choices=["overwrite", "append"],
        default="overwrite",
        help="Mode d'écriture Parquet : overwrite (défaut) ou append"
    )
    return parser.parse_args()


def create_spark_session() -> SparkSession:
    """Crée la session Spark avec le connecteur JDBC PostgreSQL."""
    log.info("Initialisation de la session Spark...")
    spark = (
        SparkSession.builder
        .appName("CHU_E1_Extract_Postgres")
        .master("spark://chu-spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
        # Driver JDBC PostgreSQL
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        # Mémoire : 1 GB par exécuteur pour les grosses tables
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "2")
        # Optimisation des shuffles
        .config("spark.sql.shuffle.partitions", "4")
        # Compression des fichiers Parquet en sortie
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info(f"Session Spark créée - App ID : {spark.sparkContext.applicationId}")
    return spark


def extract_table(spark: SparkSession, config: dict, extraction_date: str) -> DataFrame:
    """
    Extrait une table PostgreSQL via JDBC et ajoute les colonnes de traçabilité.

    Pour les grosses tables (PATIENT, CONSULTATION, PRESCRIPTION, ADHER),
    utilise le partitionnement JDBC parallèle pour répartir la charge sur
    plusieurs Workers Spark.

    Args:
        spark          : Session Spark active
        config         : Configuration de la table (voir TABLE_CONFIGS)
        extraction_date: Date d'extraction au format YYYY-MM-DD

    Returns:
        DataFrame Spark enrichi avec colonnes de traçabilité
    """
    table = config["table_name"]
    log.info(f"\n  Extraction de '{table}'...")
    log.info(f"    Description : {config['description']}")
    log.info(f"    Lignes attendues : ~{config['row_count_hint']:,}")

    extract_start = datetime.now()

    try:
        # ── Lecture JDBC ──────────────────────────────────────────────────
        # Utilise jdbc_table (nom quoté) si disponible, sinon table_name
        jdbc_table = config.get("jdbc_table", table)

        if config["partition_col"] is not None:
            # EXTRACTION PARALLÈLE pour les grosses tables
            log.info(f"    Mode : Parallèle ({config['num_partitions']} partitions) "
                     f"sur colonne '{config['partition_col']}'")
            df = spark.read.jdbc(
                url=JDBC_URL,
                table=jdbc_table,
                column=config["partition_col"],
                lowerBound=config["lower_bound"],
                upperBound=config["upper_bound"],
                numPartitions=config["num_partitions"],
                properties=JDBC_PROPERTIES,
            )
        else:
            # EXTRACTION SÉQUENTIELLE pour les petites tables référentielles
            log.info(f"    Mode : Séquentiel (table référentielle)")
            df = spark.read.jdbc(
                url=JDBC_URL,
                table=jdbc_table,
                properties=JDBC_PROPERTIES,
            )

        # ── Normalisation des noms de colonnes ────────────────────────────
        # Passer les colonnes en minuscule pour standardisation
        for col_name in df.columns:
            if col_name != col_name.lower():
                df = df.withColumnRenamed(col_name, col_name.lower())

        # ── Ajout des colonnes de traçabilité ─────────────────────────────
        df = df \
            .withColumn("_extraction_date",
                        F.lit(extraction_date).cast("date")) \
            .withColumn("_extraction_ts",
                        F.lit(datetime.now().isoformat())) \
            .withColumn("_source_system",
                        F.lit("CHU_SIH_POSTGRESQL")) \
            .withColumn("_source_table",
                        F.lit(table)) \
            .withColumn("_source_db",
                        F.lit(f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"))

        duration = (datetime.now() - extract_start).total_seconds()
        nb_rows = df.count()

        log.info(f"    Lignes extraites : {nb_rows:,}")
        log.info(f"    Colonnes         : {len(df.columns)} (dont 5 traçabilité)")
        log.info(f"    Durée extraction : {duration:.1f}s")

        # Validation : alerte si écart > 20% avec le hint
        if config["row_count_hint"] > 0:
            ratio = nb_rows / config["row_count_hint"]
            if ratio < 0.8 or ratio > 1.2:
                log.warning(f"    ⚠️ Écart significatif avec l'estimation : "
                            f"{nb_rows:,} vs ~{config['row_count_hint']:,} "
                            f"(ratio: {ratio:.2f})")

        return df

    except Exception as e:
        log.error(f"    ❌ Erreur d'extraction pour '{table}' : {e}")
        raise


def write_to_bronze(df: DataFrame, config: dict, extraction_date: str, mode: str):
    """
    Écrit le DataFrame extrait en format Parquet dans la couche Bronze HDFS.
    Les fichiers sont partitionnés par date d'extraction.

    Structure cible :
      /data/bronze/postgres/{table}/extraction_date={YYYY-MM-DD}/part-*.snappy.parquet

    Args:
        df             : DataFrame à écrire
        config         : Configuration de la table
        extraction_date: Date d'extraction pour le partitionnement
        mode           : "overwrite" ou "append"
    """
    folder = config["hdfs_folder"]
    output_path = f"{BRONZE_BASE}/{folder}"

    log.info(f"\n    → Écriture Bronze HDFS : {output_path}")
    log.info(f"    → Mode              : {mode}")
    log.info(f"    → Partition         : extraction_date={extraction_date}")

    write_start = datetime.now()

    try:
        df.write \
            .mode(mode) \
            .partitionBy("_extraction_date") \
            .option("compression", "snappy") \
            .parquet(output_path)

        duration = (datetime.now() - write_start).total_seconds()
        log.info(f"    → ✅ Écriture réussie en {duration:.1f}s")
        log.info(f"    → Chemin HDFS : {output_path}/extraction_date={extraction_date}/")

    except Exception as e:
        log.error(f"    → ❌ Erreur d'écriture : {e}")
        raise


def verify_bronze_write(spark: SparkSession, config: dict, extraction_date: str) -> int:
    """
    Vérifie qu'un fichier Parquet Bronze a bien été écrit en le relisant.

    Returns:
        Nombre de lignes dans le fichier Bronze
    """
    folder = config["hdfs_folder"]
    output_path = f"{BRONZE_BASE}/{folder}/extraction_date={extraction_date}"

    try:
        df_check = spark.read.parquet(output_path)
        nb_rows = df_check.count()
        log.info(f"    → Vérification Bronze : {nb_rows:,} lignes relues depuis HDFS")
        return nb_rows
    except Exception as e:
        log.warning(f"    → Vérification impossible : {e}")
        return -1


def log_to_metadata(spark: SparkSession, job_results: list, extraction_date: str):
    """
    Enregistre les résultats du job dans PostgreSQL (etl_metadata.job_execution_log).
    Permet de tracer l'historique des extractions dans le tableau de bord ETL.
    """
    try:
        import json
        summary = {
            "job": "E1_extract_postgres",
            "date": extraction_date,
            "tables": [
                {
                    "table": r["table"],
                    "rows_extracted": r["rows"],
                    "status": r["status"],
                    "duration_s": r["duration"]
                }
                for r in job_results
            ]
        }

        total_rows = sum(r["rows"] for r in job_results if r["rows"] > 0)
        total_ok = sum(1 for r in job_results if r["status"] == "OK")
        status = "SUCCESS" if total_ok == len(job_results) else "PARTIAL"

        # Insertion directe via JDBC
        log_df = spark.createDataFrame([{
            "job_name"          : "E1_extract_postgres",
            "execution_date"    : extraction_date,
            "status"            : status,
            "records_processed" : total_rows,
            "records_inserted"  : total_rows,
            "records_failed"    : 0,
            "airflow_dag_id"    : "dag_chu_etl_daily",
            "airflow_task_id"   : "extract_postgres",
        }])

        log_df.write.jdbc(
            url=JDBC_URL,
            table="etl_metadata.job_execution_log",
            mode="append",
            properties=JDBC_PROPERTIES
        )
        log.info(f"  → Log ETL enregistré dans etl_metadata.job_execution_log")

    except Exception as e:
        log.warning(f"  → Impossible d'écrire le log ETL : {e}")


# ─────────────────────────────────────────────────────────────────────────────
# SCRIPT PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    extraction_date = args.date
    mode = args.mode

    start_time = datetime.now()

    log.info("=" * 70)
    log.info("  CHU ETL - JOB E1 : EXTRACTION POSTGRESQL → BRONZE HDFS")
    log.info(f"  Date d'extraction : {extraction_date}")
    log.info(f"  Source            : {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    log.info(f"  Destination       : {BRONZE_BASE}/")
    log.info(f"  Mode écriture     : {mode}")
    log.info(f"  Démarrage         : {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 70)

    spark = None
    job_results = []

    try:
        # ── ÉTAPE 1 : Session Spark ────────────────────────────────────────
        log.info("\n[ÉTAPE 1] Connexion au cluster Spark...")
        spark = create_spark_session()

        # ── ÉTAPE 2 : Sélection des tables ────────────────────────────────
        configs_to_run = TABLE_CONFIGS
        if args.tables:
            configs_to_run = [c for c in TABLE_CONFIGS if c["table_name"] in args.tables]
            log.info(f"\n[ÉTAPE 2] Tables sélectionnées : {[c['table_name'] for c in configs_to_run]}")
        else:
            log.info(f"\n[ÉTAPE 2] Extraction de toutes les {len(TABLE_CONFIGS)} tables...")

        # ── ÉTAPE 3 : Extraction + écriture Bronze table par table ────────
        log.info("\n[ÉTAPE 3] Extraction et chargement Bronze :")
        log.info("=" * 60)

        for i, config in enumerate(configs_to_run, 1):
            table = config["table_name"]
            table_start = datetime.now()

            log.info(f"\n  [{i}/{len(configs_to_run)}] TABLE : {table}")
            log.info(f"  {'─' * 50}")

            result = {"table": table, "rows": 0, "status": "ERROR", "duration": 0}

            try:
                # Extraction depuis PostgreSQL
                df = extract_table(spark, config, extraction_date)

                # Écriture Bronze HDFS
                write_to_bronze(df, config, extraction_date, mode)

                # Vérification de l'écriture
                nb_verified = verify_bronze_write(spark, config, extraction_date)

                duration = (datetime.now() - table_start).total_seconds()
                result = {
                    "table"   : table,
                    "rows"    : nb_verified if nb_verified > 0 else df.count(),
                    "status"  : "OK",
                    "duration": round(duration, 1)
                }
                log.info(f"\n  ✅ {table} → Bronze OK ({duration:.1f}s)")

            except Exception as e:
                duration = (datetime.now() - table_start).total_seconds()
                result = {"table": table, "rows": 0, "status": "ERROR", "duration": round(duration, 1)}
                log.error(f"\n  ❌ {table} → Erreur : {e}")

            job_results.append(result)

        # ── ÉTAPE 4 : Log des résultats dans PostgreSQL ────────────────────
        log.info("\n[ÉTAPE 4] Enregistrement des logs ETL...")
        log_to_metadata(spark, job_results, extraction_date)

        # ── RÉSUMÉ FINAL ──────────────────────────────────────────────────
        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds()
        total_rows = sum(r["rows"] for r in job_results)
        ok_count = sum(1 for r in job_results if r["status"] == "OK")
        err_count = sum(1 for r in job_results if r["status"] == "ERROR")

        log.info("\n" + "=" * 70)
        log.info("  RÉSUMÉ - JOB E1 : EXTRACTION POSTGRESQL → BRONZE")
        log.info("=" * 70)
        log.info(f"  Date d'extraction    : {extraction_date}")
        log.info(f"  Tables traitées      : {ok_count}/{len(configs_to_run)} ✅")
        log.info(f"  Tables en erreur     : {err_count} ❌")
        log.info(f"  Total lignes extraites : {total_rows:,}")
        log.info(f"  Durée totale         : {total_duration:.0f}s ({total_duration/60:.1f} min)")
        log.info(f"  Statut               : {'✅ SUCCÈS' if err_count == 0 else '❌ ERREUR'}")

        log.info("\n  Détail par table :")
        for r in job_results:
            status_icon = "✅" if r["status"] == "OK" else "❌"
            log.info(f"    {status_icon} {r['table']:<30} {r['rows']:>10,} lignes   {r['duration']:>6.1f}s")

        log.info("=" * 70)
        log.info("\nProchaine étape : Exécuter T1_clean_bronze.py")

        if err_count > 0:
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
