#!/usr/bin/env python3
# =============================================================================
# LIVRABLE 2 - Job E2 : CHARGEMENT CSV OPEN DATA → BRONZE HDFS
# =============================================================================
# Projet    : CHU Data Warehouse - Architecture Médaillon
# Auteur    : Groupe 6 BigData CESI
# Date      : Juin 2026
# Version   : 1.0
#
# DESCRIPTION :
#   Ce job PySpark charge les fichiers CSV des sources Open Data (hospitalisations,
#   décès, établissements de santé, professionnels de santé, satisfaction)
#   depuis la zone de staging HDFS et les écrit dans la couche Bronze du
#   Data Lake au format Parquet Snappy.
#
#   ARCHITECTURE DU FLUX :
#
#     [Fichiers CSV (Windows Host)]
#            ↓  docker cp + hdfs dfs -put  (script 00b_upload_csv_hdfs.sh)
#     [HDFS /data/staging/csv/...]
#            ↓  Spark read (ce script E2)
#     [HDFS /data/bronze/csv/...]  Parquet Snappy partitionné par date
#
#   Sources traitées :
#     1. hospitalisations   — Hospitalisations.csv           (~2 479 lignes)
#     2. deces              — deces.csv                      (France entière)
#     3. etablissements     — etablissement_sante.csv        (répertoire FINESS)
#     4. professionnel_open — professionnel_sante.csv        (RPPS/ADELI open)
#     5. satisfaction       — ESATIS48H_MCO_recueil2017_donnees.csv
#
# PRÉREQUIS :
#   1. Script 00_setup_hdfs.py exécuté (dossiers HDFS créés)
#   2. CSV uploadés dans HDFS via le script 00b_upload_csv_hdfs.sh :
#        docker exec chu-namenode bash /tmp/00b_upload_csv_hdfs.sh
#
# EXÉCUTION :
#   docker exec chu-spark-master spark-submit \
#     --master spark://chu-spark-master:7077 \
#     --conf spark.executor.memory=1g \
#     --conf spark.executor.cores=1 \
#     /opt/airflow/dags/jobs/E2_load_csv_bronze.py \
#     --date 2024-06-01
# =============================================================================

import sys
import argparse
import logging
from datetime import datetime, date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DateType, DoubleType
)

# ─────────────────────────────────────────────────────────────────────────────
# LOGGER
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("E2_load_csv_bronze")

# ─────────────────────────────────────────────────────────────────────────────
# HDFS
# ─────────────────────────────────────────────────────────────────────────────
HDFS_NAMENODE  = "hdfs://chu-namenode:9000"
STAGING_BASE   = f"{HDFS_NAMENODE}/data/staging/csv"   # Zone d'arrivée des CSV bruts
BRONZE_BASE    = f"{HDFS_NAMENODE}/data/bronze/csv"    # Zone Bronze Parquet Snappy

# ─────────────────────────────────────────────────────────────────────────────
# SCHÉMAS EXPLICITES DES CSV
# ─────────────────────────────────────────────────────────────────────────────
# Définir les schémas explicitement évite une lecture préalable pour inférence
# et garantit des types corrects dès la couche Bronze.

SCHEMA_HOSPITALISATIONS = StructType([
    StructField("Num_Hospitalisation",            IntegerType(), True),
    StructField("Id_patient",                     IntegerType(), True),
    StructField("identifiant_organisation",       StringType(),  True),
    StructField("Code_diagnostic",                StringType(),  True),
    StructField("Suite_diagnostic_consultation",  StringType(),  True),
    StructField("Date_Entree",                    StringType(),  True),  # dd/MM/yyyy → cast en Silver
    StructField("Jour_Hospitalisation",           IntegerType(), True),
])

SCHEMA_DECES = StructType([
    StructField("nom",                  StringType(), True),
    StructField("prenom",               StringType(), True),
    StructField("sexe",                 StringType(), True),  # 1=M, 2=F
    StructField("date_naissance",       StringType(), True),  # YYYY-MM-DD
    StructField("code_lieu_naissance",  StringType(), True),
    StructField("lieu_naissance",       StringType(), True),
    StructField("pays_naissance",       StringType(), True),
    StructField("date_deces",           StringType(), True),  # YYYY-MM-DD
    StructField("code_lieu_deces",      StringType(), True),
    StructField("numero_acte_deces",    StringType(), True),
])

SCHEMA_ETABLISSEMENTS = StructType([
    StructField("adresse",                          StringType(), True),
    StructField("cedex",                            StringType(), True),
    StructField("code_commune",                     StringType(), True),
    StructField("code_postal",                      StringType(), True),
    StructField("commune",                          StringType(), True),
    StructField("complement_destinataire",          StringType(), True),
    StructField("complement_point_geographique",    StringType(), True),
    StructField("email",                            StringType(), True),
    StructField("enseigne_commerciale_site",        StringType(), True),
    StructField("finess_etablissement_juridique",   StringType(), True),
    StructField("finess_site",                      StringType(), True),
    StructField("identifiant_organisation",         StringType(), True),
    StructField("indice_repetition_voie",           StringType(), True),
    StructField("mention_distribution",             StringType(), True),
    StructField("numero_voie",                      StringType(), True),
    StructField("pays",                             StringType(), True),
    StructField("raison_sociale_site",              StringType(), True),
    StructField("siren_site",                       StringType(), True),
    StructField("siret_site",                       StringType(), True),
    StructField("telecopie",                        StringType(), True),
    StructField("telephone",                        StringType(), True),
    StructField("telephone_2",                      StringType(), True),
    StructField("type_voie",                        StringType(), True),
    StructField("voie",                             StringType(), True),
])

SCHEMA_PROFESSIONNEL_OPEN = StructType([
    StructField("identifiant",               StringType(), True),
    StructField("civilite",                  StringType(), True),
    StructField("categorie_professionnelle", StringType(), True),
    StructField("nom",                       StringType(), True),
    StructField("prenom",                    StringType(), True),
    StructField("commune",                   StringType(), True),
    StructField("profession",                StringType(), True),
    StructField("specialite",                StringType(), True),
    StructField("type_identifiant",          StringType(), True),
])

# Satisfaction ESATIS — colonnes scores MCO 48h (2017)
# Seules les colonnes clés sont typées strictement ; les colonnes de scores
# (nombreuses et non-nullables) sont gardées en String pour robustesse,
# le typage numérique sera appliqué en Silver.
SCHEMA_SATISFACTION = StructType([
    StructField("finess",             StringType(), True),
    StructField("rs_finess",          StringType(), True),
    StructField("finess_geo",         StringType(), True),
    StructField("rs_finess_geo",      StringType(), True),
    StructField("region",             StringType(), True),
    StructField("participation",      StringType(), True),
    StructField("Depot",              StringType(), True),
    StructField("nb_rep_score_all_rea_ajust",       StringType(), True),
    StructField("score_all_rea_ajust",              StringType(), True),
    StructField("classement",                       StringType(), True),
    StructField("evolution",                        StringType(), True),
    StructField("nb_rep_score_accueil_rea_ajust",   StringType(), True),
    StructField("score_accueil_rea_ajust",          StringType(), True),
    StructField("nb_rep_score_PECinf_rea_ajust",    StringType(), True),
    StructField("score_PECinf_rea_ajust",           StringType(), True),
    StructField("nb_rep_score_PECmed_rea_ajust",    StringType(), True),
    StructField("score_PECmed_rea_ajust",           StringType(), True),
    StructField("nb_rep_score_chambre_rea_ajust",   StringType(), True),
    StructField("score_chambre_rea_ajust",          StringType(), True),
    StructField("nb_rep_score_repas_rea_ajust",     StringType(), True),
    StructField("score_repas_rea_ajust",            StringType(), True),
    StructField("nb_rep_score_sortie_rea_ajust",    StringType(), True),
    StructField("score_sortie_rea_ajust",           StringType(), True),
])

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION DES SOURCES CSV
# ─────────────────────────────────────────────────────────────────────────────
#
# Chaque source est configurée avec :
#   source_name    : Identifiant logique de la source (logs, métadonnées)
#   hdfs_input     : Chemin HDFS de staging (où le CSV a été uploadé)
#   hdfs_folder    : Sous-dossier de destination dans Bronze
#   separator      : Délimiteur CSV (;  ou ,)
#   encoding       : Encodage du fichier (UTF-8 ou ISO-8859-1)
#   header         : True si la première ligne est l'en-tête
#   schema         : StructType Spark (None = inférence automatique)
#   multiline      : True si des champs peuvent contenir des retours à la ligne
#   quote          : Caractère de délimitation des chaînes (par défaut ")
#   escape         : Caractère d'échappement (par défaut \)
#   row_count_hint : Estimation du nombre de lignes (validation)
#   description    : Description métier de la source
#
CSV_CONFIGS = [
    {
        "source_name"   : "hospitalisations",
        "hdfs_input"    : f"{STAGING_BASE}/hospitalisations/Hospitalisations.csv",
        "hdfs_folder"   : "hospitalisations",
        "separator"     : ";",
        "encoding"      : "UTF-8",
        "header"        : True,
        "schema"        : SCHEMA_HOSPITALISATIONS,
        "multiline"     : False,
        "quote"         : '"',
        "escape"        : "\\",
        "row_count_hint": 2_479,
        "description"   : "Hospitalisations du CHU (données internes SIH)",
    },
    {
        "source_name"   : "deces",
        "hdfs_input"    : f"{STAGING_BASE}/deces/deces.csv",
        "hdfs_folder"   : "deces",
        "separator"     : ",",
        "encoding"      : "UTF-8",
        "header"        : True,
        "schema"        : SCHEMA_DECES,
        "multiline"     : False,
        "quote"         : '"',
        "escape"        : "\\",
        "row_count_hint": 2_000_000,        # Fichier décès France ~ 2M lignes
        "description"   : "Décès en France (INSEE Open Data)",
    },
    {
        "source_name"   : "etablissements",
        "hdfs_input"    : f"{STAGING_BASE}/etablissements/etablissement_sante.csv",
        "hdfs_folder"   : "etablissements",
        "separator"     : ";",
        "encoding"      : "UTF-8",
        "header"        : True,
        "schema"        : SCHEMA_ETABLISSEMENTS,
        "multiline"     : True,             # Adresses peuvent contenir des \n
        "quote"         : '"',
        "escape"        : "\\",
        "row_count_hint": 15_000,
        "description"   : "Établissements de santé (FINESS Open Data)",
    },
    {
        "source_name"   : "professionnel_sante_open",
        "hdfs_input"    : f"{STAGING_BASE}/professionnel_sante_open/professionnel_sante.csv",
        "hdfs_folder"   : "professionnel_sante_open",
        "separator"     : ";",
        "encoding"      : "UTF-8",
        "header"        : True,
        "schema"        : SCHEMA_PROFESSIONNEL_OPEN,
        "multiline"     : False,
        "quote"         : '"',
        "escape"        : "\\",
        "row_count_hint": 10_000,
        "description"   : "Professionnels de santé (RPPS/ADELI Open Data)",
    },
    {
        "source_name"   : "satisfaction_esatis_2017",
        "hdfs_input"    : f"{STAGING_BASE}/satisfaction/ESATIS48H_MCO_recueil2017_donnees.csv",
        "hdfs_folder"   : "satisfaction",
        "separator"     : ";",
        "encoding"      : "UTF-8",
        "header"        : True,
        "schema"        : SCHEMA_SATISFACTION,
        "multiline"     : False,
        "quote"         : '"',
        "escape"        : "\\",
        "row_count_hint": 3_000,
        "description"   : "Satisfaction patients ESATIS MCO 48h (2017)",
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# FONCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    """Analyse les arguments de la ligne de commande."""
    parser = argparse.ArgumentParser(
        description="Job E2 - Chargement CSV Open Data vers Bronze HDFS"
    )
    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        help="Date d'extraction (format YYYY-MM-DD). Défaut: aujourd'hui."
    )
    parser.add_argument(
        "--sources",
        nargs="*",
        help=(
            "Noms de sources à charger (défaut: toutes). "
            "Ex: hospitalisations deces etablissements"
        )
    )
    parser.add_argument(
        "--mode",
        choices=["overwrite", "append"],
        default="overwrite",
        help="Mode d'écriture Parquet : overwrite (défaut) ou append"
    )
    return parser.parse_args()


def create_spark_session() -> SparkSession:
    """Crée la session Spark optimisée pour le traitement CSV."""
    log.info("Initialisation de la session Spark...")
    spark = (
        SparkSession.builder
        .appName("CHU_E2_Load_CSV_Bronze")
        .master("spark://chu-spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
        # Mémoire : 1 GB par exécuteur (fichiers CSV modérés)
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "2")
        # Optimisation des shuffles (peu nécessaire pour CSV → Parquet direct)
        .config("spark.sql.shuffle.partitions", "4")
        # Compression de sortie Parquet
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Tolérance aux fichiers CSV mal formés
        .config("spark.sql.csv.parser.columnPruning", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info(f"Session Spark créée - App ID : {spark.sparkContext.applicationId}")
    return spark


def check_hdfs_path_exists(spark: SparkSession, path: str) -> bool:
    """
    Vérifie qu'un chemin HDFS existe avant de tenter de le lire.

    Utilise le FileSystem Hadoop via Spark pour éviter un appel shell externe.

    Returns:
        True si le chemin existe, False sinon.
    """
    try:
        sc = spark.sparkContext
        hadoop_conf = sc._jvm.org.apache.hadoop.conf.Configuration()
        hadoop_conf.set("fs.defaultFS", HDFS_NAMENODE)
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        hdfs_path = sc._jvm.org.apache.hadoop.fs.Path(path)
        return fs.exists(hdfs_path)
    except Exception as e:
        log.warning(f"Impossible de vérifier l'existence de {path} : {e}")
        return False


def load_csv_source(
    spark: SparkSession,
    config: dict,
    extraction_date: str
) -> DataFrame:
    """
    Charge un fichier CSV depuis HDFS et ajoute les colonnes de traçabilité.

    Le CSV est lu depuis la zone de staging (/data/staging/csv/) avec le
    schéma défini dans CSV_CONFIGS. Aucune transformation métier n'est
    appliquée ici (nettoyage → T1, déduplication → T2).

    Args:
        spark          : Session Spark active
        config         : Configuration de la source (voir CSV_CONFIGS)
        extraction_date: Date d'extraction au format YYYY-MM-DD

    Returns:
        DataFrame Spark enrichi avec colonnes de traçabilité
    """
    source = config["source_name"]
    input_path = config["hdfs_input"]

    log.info(f"\n  Chargement de '{source}'...")
    log.info(f"    Description : {config['description']}")
    log.info(f"    Chemin HDFS : {input_path}")
    log.info(f"    Lignes attendues : ~{config['row_count_hint']:,}")

    # ── Vérification pré-lecture ──────────────────────────────────────────
    if not check_hdfs_path_exists(spark, input_path):
        raise FileNotFoundError(
            f"Fichier CSV introuvable dans HDFS : {input_path}\n"
            f"  → Exécutez d'abord le script 00b_upload_csv_hdfs.sh :\n"
            f"    docker exec chu-namenode bash /tmp/00b_upload_csv_hdfs.sh"
        )

    load_start = datetime.now()

    try:
        # ── Construction du reader CSV ────────────────────────────────────
        reader = (
            spark.read
            .option("sep",            config["separator"])
            .option("header",         str(config["header"]).lower())
            .option("encoding",       config["encoding"])
            .option("multiline",      str(config["multiline"]).lower())
            .option("quote",          config["quote"])
            .option("escape",         config["escape"])
            # Tolérance : les lignes corrompues sont logguées, pas abandonnées
            .option("mode",           "PERMISSIVE")
            # Colonne Spark interne de diagnostic (lignes malformées)
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            # Pas d'inférence automatique (on force le schéma fourni)
            .option("inferSchema",    "false")
        )

        # Appliquer le schéma explicite si disponible
        if config["schema"] is not None:
            df = reader.schema(config["schema"]).csv(input_path)
        else:
            # Fallback : tout en String (schéma non défini pour cette source)
            df = reader.csv(input_path)

        # ── Normalisation des noms de colonnes ────────────────────────────
        # Passer en minuscule pour cohérence avec les autres couches
        for col_name in df.columns:
            if col_name != col_name.lower():
                df = df.withColumnRenamed(col_name, col_name.lower())

        # ── Suppression des lignes totalement vides ───────────────────────
        # (fréquentes en fin de fichier CSV)
        non_meta_cols = [c for c in df.columns if not c.startswith("_")]
        df = df.dropna(how="all", subset=non_meta_cols)

        # ── Ajout des colonnes de traçabilité ─────────────────────────────
        df = df \
            .withColumn("_extraction_date",
                        F.lit(extraction_date).cast("date")) \
            .withColumn("_extraction_ts",
                        F.lit(datetime.now().isoformat())) \
            .withColumn("_source_system",
                        F.lit("CSV_OPEN_DATA")) \
            .withColumn("_source_name",
                        F.lit(source)) \
            .withColumn("_source_file",
                        F.lit(input_path.split("/")[-1]))

        duration = (datetime.now() - load_start).total_seconds()
        nb_rows  = df.count()

        log.info(f"    Lignes chargées : {nb_rows:,}")
        log.info(f"    Colonnes        : {len(df.columns)} (dont 5 traçabilité)")
        log.info(f"    Durée lecture   : {duration:.1f}s")

        # Validation : alerte si écart > 50% avec le hint
        # (plus souple qu'E1 car taille des CSV Open Data peut varier)
        if config["row_count_hint"] > 0:
            ratio = nb_rows / config["row_count_hint"]
            if ratio < 0.5 or ratio > 2.0:
                log.warning(
                    f"    ⚠️ Écart significatif avec l'estimation : "
                    f"{nb_rows:,} vs ~{config['row_count_hint']:,} "
                    f"(ratio: {ratio:.2f})"
                )

        # Alerte si lignes corrompues détectées
        if "_corrupt_record" in df.columns:
            nb_corrupt = df.filter(F.col("_corrupt_record").isNotNull()).count()
            if nb_corrupt > 0:
                log.warning(
                    f"    ⚠️ {nb_corrupt} ligne(s) malformée(s) détectée(s) "
                    f"(colonne _corrupt_record)"
                )

        return df

    except FileNotFoundError:
        raise
    except Exception as e:
        log.error(f"    ❌ Erreur de chargement pour '{source}' : {e}")
        raise


def write_to_bronze(
    df: DataFrame,
    config: dict,
    extraction_date: str,
    mode: str
):
    """
    Écrit le DataFrame en format Parquet Snappy dans la couche Bronze HDFS.

    Structure cible :
      /data/bronze/csv/{source}/extraction_date={YYYY-MM-DD}/part-*.snappy.parquet

    La partition sur _extraction_date permet un chargement incrémental et
    facilite la gestion du cycle de vie des données (rétention, expiration).

    Args:
        df             : DataFrame à écrire
        config         : Configuration de la source
        extraction_date: Date d'extraction pour le partitionnement
        mode           : "overwrite" ou "append"
    """
    folder      = config["hdfs_folder"]
    output_path = f"{BRONZE_BASE}/{folder}"

    log.info(f"\n    → Écriture Bronze HDFS : {output_path}")
    log.info(f"    → Mode              : {mode}")
    log.info(f"    → Partition         : extraction_date={extraction_date}")

    write_start = datetime.now()

    # Supprimer la colonne _corrupt_record avant l'écriture
    # (colonne interne Spark, non utile pour les étapes aval)
    if "_corrupt_record" in df.columns:
        df = df.drop("_corrupt_record")

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


def verify_bronze_write(
    spark: SparkSession,
    config: dict,
    extraction_date: str
) -> int:
    """
    Vérifie qu'un fichier Parquet Bronze a bien été écrit en le relisant.

    Returns:
        Nombre de lignes dans le fichier Bronze écrit.
    """
    folder      = config["hdfs_folder"]
    output_path = f"{BRONZE_BASE}/{folder}/extraction_date={extraction_date}"

    try:
        df_check = spark.read.parquet(output_path)
        nb_rows  = df_check.count()
        log.info(f"    → ✅ Vérification OK : {nb_rows:,} lignes dans Bronze")
        return nb_rows
    except Exception as e:
        log.warning(f"    → ⚠️ Vérification impossible : {e}")
        return -1


# ─────────────────────────────────────────────────────────────────────────────
# PROGRAMME PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    extraction_date = args.date

    log.info("=" * 70)
    log.info("  JOB E2 - CHARGEMENT CSV OPEN DATA → BRONZE HDFS")
    log.info(f"  Date d'extraction : {extraction_date}")
    log.info("=" * 70)

    # ── Sélection des sources à traiter ───────────────────────────────────
    if args.sources:
        configs_to_run = [
            c for c in CSV_CONFIGS
            if c["source_name"] in args.sources
        ]
        if not configs_to_run:
            log.error(
                f"Aucune source valide trouvée parmi : {args.sources}\n"
                f"Sources disponibles : {[c['source_name'] for c in CSV_CONFIGS]}"
            )
            sys.exit(1)
    else:
        configs_to_run = CSV_CONFIGS

    log.info(f"\n  Sources à traiter : {[c['source_name'] for c in configs_to_run]}")

    # ── Initialisation Spark ──────────────────────────────────────────────
    spark = create_spark_session()

    # ── Rapport de traitement ─────────────────────────────────────────────
    results = []
    errors  = []

    try:
        for config in configs_to_run:
            source = config["source_name"]
            log.info(f"\n{'─' * 60}")
            log.info(f"  [{configs_to_run.index(config)+1}/{len(configs_to_run)}] {source.upper()}")
            log.info(f"{'─' * 60}")

            try:
                # 1. Charger le CSV depuis HDFS staging
                df = load_csv_source(spark, config, extraction_date)

                # 2. Écrire en Bronze Parquet Snappy
                write_to_bronze(df, config, extraction_date, args.mode)

                # 3. Vérifier l'écriture
                nb_bronze = verify_bronze_write(spark, config, extraction_date)

                results.append({
                    "source"    : source,
                    "status"    : "SUCCESS",
                    "nb_lignes" : nb_bronze,
                    "hdfs_path" : f"{BRONZE_BASE}/{config['hdfs_folder']}",
                })

            except FileNotFoundError as e:
                log.error(f"  ❌ Fichier manquant : {e}")
                errors.append({"source": source, "erreur": str(e)})

            except Exception as e:
                log.error(f"  ❌ Erreur inattendue pour '{source}' : {e}")
                errors.append({"source": source, "erreur": str(e)})

    finally:
        # ── Rapport final ─────────────────────────────────────────────────
        log.info("\n" + "=" * 70)
        log.info("  RAPPORT FINAL - JOB E2")
        log.info("=" * 70)
        log.info(f"\n  Sources traitées  : {len(results)}/{len(configs_to_run)}")
        log.info(f"  Sources en erreur : {len(errors)}")

        if results:
            log.info("\n  ✅ Succès :")
            for r in results:
                log.info(
                    f"    - {r['source']:<30} "
                    f"{r['nb_lignes']:>10,} lignes → {r['hdfs_path']}"
                )

        if errors:
            log.info("\n  ❌ Erreurs :")
            for e in errors:
                log.info(f"    - {e['source']:<30} {e['erreur'][:80]}")
            log.info(
                "\n  💡 Pour résoudre les erreurs 'Fichier manquant' :\n"
                "     1. Copiez le script dans le namenode :\n"
                "        docker cp docker-infrastructure/scripts/00b_upload_csv_hdfs.sh "
                "chu-namenode:/tmp/\n"
                "     2. Exécutez le script :\n"
                "        docker exec chu-namenode bash /tmp/00b_upload_csv_hdfs.sh"
            )

        log.info("\n" + "=" * 70)
        spark.stop()

        if errors:
            sys.exit(1)


if __name__ == "__main__":
    main()
