#!/usr/bin/env python3
# =============================================================================
# LIVRABLE 2 - Job T4 : CONSTRUCTION DES DIMENSIONS GOLD (SCD Type 2)
# =============================================================================
# Projet    : CHU Data Warehouse - Architecture Médaillon
# Auteur    : Groupe 6 BigData CESI
# Date      : Juin 2026
# Version   : 1.0
#
# DESCRIPTION :
#   Ce job construit les 7 tables de dimensions du Data Warehouse Gold à
#   partir des données pseudonymisées de la couche Silver.
#
#   Dimensions produites (SCD = Slowly Changing Dimension) :
#     1. dim_temps        → Calendrier 2010-2030 (calculée, pas de Silver)
#     2. dim_patient      → SCD Type 2 (historique des changements)
#     3. dim_etablissement→ SCD Type 1 (dernière version)
#     4. dim_diagnostic   → SCD Type 1 (codes CIM-10)
#     5. dim_professionnel→ SCD Type 2 (historique des exercices)
#     6. dim_geographie   → SCD Type 1 (codes postaux → communes/depts)
#     7. dim_question     → Table statique (7 items ESATIS)
#
#   Clés de substitution (surrogate keys) : entiers séquentiels générés avec
#   monotonically_increasing_id() → garanties de croissance, non continues.
#
# PRÉREQUIS :
#   - Job T3_pseudonymize_rgpd.py exécuté avec succès
#
# EXÉCUTION :
#   docker exec chu-spark-master spark-submit \
#     --master spark://chu-spark-master:7077 \
#     /opt/airflow/dags/jobs/T4_build_dimensions.py \
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
    IntegerType, StringType, DateType, BooleanType, LongType, FloatType
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("T4_build_dimensions")

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTES
# ─────────────────────────────────────────────────────────────────────────────
HDFS_NAMENODE = "hdfs://chu-namenode:9000"
SILVER_BASE   = f"{HDFS_NAMENODE}/data/silver"
GOLD_DIMS     = f"{HDFS_NAMENODE}/data/gold/dimensions"

# Correspondance département → région (les 13 régions métropolitaines)
DEPT_TO_REGION = {
    "01": "Auvergne-Rhône-Alpes",   "03": "Auvergne-Rhône-Alpes",
    "07": "Auvergne-Rhône-Alpes",   "15": "Auvergne-Rhône-Alpes",
    "26": "Auvergne-Rhône-Alpes",   "38": "Auvergne-Rhône-Alpes",
    "42": "Auvergne-Rhône-Alpes",   "43": "Auvergne-Rhône-Alpes",
    "63": "Auvergne-Rhône-Alpes",   "69": "Auvergne-Rhône-Alpes",
    "73": "Auvergne-Rhône-Alpes",   "74": "Auvergne-Rhône-Alpes",
    "21": "Bourgogne-Franche-Comté","25": "Bourgogne-Franche-Comté",
    "39": "Bourgogne-Franche-Comté","58": "Bourgogne-Franche-Comté",
    "70": "Bourgogne-Franche-Comté","71": "Bourgogne-Franche-Comté",
    "89": "Bourgogne-Franche-Comté","90": "Bourgogne-Franche-Comté",
    "22": "Bretagne",               "29": "Bretagne",
    "35": "Bretagne",               "56": "Bretagne",
    "18": "Centre-Val de Loire",    "28": "Centre-Val de Loire",
    "36": "Centre-Val de Loire",    "37": "Centre-Val de Loire",
    "41": "Centre-Val de Loire",    "45": "Centre-Val de Loire",
    "2A": "Corse",                  "2B": "Corse",
    "08": "Grand Est",              "10": "Grand Est",
    "51": "Grand Est",              "52": "Grand Est",
    "54": "Grand Est",              "55": "Grand Est",
    "57": "Grand Est",              "67": "Grand Est",
    "68": "Grand Est",              "88": "Grand Est",
    "02": "Hauts-de-France",        "59": "Hauts-de-France",
    "60": "Hauts-de-France",        "62": "Hauts-de-France",
    "80": "Hauts-de-France",
    "75": "Île-de-France",          "77": "Île-de-France",
    "78": "Île-de-France",          "91": "Île-de-France",
    "92": "Île-de-France",          "93": "Île-de-France",
    "94": "Île-de-France",          "95": "Île-de-France",
    "14": "Normandie",              "27": "Normandie",
    "50": "Normandie",              "61": "Normandie",
    "76": "Normandie",
    "16": "Nouvelle-Aquitaine",     "17": "Nouvelle-Aquitaine",
    "19": "Nouvelle-Aquitaine",     "23": "Nouvelle-Aquitaine",
    "24": "Nouvelle-Aquitaine",     "33": "Nouvelle-Aquitaine",
    "40": "Nouvelle-Aquitaine",     "47": "Nouvelle-Aquitaine",
    "64": "Nouvelle-Aquitaine",     "79": "Nouvelle-Aquitaine",
    "86": "Nouvelle-Aquitaine",     "87": "Nouvelle-Aquitaine",
    "09": "Occitanie",              "11": "Occitanie",
    "12": "Occitanie",              "30": "Occitanie",
    "31": "Occitanie",              "32": "Occitanie",
    "34": "Occitanie",              "46": "Occitanie",
    "48": "Occitanie",              "65": "Occitanie",
    "66": "Occitanie",              "81": "Occitanie",
    "82": "Occitanie",
    "44": "Pays de la Loire",       "49": "Pays de la Loire",
    "53": "Pays de la Loire",       "72": "Pays de la Loire",
    "85": "Pays de la Loire",
    "04": "Provence-Alpes-Côte d'Azur","05": "Provence-Alpes-Côte d'Azur",
    "06": "Provence-Alpes-Côte d'Azur","13": "Provence-Alpes-Côte d'Azur",
    "83": "Provence-Alpes-Côte d'Azur","84": "Provence-Alpes-Côte d'Azur",
    "971": "Guadeloupe",            "972": "Martinique",
    "973": "Guyane",                "974": "La Réunion",
    "976": "Mayotte",
}


# ─────────────────────────────────────────────────────────────────────────────
# 1. DIM_TEMPS
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_temps(spark: SparkSession, extraction_date: str) -> dict:
    """
    Génère un calendrier complet 2010-2030 avec toutes les colonnes
    nécessaires aux analyses temporelles.

    Cette dimension est entièrement calculée (pas de source Silver).
    Granularité : jour (7306 lignes pour 2010-2030).

    Colonnes :
      id_temps, date_complete, annee, trimestre, mois, semaine_annee,
      jour_annee, jour_mois, jour_semaine, nom_jour, nom_mois,
      est_week_end, est_jour_ferie, saison
    """
    log.info("  [1/7] Construction dim_temps (calendrier 2010-2030)...")

    # Génération de la séquence de dates avec Spark
    df = spark.range(0, 7306) \
        .withColumn("date_complete",
                    F.date_add(F.lit("2010-01-01"), F.col("id").cast(IntegerType()))) \
        .withColumn("id_temps", F.col("id").cast(IntegerType()) + 1) \
        .withColumn("annee",          F.year("date_complete")) \
        .withColumn("trimestre",      F.quarter("date_complete")) \
        .withColumn("mois",           F.month("date_complete")) \
        .withColumn("semaine_annee",  F.weekofyear("date_complete")) \
        .withColumn("jour_annee",     F.dayofyear("date_complete")) \
        .withColumn("jour_mois",      F.dayofmonth("date_complete")) \
        .withColumn("jour_semaine",   F.dayofweek("date_complete")) \
        .withColumn("nom_jour",
            F.when(F.dayofweek("date_complete") == 1, "Dimanche")
             .when(F.dayofweek("date_complete") == 2, "Lundi")
             .when(F.dayofweek("date_complete") == 3, "Mardi")
             .when(F.dayofweek("date_complete") == 4, "Mercredi")
             .when(F.dayofweek("date_complete") == 5, "Jeudi")
             .when(F.dayofweek("date_complete") == 6, "Vendredi")
             .otherwise("Samedi")
        ) \
        .withColumn("nom_mois",
            F.when(F.month("date_complete") == 1, "Janvier")
             .when(F.month("date_complete") == 2, "Février")
             .when(F.month("date_complete") == 3, "Mars")
             .when(F.month("date_complete") == 4, "Avril")
             .when(F.month("date_complete") == 5, "Mai")
             .when(F.month("date_complete") == 6, "Juin")
             .when(F.month("date_complete") == 7, "Juillet")
             .when(F.month("date_complete") == 8, "Août")
             .when(F.month("date_complete") == 9, "Septembre")
             .when(F.month("date_complete") == 10, "Octobre")
             .when(F.month("date_complete") == 11, "Novembre")
             .otherwise("Décembre")
        ) \
        .withColumn("est_week_end",
            F.dayofweek("date_complete").isin([1, 7])
        ) \
        .withColumn("saison",
            F.when(F.month("date_complete").isin([12, 1, 2]),  "Hiver")
             .when(F.month("date_complete").isin([3, 4, 5]),   "Printemps")
             .when(F.month("date_complete").isin([6, 7, 8]),   "Été")
             .otherwise("Automne")
        ) \
        .withColumn("est_jour_ferie", F.lit(False))  # Simplification

    # Sélection finale
    df_final = df.select(
        "id_temps", "date_complete", "annee", "trimestre", "mois",
        "semaine_annee", "jour_annee", "jour_mois", "jour_semaine",
        "nom_jour", "nom_mois", "est_week_end", "est_jour_ferie", "saison"
    )

    path = f"{GOLD_DIMS}/dim_temps"
    df_final.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(path)

    cnt = df_final.count()
    log.info(f"    dim_temps → {cnt:,} jours (2010-2030) ✅")
    return {"dim": "dim_temps", "count": cnt}


# ─────────────────────────────────────────────────────────────────────────────
# 2. DIM_PATIENT (SCD Type 2)
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_patient(spark: SparkSession, extraction_date: str) -> dict:
    """
    Construit dim_patient depuis Silver/patient_rgpd (pseudonymisé).
    SCD Type 2 : clé naturelle = id_patient_hash.

    Colonnes Gold :
      sk_patient (surrogate key), id_patient_hash, sexe, tranche_age,
      code_postal, departement, est_courant, date_debut_validite, date_fin_validite
    """
    log.info("  [2/7] Construction dim_patient (SCD Type 2)...")

    path_in  = f"{SILVER_BASE}/patient_rgpd"
    path_out = f"{GOLD_DIMS}/dim_patient"

    df = spark.read.parquet(path_in)

    # Clé de substitution (surrogate key)
    df_gold = df \
        .withColumn("sk_patient", F.monotonically_increasing_id() + 1) \
        .select(
            "sk_patient",
            "id_patient_hash",
            F.col("sexe"),
            F.col("tranche_age"),
            F.coalesce(F.col("code_postal"), F.lit("00000")).alias("code_postal"),
            F.coalesce(F.col("departement"), F.lit("INCONNU")).alias("departement"),
            F.col("est_courant"),
            F.col("date_debut_validite"),
            F.col("date_fin_validite"),
        )

    df_gold.write.mode("overwrite").option("compression", "snappy").parquet(path_out)

    cnt = df_gold.count()
    log.info(f"    dim_patient → {cnt:,} lignes ✅")
    return {"dim": "dim_patient", "count": cnt}


# ─────────────────────────────────────────────────────────────────────────────
# 3. DIM_ETABLISSEMENT
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_etablissement(spark: SparkSession, extraction_date: str) -> dict:
    """
    Construit dim_etablissement depuis les fichiers CSV des établissements de santé.
    SCD Type 1 : on garde toujours la version la plus récente.
    """
    log.info("  [3/7] Construction dim_etablissement (SCD Type 1)...")

    path_csv = f"{HDFS_NAMENODE}/data/bronze/csv/etablissements_sante"
    path_out = f"{GOLD_DIMS}/dim_etablissement"

    try:
        df = spark.read.option("header", "true").option("sep", ";").csv(path_csv)
        df_gold = df \
            .dropDuplicates(["finess_et"]) \
            .withColumn("sk_etablissement", F.monotonically_increasing_id() + 1) \
            .withColumnRenamed("finess_et", "id_etablissement_src") \
            .withColumnRenamed("rs", "nom_etablissement") \
            .withColumnRenamed("categ_etab", "categorie") \
            .withColumnRenamed("departement_code", "departement") \
            .select("sk_etablissement", "id_etablissement_src",
                    "nom_etablissement", "categorie", "departement")
    except Exception:
        # Données non chargées en Bronze CSV → créer une dimension vide
        log.warning("    ⚠️  CSV établissements non trouvés → dimension vide (skeleton)")
        schema = StructType([
            StructField("sk_etablissement",     LongType(),   False),
            StructField("id_etablissement_src", StringType(), True),
            StructField("nom_etablissement",    StringType(), True),
            StructField("categorie",            StringType(), True),
            StructField("departement",          StringType(), True),
        ])
        df_gold = spark.createDataFrame([], schema)

    df_gold.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(path_out)
    cnt = df_gold.count()
    log.info(f"    dim_etablissement → {cnt:,} lignes ✅")
    return {"dim": "dim_etablissement", "count": cnt}


# ─────────────────────────────────────────────────────────────────────────────
# 4. DIM_DIAGNOSTIC (CIM-10)
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_diagnostic(spark: SparkSession, extraction_date: str) -> dict:
    """
    Construit dim_diagnostic depuis Silver/diagnostic_dedup.
    Enrichit les codes CIM-10 avec le regroupement hiérarchique en grands chapitres.
    """
    log.info("  [4/7] Construction dim_diagnostic (codes CIM-10)...")

    path_in  = f"{SILVER_BASE}/diagnostic_dedup"
    path_out = f"{GOLD_DIMS}/dim_diagnostic"

    df = spark.read.parquet(path_in)

    # Extraction du chapitre CIM-10 (première lettre du code)
    df_gold = df \
        .withColumn("sk_diagnostic", F.monotonically_increasing_id() + 1) \
        .withColumn("chapitre_cim10", F.substring(F.col("code_cim10"), 1, 1)) \
        .withColumn("libelle_chapitre",
            F.when(F.col("chapitre_cim10") == "A", "Maladies infectieuses et parasitaires")
             .when(F.col("chapitre_cim10") == "B", "Maladies infectieuses et parasitaires")
             .when(F.col("chapitre_cim10") == "C", "Tumeurs malignes")
             .when(F.col("chapitre_cim10") == "D", "Tumeurs bénignes / Maladies du sang")
             .when(F.col("chapitre_cim10") == "E", "Maladies endocriniennes et nutritionnelles")
             .when(F.col("chapitre_cim10") == "F", "Troubles mentaux et du comportement")
             .when(F.col("chapitre_cim10") == "G", "Maladies du système nerveux")
             .when(F.col("chapitre_cim10") == "H", "Maladies de l'œil / oreille")
             .when(F.col("chapitre_cim10") == "I", "Maladies de l'appareil circulatoire")
             .when(F.col("chapitre_cim10") == "J", "Maladies de l'appareil respiratoire")
             .when(F.col("chapitre_cim10") == "K", "Maladies de l'appareil digestif")
             .when(F.col("chapitre_cim10") == "L", "Maladies de la peau")
             .when(F.col("chapitre_cim10") == "M", "Maladies ostéo-articulaires")
             .when(F.col("chapitre_cim10") == "N", "Maladies de l'appareil génito-urinaire")
             .when(F.col("chapitre_cim10") == "O", "Grossesse, accouchement, post-partum")
             .when(F.col("chapitre_cim10") == "P", "Certaines affections - période périnatale")
             .when(F.col("chapitre_cim10") == "Q", "Malformations congénitales")
             .when(F.col("chapitre_cim10") == "R", "Symptômes et signes anormaux")
             .when(F.col("chapitre_cim10") == "S", "Lésions traumatiques")
             .when(F.col("chapitre_cim10") == "T", "Empoisonnements et effets toxiques")
             .when(F.col("chapitre_cim10") == "V", "Causes externes de traumatismes")
             .when(F.col("chapitre_cim10") == "W", "Causes externes - accidents")
             .when(F.col("chapitre_cim10") == "X", "Causes externes - intentionnelles")
             .when(F.col("chapitre_cim10") == "Y", "Causes externes - indéterminées")
             .when(F.col("chapitre_cim10") == "Z", "Facteurs influençant l'état de santé")
             .otherwise("Autre / Non classé")
        ) \
        .select("sk_diagnostic", "code_cim10", "libelle_diagnostic",
                "chapitre_cim10", "libelle_chapitre")

    df_gold.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(path_out)
    cnt = df_gold.count()
    log.info(f"    dim_diagnostic → {cnt:,} lignes ✅")
    return {"dim": "dim_diagnostic", "count": cnt}


# ─────────────────────────────────────────────────────────────────────────────
# 5. DIM_PROFESSIONNEL (SCD Type 2)
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_professionnel(spark: SparkSession, extraction_date: str) -> dict:
    """
    Construit dim_professionnel depuis Silver/professionnel_sante_rgpd.
    SCD Type 2 (les changements de spécialité / mode d'exercice sont historisés).
    """
    log.info("  [5/7] Construction dim_professionnel (SCD Type 2)...")

    path_in  = f"{SILVER_BASE}/professionnel_sante_rgpd"
    path_out = f"{GOLD_DIMS}/dim_professionnel"

    df = spark.read.parquet(path_in)

    df_gold = df \
        .withColumn("sk_professionnel", F.monotonically_increasing_id() + 1) \
        .select(
            "sk_professionnel",
            "id_professionnel_hash",
            F.coalesce(F.col("specialite"), F.lit("NON RENSEIGNÉE")).alias("specialite"),
            F.coalesce(F.col("mode_exercice"), F.lit("INCONNU")).alias("mode_exercice"),
            F.coalesce(F.col("categorie_pro"), F.lit("NON RENSEIGNÉE")).alias("categorie_pro"),
            F.col("est_courant"),
            F.col("date_debut_validite"),
            F.col("date_fin_validite"),
        )

    df_gold.write.mode("overwrite").option("compression", "snappy").parquet(path_out)
    cnt = df_gold.count()
    log.info(f"    dim_professionnel → {cnt:,} lignes ✅")
    return {"dim": "dim_professionnel", "count": cnt}


# ─────────────────────────────────────────────────────────────────────────────
# 6. DIM_GEOGRAPHIE
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_geographie(spark: SparkSession, extraction_date: str) -> dict:
    """
    Construit dim_geographie depuis les codes postaux distincts de Silver/patient_rgpd.
    Enrichit avec département et région via le mapping statique DEPT_TO_REGION.
    """
    log.info("  [6/7] Construction dim_geographie (codes postaux → régions)...")

    path_in  = f"{SILVER_BASE}/patient_rgpd"
    path_out = f"{GOLD_DIMS}/dim_geographie"

    df = spark.read.parquet(path_in)

    # Extraire les codes postaux uniques
    df_geo = df.select("code_postal", "departement").dropDuplicates()

    # Broadcast le mapping département → région
    mapping_list = [(k, v) for k, v in DEPT_TO_REGION.items()]
    schema_mapping = StructType([
        StructField("departement", StringType(), True),
        StructField("region", StringType(), True)
    ])
    df_mapping = spark.createDataFrame(mapping_list, schema_mapping)
    df_mapping_bc = F.broadcast(df_mapping)

    df_gold = df_geo \
        .join(df_mapping_bc, on="departement", how="left") \
        .withColumn("sk_geographie", F.monotonically_increasing_id() + 1) \
        .withColumn("region", F.coalesce(F.col("region"), F.lit("Non déterminée"))) \
        .select("sk_geographie", "code_postal", "departement", "region")

    df_gold.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(path_out)
    cnt = df_gold.count()
    log.info(f"    dim_geographie → {cnt:,} codes postaux ✅")
    return {"dim": "dim_geographie", "count": cnt}


# ─────────────────────────────────────────────────────────────────────────────
# 7. DIM_QUESTION (ESATIS - table statique)
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_question(spark: SparkSession, extraction_date: str) -> dict:
    """
    Construit dim_question : les 7 dimensions de satisfaction ESATIS (HAS).
    Table statique (ne dépend pas de Silver).

    Les dimensions ESATIS correspondent aux 7 thèmes évalués par les patients
    (source : HAS - Haute Autorité de Santé, referentiel ESATIS 48h MCO).
    """
    log.info("  [7/7] Construction dim_question (ESATIS HAS)...")

    path_out = f"{GOLD_DIMS}/dim_question"

    questions_data = [
        (1, "ACCOMP",  "Accompagnement",         "Accueil et accompagnement à l'entrée"),
        (2, "CHAMBRE", "Chambre et environnement","Propreté et confort de la chambre"),
        (3, "REPAS",   "Repas",                   "Qualité et service des repas"),
        (4, "DOULEUR", "Prise en charge douleur", "Gestion de la douleur pendant le séjour"),
        (5, "INFOS",   "Information patient",     "Qualité des informations reçues"),
        (6, "SORTIE",  "Organisation sortie",     "Préparation et organisation de la sortie"),
        (7, "GLOBAL",  "Satisfaction globale",    "Note de satisfaction générale du séjour"),
    ]

    schema = StructType([
        StructField("sk_question",   IntegerType(), False),
        StructField("code_question", StringType(),  True),
        StructField("libelle_court", StringType(),  True),
        StructField("libelle_long",  StringType(),  True),
    ])

    df_gold = spark.createDataFrame(questions_data, schema)
    df_gold.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(path_out)
    cnt = df_gold.count()
    log.info(f"    dim_question → {cnt:,} thèmes ESATIS ✅")
    return {"dim": "dim_question", "count": cnt}


# ─────────────────────────────────────────────────────────────────────────────
# SCRIPT PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=date.today().isoformat())
    return parser.parse_args()


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("CHU_T4_Build_Dimensions")
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


def main():
    args = parse_args()
    extraction_date = args.date
    start_time = datetime.now()

    log.info("=" * 70)
    log.info("  CHU ETL - JOB T4 : CONSTRUCTION DES DIMENSIONS GOLD")
    log.info(f"  Date extraction  : {extraction_date}")
    log.info(f"  Démarrage        : {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 70)

    spark = None
    results = []

    try:
        spark = create_spark_session()

        builders = [
            build_dim_temps,
            build_dim_patient,
            build_dim_etablissement,
            build_dim_diagnostic,
            build_dim_professionnel,
            build_dim_geographie,
            build_dim_question,
        ]

        for build_fn in builders:
            try:
                r = build_fn(spark, extraction_date)
                results.append({**r, "status": "OK"})
            except Exception as e:
                name = build_fn.__name__.replace("build_", "")
                log.error(f"  ❌ {name} : {e}", exc_info=True)
                results.append({"dim": name, "count": 0, "status": "ERROR"})

        # Rapport
        log.info("\n" + "=" * 60)
        log.info("  RAPPORT DIMENSIONS GOLD")
        log.info("=" * 60)
        total_rows = 0
        for r in results:
            status = "✅" if r["status"] == "OK" else "❌"
            log.info(f"    {status} {r['dim']:<25} {r['count']:>10,} lignes")
            total_rows += r["count"]
        log.info(f"  {'─'*50}")
        log.info(f"    TOTAL                      {total_rows:>10,} lignes")

        duration = (datetime.now() - start_time).total_seconds()
        errors = sum(1 for r in results if r["status"] != "OK")
        log.info(f"\n  Durée : {duration:.0f}s  |  Erreurs : {errors}")
        log.info("Prochaine étape : Exécuter T5_build_facts.py")

        if errors > 0:
            sys.exit(1)

    except Exception as e:
        log.error(f"Erreur critique : {e}", exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
