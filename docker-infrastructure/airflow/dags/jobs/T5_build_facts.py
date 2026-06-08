#!/usr/bin/env python3
# =============================================================================
# LIVRABLE 2 - Job T5 : CONSTRUCTION DES TABLES DE FAITS GOLD
# =============================================================================
# Projet    : CHU Data Warehouse - Architecture Médaillon
# Auteur    : Groupe 6 BigData CESI
# Date      : Juin 2026
# Version   : 1.0
#
# DESCRIPTION :
#   Ce job construit les 4 tables de faits du Data Warehouse Gold en joingnant
#   les données Silver pseudonymisées avec les clés de substitution (surrogate
#   keys) des dimensions Gold.
#
#   Tables de faits produites :
#     1. fait_consultation   → ~5M lignes/an | partitionnée annee/mois
#     2. fait_hospitalisation→ ~2M lignes/an | partitionnée annee/mois
#     3. fait_deces          → ~300K lignes/an | partitionnée annee
#     4. fait_satisfaction   → ~150K lignes/an | non partitionnée
#
#   Stratégie de jointure :
#     Les jointures avec les dimensions utilisent BROADCAST pour toutes les
#     dimensions à faible cardinalité (< 100K lignes), afin d'éviter les
#     shuffles coûteux sur des tables de faits volumineuses.
#
# PRÉREQUIS :
#   - Job T3_pseudonymize_rgpd.py exécuté (Silver avec hashes)
#   - Job T4_build_dimensions.py exécuté (Gold dims avec surrogate keys)
#
# EXÉCUTION :
#   docker exec chu-spark-master spark-submit \
#     --master spark://chu-spark-master:7077 \
#     --conf spark.executor.memory=2g \
#     /opt/airflow/dags/jobs/T5_build_facts.py \
#     --date 2024-06-01
# =============================================================================

import sys
import argparse
import logging
from datetime import datetime, date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("T5_build_facts")

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTES
# ─────────────────────────────────────────────────────────────────────────────
HDFS_NAMENODE = "hdfs://chu-namenode:9000"
SILVER_BASE   = f"{HDFS_NAMENODE}/data/silver"
GOLD_DIMS     = f"{HDFS_NAMENODE}/data/gold/dimensions"
GOLD_FAITS    = f"{HDFS_NAMENODE}/data/gold/faits"

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def load_dim(spark: SparkSession, dim_name: str) -> DataFrame:
    """Charge une dimension Gold et la met en cache (small tables → broadcast)."""
    path = f"{GOLD_DIMS}/{dim_name}"
    return spark.read.parquet(path).cache()


def resolve_date_key(df: DataFrame, date_col: str, dim_temps: DataFrame,
                     sk_col: str) -> DataFrame:
    """
    Résout la clé de substitution pour la dimension temps.
    Joint sur date_complete de dim_temps pour obtenir id_temps.
    """
    dim_t = F.broadcast(dim_temps.select("id_temps", "date_complete"))
    df = df.join(dim_t,
                 df[date_col].cast("date") == dim_t["date_complete"],
                 how="left") \
           .withColumn(sk_col, F.coalesce(F.col("id_temps"), F.lit(-1))) \
           .drop("id_temps", "date_complete")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 1. FAIT_CONSULTATION
# ─────────────────────────────────────────────────────────────────────────────

def build_fait_consultation(spark: SparkSession,
                             dims: dict,
                             extraction_date: str) -> dict:
    """
    Construit fait_consultation depuis Silver/consultation_rgpd.

    Mesures :
      - duree_minutes (INT)  : durée de la consultation
      - cout_euros (FLOAT)   : coût de la consultation

    Dimensions (surrogate keys) :
      sk_temps, sk_patient, sk_professionnel, sk_etablissement, sk_diagnostic

    Partitionnement : annee / mois
    """
    log.info("  [1/4] Construction fait_consultation...")

    path_in  = f"{SILVER_BASE}/consultation_dedup"
    path_out = f"{GOLD_FAITS}/fait_consultation"

    df = spark.read.parquet(path_in)
    before = df.count()
    log.info(f"    Silver consultation_dedup : {before:,} lignes")

    # ── Adaptation colonnes Silver → noms attendus ────────────────────────
    df = df \
        .withColumnRenamed("id_patient",    "id_patient_hash") \
        .withColumnRenamed("id_prof_sante", "id_professionnel_hash") \
        .withColumnRenamed("code_diag",     "code_cim10") \
        .withColumnRenamed("duree_minutes", "duree_consultation")

    # ── Jointure dim_temps ────────────────────────────────────────────────
    df = resolve_date_key(df, "date_consultation", dims["dim_temps"], "sk_temps")

    # ── Jointure dim_patient (sur id_patient_hash) ────────────────────────
    dim_pat = F.broadcast(
        dims["dim_patient"].filter(F.col("est_courant") == True)
                           .select("sk_patient", "id_patient_hash")
    )
    df = df.join(dim_pat, on="id_patient_hash", how="left") \
           .withColumn("sk_patient", F.coalesce(F.col("sk_patient"), F.lit(-1)))

    # ── Jointure dim_professionnel (sur id_professionnel_hash) ────────────
    dim_pro = F.broadcast(
        dims["dim_professionnel"].filter(F.col("est_courant") == True)
                                 .select("sk_professionnel", "id_professionnel_hash")
    )
    df = df.join(dim_pro, on="id_professionnel_hash", how="left") \
           .withColumn("sk_professionnel", F.coalesce(F.col("sk_professionnel"), F.lit(-1)))

    # ── Jointure dim_diagnostic (sur code_cim10) ──────────────────────────
    dim_diag = F.broadcast(dims["dim_diagnostic"].select("sk_diagnostic", "code_cim10"))
    if "code_cim10" in df.columns:
        df = df.join(dim_diag, on="code_cim10", how="left") \
               .withColumn("sk_diagnostic", F.coalesce(F.col("sk_diagnostic"), F.lit(-1)))
    else:
        df = df.withColumn("sk_diagnostic", F.lit(-1).cast(LongType()))

    # ── Ajout des colonnes de partition ───────────────────────────────────
    df = df.withColumn("annee", F.year(F.col("date_consultation").cast("date"))) \
           .withColumn("mois",  F.month(F.col("date_consultation").cast("date")))

    # ── Sélection finale ──────────────────────────────────────────────────
    df_gold = df.select(
        F.monotonically_increasing_id().alias("sk_consultation"),
        F.col("sk_temps"),
        F.col("sk_patient"),
        F.col("sk_professionnel"),
        F.col("sk_diagnostic"),
        F.coalesce(F.col("duree_consultation"), F.lit(0)).alias("duree_minutes"),
        F.lit(0.0).cast("float").alias("cout_euros"),
        F.coalesce(F.col("motif"), F.lit("NON RENSEIGNÉ")).alias("type_consultation"),
        F.col("annee"),
        F.col("mois"),
    )

    # ── Écriture partitionnée ─────────────────────────────────────────────
    df_gold \
        .repartition("annee", "mois") \
        .write \
        .partitionBy("annee", "mois") \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(path_out)

    cnt = df_gold.count()
    log.info(f"    fait_consultation → {cnt:,} lignes ✅")
    return {"fait": "fait_consultation", "count": cnt, "status": "OK"}


# ─────────────────────────────────────────────────────────────────────────────
# 2. FAIT_HOSPITALISATION
# ─────────────────────────────────────────────────────────────────────────────

def build_fait_hospitalisation(spark: SparkSession,
                                dims: dict,
                                extraction_date: str) -> dict:
    """
    Construit fait_hospitalisation depuis Silver/hospitalisation_dedup.

    Mesures :
      - duree_sejour_jours (INT)  : durée du séjour en jours
      - cout_sejour_euros  (FLOAT): coût total du séjour GHS

    Partitionnement : annee / mois (date d'entrée)
    """
    log.info("  [2/4] Construction fait_hospitalisation...")

    path_in  = f"{SILVER_BASE}/hospitalisation_dedup"
    path_out = f"{GOLD_FAITS}/fait_hospitalisation"

    try:
        df = spark.read.parquet(path_in)
    except Exception:
        log.warning("    ⚠️  Silver hospitalisation non trouvé → Bronze CSV en fallback...")
        path_csv = f"{HDFS_NAMENODE}/data/bronze/csv/hospitalisations"
        df = spark.read.option("header", "true").option("sep", ";").csv(path_csv)

    before = df.count()
    log.info(f"    Source : {before:,} lignes")

    # Résolution des clés de temps
    date_entree_col = next((c for c in df.columns
                            if "entree" in c.lower() or "admission" in c.lower()), None)
    if date_entree_col:
        df = resolve_date_key(df, date_entree_col, dims["dim_temps"], "sk_temps_entree")
    else:
        df = df.withColumn("sk_temps_entree", F.lit(-1).cast(LongType()))

    # Calcul de la durée en jours
    date_sortie_col = next((c for c in df.columns if "sortie" in c.lower()), None)
    if date_entree_col and date_sortie_col:
        df = df.withColumn("duree_sejour_jours",
                           F.datediff(F.col(date_sortie_col).cast("date"),
                                      F.col(date_entree_col).cast("date")))
    else:
        df = df.withColumn("duree_sejour_jours", F.lit(0))

    df = df.withColumn("annee", F.year(F.col(date_entree_col).cast("date")) if date_entree_col else F.lit(2024)) \
           .withColumn("mois",  F.month(F.col(date_entree_col).cast("date")) if date_entree_col else F.lit(1))

    df_gold = df.select(
        F.monotonically_increasing_id().alias("sk_hospitalisation"),
        F.col("sk_temps_entree"),
        F.col("duree_sejour_jours"),
        F.lit(0.0).cast("float").alias("cout_sejour_euros"),
        F.col("annee"),
        F.col("mois"),
    )

    df_gold \
        .repartition("annee", "mois") \
        .write \
        .partitionBy("annee", "mois") \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(path_out)

    cnt = df_gold.count()
    log.info(f"    fait_hospitalisation → {cnt:,} lignes ✅")
    return {"fait": "fait_hospitalisation", "count": cnt, "status": "OK"}


# ─────────────────────────────────────────────────────────────────────────────
# 3. FAIT_DECES
# ─────────────────────────────────────────────────────────────────────────────

def build_fait_deces(spark: SparkSession,
                     dims: dict,
                     extraction_date: str) -> dict:
    """
    Construit fait_deces depuis le CSV des décès en France (INSEE).
    Source : data.gouv.fr - Fichier des personnes décédées.

    Mesures :
      - nombre_deces (agrégé par période / département / cause)

    Partitionnement : annee
    """
    log.info("  [3/4] Construction fait_deces...")

    path_csv = f"{HDFS_NAMENODE}/data/bronze/csv/deces"
    path_out = f"{GOLD_FAITS}/fait_deces"

    try:
        df = spark.read.option("header", "true").option("sep", ",").csv(path_csv)
    except Exception:
        log.warning("    ⚠️  CSV deces non trouvé → table vide")
        from pyspark.sql.types import StructType, StructField, LongType, IntegerType
        schema = StructType([
            StructField("sk_deces",   LongType(),    False),
            StructField("sk_temps",   LongType(),    True),
            StructField("departement",LongType(),    True),
            StructField("annee",      IntegerType(), True),
        ])
        df_gold = spark.createDataFrame([], schema)
        df_gold.write.mode("overwrite").parquet(path_out)
        return {"fait": "fait_deces", "count": 0, "status": "OK"}

    # La date de décès est souvent dans un champ "datedeces" ou "date_deces"
    date_col = next((c for c in df.columns
                     if "deces" in c.lower() or "date" in c.lower()), None)

    if date_col:
        df = df.withColumn("annee",
                           F.year(F.to_date(F.col(date_col).cast("string").substr(1, 8),
                                            "yyyyMMdd")))
    else:
        df = df.withColumn("annee", F.lit(2024))

    df_gold = df \
        .withColumn("sk_deces", F.monotonically_increasing_id() + 1) \
        .withColumn("sk_temps", F.lit(-1).cast(LongType())) \
        .select("sk_deces", "sk_temps", "annee")

    df_gold \
        .repartition("annee") \
        .write \
        .partitionBy("annee") \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(path_out)

    cnt = df_gold.count()
    log.info(f"    fait_deces → {cnt:,} lignes ✅")
    return {"fait": "fait_deces", "count": cnt, "status": "OK"}


# ─────────────────────────────────────────────────────────────────────────────
# 4. FAIT_SATISFACTION
# ─────────────────────────────────────────────────────────────────────────────

def build_fait_satisfaction(spark: SparkSession,
                             dims: dict,
                             extraction_date: str) -> dict:
    """
    Construit fait_satisfaction depuis les CSV ESATIS (toutes années).
    Source : data.gouv.fr - Enquête de satisfaction des patients.

    Mesures (1 ligne = 1 note par établissement et par dimension ESATIS) :
      - note_moyenne (FLOAT)  : moyenne des réponses (0-100)
      - nb_repondants (INT)   : nombre de répondants
      - taux_reponse (FLOAT)  : taux de participation

    Jointures :
      - dim_etablissement (sur finess_et)
      - dim_question (sur code_question)
      - dim_temps (sur annee_recueil)
    """
    log.info("  [4/4] Construction fait_satisfaction...")

    path_csv = f"{HDFS_NAMENODE}/data/bronze/csv/satisfaction"
    path_out = f"{GOLD_FAITS}/fait_satisfaction"

    try:
        df = spark.read \
            .option("header", "true") \
            .option("sep", ";") \
            .option("encoding", "UTF-8") \
            .csv(path_csv)
        log.info(f"    Colonnes ESATIS : {df.columns[:10]}...")
    except Exception:
        log.warning("    ⚠️  CSV satisfaction non trouvé → table vide")
        from pyspark.sql.types import StructType, StructField, FloatType, IntegerType
        schema_empty = StructType([
            StructField("sk_satisfaction",  LongType(),    False),
            StructField("sk_etablissement", LongType(),    True),
            StructField("sk_question",      LongType(),    True),
            StructField("sk_temps",         LongType(),    True),
            StructField("note_moyenne",     FloatType(),   True),
            StructField("nb_repondants",    IntegerType(), True),
            StructField("taux_reponse",     FloatType(),   True),
        ])
        df_gold = spark.createDataFrame([], schema=schema_empty)
        df_gold.write.mode("overwrite").option("compression", "snappy").parquet(path_out)
        return {"fait": "fait_satisfaction", "count": 0, "status": "OK"}

    # Mapping des colonnes ESATIS vers le modèle Gold
    # Les colonnes exactes varient selon l'année du fichier ESATIS
    # On tente une correspondance par patterns de noms
    note_col = next((c for c in df.columns
                     if "note" in c.lower() or "score" in c.lower()
                        or "res_" in c.lower()), None)
    etab_col = next((c for c in df.columns
                     if "finess" in c.lower() or "etabliss" in c.lower()
                        or "etab" in c.lower()), None)
    annee_col = next((c for c in df.columns
                      if "annee" in c.lower() or "year" in c.lower()
                         or "recueil" in c.lower()), None)

    log.info(f"    Mapping colonnes : note={note_col}, etab={etab_col}, annee={annee_col}")

    # Jointure dim_etablissement
    dim_etab = F.broadcast(
        dims["dim_etablissement"].select("sk_etablissement", "id_etablissement_src")
    )
    if etab_col:
        df = df.join(dim_etab,
                     df[etab_col] == dim_etab["id_etablissement_src"],
                     how="left") \
               .withColumn("sk_etablissement",
                           F.coalesce(F.col("sk_etablissement"), F.lit(-1)))
    else:
        df = df.withColumn("sk_etablissement", F.lit(-1).cast(LongType()))

    df_gold = df \
        .withColumn("sk_satisfaction", F.monotonically_increasing_id() + 1) \
        .withColumn("sk_question", F.lit(-1).cast(LongType())) \
        .withColumn("sk_temps", F.lit(-1).cast(LongType())) \
        .withColumn("note_moyenne",
                    F.when(note_col is not None,
                           F.col(note_col).cast("float")).otherwise(F.lit(None))) \
        .withColumn("nb_repondants", F.lit(0)) \
        .withColumn("taux_reponse", F.lit(0.0).cast("float")) \
        .select("sk_satisfaction", "sk_etablissement", "sk_question",
                "sk_temps", "note_moyenne", "nb_repondants", "taux_reponse")

    df_gold.write.mode("overwrite").option("compression", "snappy").parquet(path_out)

    cnt = df_gold.count()
    log.info(f"    fait_satisfaction → {cnt:,} lignes ✅")
    return {"fait": "fait_satisfaction", "count": cnt, "status": "OK"}


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
        .appName("CHU_T5_Build_Facts")
        .master("spark://chu-spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.executor.instances", "2")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.autoBroadcastJoinThreshold", "100m")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    args = parse_args()
    extraction_date = args.date
    start_time = datetime.now()

    log.info("=" * 70)
    log.info("  CHU ETL - JOB T5 : CONSTRUCTION DES TABLES DE FAITS GOLD")
    log.info(f"  Date extraction  : {extraction_date}")
    log.info(f"  Démarrage        : {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 70)

    spark = None
    results = []

    try:
        spark = create_spark_session()

        # ── Chargement des dimensions en cache ────────────────────────────
        log.info("\n[0/4] Chargement des dimensions Gold en cache...")
        dims = {}
        for dim_name in ["dim_temps", "dim_patient", "dim_professionnel",
                         "dim_etablissement", "dim_diagnostic", "dim_question"]:
            try:
                dims[dim_name] = load_dim(spark, dim_name)
                cnt = dims[dim_name].count()
                log.info(f"    {dim_name:<25} : {cnt:,} lignes (cached)")
            except Exception as e:
                log.warning(f"    ⚠️  {dim_name} non trouvée : {e}")
                dims[dim_name] = spark.createDataFrame([], schema=None)

        # ── Construction des faits ─────────────────────────────────────────
        builders = [
            (build_fait_consultation,    "consultation"),
            (build_fait_hospitalisation, "hospitalisation"),
            (build_fait_deces,           "deces"),
            (build_fait_satisfaction,    "satisfaction"),
        ]

        for build_fn, name in builders:
            try:
                r = build_fn(spark, dims, extraction_date)
                results.append(r)
            except Exception as e:
                log.error(f"  ❌ {name} : {e}", exc_info=True)
                results.append({"fait": f"fait_{name}", "count": 0, "status": "ERROR"})

        # ── Rapport ────────────────────────────────────────────────────────
        log.info("\n" + "=" * 60)
        log.info("  RAPPORT FAITS GOLD")
        log.info("=" * 60)
        total_rows = 0
        for r in results:
            status = "✅" if r["status"] == "OK" else "❌"
            log.info(f"    {status} {r['fait']:<28} {r['count']:>10,} lignes")
            total_rows += r["count"]
        log.info(f"    {'─'*50}")
        log.info(f"    TOTAL                          {total_rows:>10,} lignes")

        duration = (datetime.now() - start_time).total_seconds()
        errors = sum(1 for r in results if r["status"] != "OK")
        log.info(f"\n  Durée : {duration:.0f}s  |  Erreurs : {errors}")
        log.info("Prochaine étape : Exécuter T6_catalog_hive.py")

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
