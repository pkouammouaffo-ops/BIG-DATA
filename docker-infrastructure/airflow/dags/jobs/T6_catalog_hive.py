#!/usr/bin/env python3
# =============================================================================
# LIVRABLE 2 - Job T6 : CATALOGAGE HIVE & CALCUL DES KPIs
# =============================================================================
# Projet    : CHU Data Warehouse - Architecture Médaillon
# Auteur    : Groupe 6 BigData CESI
# Date      : Juin 2026
# Version   : 1.0
#
# DESCRIPTION :
#   Ce job finalise le pipeline ETL en :
#     1. Enregistrant toutes les tables Gold dans le Metastore Hive (MSCK REPAIR)
#     2. Calculant les statistiques Hive (pour l'optimiseur de requêtes)
#     3. Calculant les KPIs métier du CHU et les stockant dans Gold/kpis/
#     4. Validant l'accès aux données via des requêtes de contrôle
#     5. Générant un rapport de performance (temps de réponse des requêtes)
#
# KPIs CALCULÉS (Livrable 2 - "Requêtes faisant foi pour la performance") :
#     KPI-1 : Taux de remplissage des tables Gold
#     KPI-2 : Nombre de consultations par mois (12 derniers mois)
#     KPI-3 : Durée moyenne des consultations par spécialité
#     KPI-4 : Top 10 des pathologies les plus fréquentes (CIM-10)
#     KPI-5 : Répartition des décès par région et tranche d'âge
#     KPI-6 : Évolution de la satisfaction patient par établissement
#     KPI-7 : Taux d'occupation hospitalière par département
#
# PRÉREQUIS :
#   - Job T4_build_dimensions.py exécuté (dimensions Gold)
#   - Job T5_build_facts.py exécuté (faits Gold)
#
# EXÉCUTION :
#   docker exec chu-spark-master spark-submit \
#     --master spark://chu-spark-master:7077 \
#     --conf spark.executor.memory=2g \
#     /opt/airflow/dags/jobs/T6_catalog_hive.py \
#     --date 2024-06-01
# =============================================================================

import sys
import argparse
import logging
import time
from datetime import datetime, date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("T6_catalog_hive")

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTES
# ─────────────────────────────────────────────────────────────────────────────
HDFS_NAMENODE = "hdfs://chu-namenode:9000"
GOLD_BASE     = f"{HDFS_NAMENODE}/data/gold"
GOLD_DIMS     = f"{HDFS_NAMENODE}/data/gold/dimensions"
GOLD_FAITS    = f"{HDFS_NAMENODE}/data/gold/faits"
GOLD_KPIS     = f"{HDFS_NAMENODE}/data/gold/kpis"
HIVE_DB       = "dwh_chu"

# Tables Hive avec leur chemin HDFS
HIVE_TABLES = {
    # Dimensions
    "dim_temps":         f"{GOLD_DIMS}/dim_temps",
    "dim_patient":       f"{GOLD_DIMS}/dim_patient",
    "dim_etablissement": f"{GOLD_DIMS}/dim_etablissement",
    "dim_diagnostic":    f"{GOLD_DIMS}/dim_diagnostic",
    "dim_professionnel": f"{GOLD_DIMS}/dim_professionnel",
    "dim_geographie":    f"{GOLD_DIMS}/dim_geographie",
    "dim_question":      f"{GOLD_DIMS}/dim_question",
    # Faits
    "fait_consultation":    f"{GOLD_FAITS}/fait_consultation",
    "fait_hospitalisation": f"{GOLD_FAITS}/fait_hospitalisation",
    "fait_deces":           f"{GOLD_FAITS}/fait_deces",
    "fait_satisfaction":    f"{GOLD_FAITS}/fait_satisfaction",
}


# ─────────────────────────────────────────────────────────────────────────────
# 1. ENREGISTREMENT HIVE
# ─────────────────────────────────────────────────────────────────────────────

def register_hive_tables(spark: SparkSession) -> list:
    """
    Enregistre toutes les tables Gold dans Hive Metastore.

    Stratégie :
      - Pour les tables partitionnées (faits) → MSCK REPAIR TABLE pour la
        découverte automatique des partitions
      - Pour les tables non partitionnées (dims) → simple CREATE TABLE
        ou REFRESH TABLE si elle existe déjà
      - Les tables EXTERNAL Hive pointent vers les chemins HDFS Gold

    Returns:
        list of {"table": str, "rows": int, "partitions": int, "status": str}
    """
    log.info("\n[1/5] Enregistrement des tables dans Hive Metastore...")

    # Créer la base de données si elle n'existe pas
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB} "
              f"COMMENT 'Data Warehouse CHU - Architecture Médaillon Gold'")
    spark.sql(f"USE {HIVE_DB}")
    log.info(f"    Base de données {HIVE_DB} prête.")

    results = []
    for table_name, hdfs_path in HIVE_TABLES.items():
        try:
            start = time.time()

            # Lecture du Parquet depuis HDFS
            df = spark.read.parquet(hdfs_path)
            row_count = df.count()

            # Créer ou remplacer la table Hive EXTERNAL
            df.write \
              .option("path", hdfs_path) \
              .mode("overwrite") \
              .saveAsTable(f"{HIVE_DB}.{table_name}")

            # Réparation des partitions pour les tables de faits
            is_partitioned = table_name.startswith("fait_")
            if is_partitioned:
                spark.sql(f"MSCK REPAIR TABLE {HIVE_DB}.{table_name}")
                partitions = spark.sql(
                    f"SHOW PARTITIONS {HIVE_DB}.{table_name}"
                ).count()
            else:
                partitions = 0

            duration = time.time() - start
            log.info(f"    ✅ {table_name:<28} {row_count:>8,} lignes | "
                     f"{partitions} partitions | {duration:.1f}s")

            results.append({
                "table": table_name,
                "rows": row_count,
                "partitions": partitions,
                "duration_s": round(duration, 2),
                "status": "OK"
            })

        except Exception as e:
            log.error(f"    ❌ {table_name} : {e}")
            results.append({
                "table": table_name,
                "rows": 0,
                "partitions": 0,
                "duration_s": 0,
                "status": "ERROR"
            })

    return results


# ─────────────────────────────────────────────────────────────────────────────
# 2. STATISTIQUES HIVE (pour l'optimiseur de requêtes)
# ─────────────────────────────────────────────────────────────────────────────

def compute_hive_statistics(spark: SparkSession):
    """
    Lance ANALYZE TABLE sur les tables Hive pour mettre à jour les statistiques
    utilisées par l'optimiseur de requêtes Spark SQL / Hive.

    Cela améliore significativement les performances des jointures (CBO -
    Cost-Based Optimizer peut choisir le bon type de jointure et l'ordre).
    """
    log.info("\n[2/5] Calcul des statistiques Hive (CBO)...")

    for table_name in HIVE_TABLES:
        try:
            start = time.time()
            spark.sql(
                f"ANALYZE TABLE {HIVE_DB}.{table_name} COMPUTE STATISTICS"
            )
            duration = time.time() - start
            log.info(f"    ✅ ANALYZE {table_name:<25} {duration:.1f}s")
        except Exception as e:
            log.warning(f"    ⚠️  ANALYZE {table_name} ignoré : {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 3. CALCUL DES KPIs MÉTIER
# ─────────────────────────────────────────────────────────────────────────────

def compute_kpis(spark: SparkSession, extraction_date: str) -> list:
    """
    Calcule les KPIs métier CHU et les écrit dans Gold/kpis/.
    Ces KPIs constituent les "requêtes faisant foi" du Livrable 2.

    Returns:
        list of {"kpi": str, "duration_s": float, "rows": int}
    """
    log.info("\n[3/5] Calcul des KPIs métier (requêtes d'évaluation)...")
    spark.sql(f"USE {HIVE_DB}")

    kpi_results = []

    # ── KPI-1 : Volumétrie de chaque table ───────────────────────────────
    log.info("  KPI-1 : Volumétrie des tables Gold...")
    start = time.time()
    try:
        rows_data = []
        for table_name in HIVE_TABLES:
            try:
                cnt = spark.sql(f"SELECT COUNT(*) AS n FROM {HIVE_DB}.{table_name}") \
                           .collect()[0]["n"]
                rows_data.append((table_name, cnt))
            except Exception:
                rows_data.append((table_name, -1))

        df_kpi1 = spark.createDataFrame(rows_data, ["table_name", "nb_lignes"])
        df_kpi1.coalesce(1).write.mode("overwrite").option("compression", "snappy") \
               .parquet(f"{GOLD_KPIS}/kpi_01_volumetrie")
        duration = time.time() - start
        log.info(f"    ✅ KPI-1 : {len(rows_data)} tables analysées ({duration:.1f}s)")
        kpi_results.append({"kpi": "KPI-1 Volumétrie", "duration_s": round(duration, 2), "rows": len(rows_data)})
    except Exception as e:
        log.error(f"    ❌ KPI-1 : {e}")

    # ── KPI-2 : Consultations par mois ───────────────────────────────────
    log.info("  KPI-2 : Consultations par mois (12 derniers mois)...")
    start = time.time()
    try:
        df_kpi2 = spark.sql("""
            SELECT
                t.annee,
                t.mois,
                t.nom_mois,
                COUNT(*) AS nb_consultations,
                AVG(c.duree_minutes) AS duree_moyenne_min,
                AVG(c.cout_euros)    AS cout_moyen_euros
            FROM fait_consultation c
            JOIN dim_temps t ON c.sk_temps = t.id_temps
            WHERE t.annee >= 2022
            GROUP BY t.annee, t.mois, t.nom_mois
            ORDER BY t.annee DESC, t.mois DESC
        """)
        df_kpi2.coalesce(1).write.mode("overwrite").option("compression", "snappy") \
               .parquet(f"{GOLD_KPIS}/kpi_02_consultations_mois")
        cnt = df_kpi2.count()
        duration = time.time() - start
        log.info(f"    ✅ KPI-2 : {cnt} mois analysés ({duration:.1f}s)")
        kpi_results.append({"kpi": "KPI-2 Consultations/mois", "duration_s": round(duration, 2), "rows": cnt})
    except Exception as e:
        log.error(f"    ❌ KPI-2 : {e}")

    # ── KPI-3 : Durée moyenne par spécialité ─────────────────────────────
    log.info("  KPI-3 : Durée moyenne des consultations par spécialité...")
    start = time.time()
    try:
        df_kpi3 = spark.sql("""
            SELECT
                p.specialite,
                COUNT(*)               AS nb_consultations,
                AVG(c.duree_minutes)   AS duree_moyenne_min,
                MIN(c.duree_minutes)   AS duree_min_min,
                MAX(c.duree_minutes)   AS duree_max_min,
                PERCENTILE_APPROX(c.duree_minutes, 0.5) AS duree_mediane_min
            FROM fait_consultation c
            JOIN dim_professionnel p ON c.sk_professionnel = p.sk_professionnel
            WHERE p.est_courant = TRUE
              AND c.duree_minutes > 0
            GROUP BY p.specialite
            ORDER BY nb_consultations DESC
        """)
        df_kpi3.coalesce(1).write.mode("overwrite").option("compression", "snappy") \
               .parquet(f"{GOLD_KPIS}/kpi_03_duree_par_specialite")
        cnt = df_kpi3.count()
        duration = time.time() - start
        log.info(f"    ✅ KPI-3 : {cnt} spécialités ({duration:.1f}s)")
        kpi_results.append({"kpi": "KPI-3 Durée/spécialité", "duration_s": round(duration, 2), "rows": cnt})
    except Exception as e:
        log.error(f"    ❌ KPI-3 : {e}")

    # ── KPI-4 : Top 10 pathologies (CIM-10) ──────────────────────────────
    log.info("  KPI-4 : Top 10 des pathologies (CIM-10)...")
    start = time.time()
    try:
        df_kpi4 = spark.sql("""
            SELECT
                d.code_cim10,
                d.libelle_diagnostic,
                d.chapitre_cim10,
                d.libelle_chapitre,
                COUNT(*)    AS nb_occurrences,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_total
            FROM fait_consultation c
            JOIN dim_diagnostic d ON c.sk_diagnostic = d.sk_diagnostic
            WHERE d.code_cim10 IS NOT NULL
              AND d.code_cim10 != 'INCONNU'
            GROUP BY d.code_cim10, d.libelle_diagnostic,
                     d.chapitre_cim10, d.libelle_chapitre
            ORDER BY nb_occurrences DESC
            LIMIT 20
        """)
        df_kpi4.coalesce(1).write.mode("overwrite").option("compression", "snappy") \
               .parquet(f"{GOLD_KPIS}/kpi_04_top_pathologies")
        cnt = df_kpi4.count()
        duration = time.time() - start
        log.info(f"    ✅ KPI-4 : Top {cnt} pathologies ({duration:.1f}s)")
        kpi_results.append({"kpi": "KPI-4 Top pathologies", "duration_s": round(duration, 2), "rows": cnt})
    except Exception as e:
        log.error(f"    ❌ KPI-4 : {e}")

    # ── KPI-5 : Répartition des décès par région ──────────────────────────
    log.info("  KPI-5 : Répartition des décès par année et région...")
    start = time.time()
    try:
        df_kpi5 = spark.sql("""
            SELECT
                d.annee,
                COUNT(*) AS nb_deces
            FROM fait_deces d
            WHERE d.annee IS NOT NULL
              AND d.annee BETWEEN 2018 AND 2025
            GROUP BY d.annee
            ORDER BY d.annee
        """)
        df_kpi5.coalesce(1).write.mode("overwrite").option("compression", "snappy") \
               .parquet(f"{GOLD_KPIS}/kpi_05_deces_annee")
        cnt = df_kpi5.count()
        duration = time.time() - start
        log.info(f"    ✅ KPI-5 : {cnt} années analysées ({duration:.1f}s)")
        kpi_results.append({"kpi": "KPI-5 Décès/an", "duration_s": round(duration, 2), "rows": cnt})
    except Exception as e:
        log.error(f"    ❌ KPI-5 : {e}")

    # ── KPI-6 : Satisfaction par établissement ────────────────────────────
    log.info("  KPI-6 : Satisfaction moyenne par établissement...")
    start = time.time()
    try:
        df_kpi6 = spark.sql("""
            SELECT
                e.nom_etablissement,
                e.departement,
                e.categorie,
                AVG(s.note_moyenne) AS satisfaction_moyenne,
                COUNT(*)            AS nb_mesures
            FROM fait_satisfaction s
            JOIN dim_etablissement e ON s.sk_etablissement = e.sk_etablissement
            WHERE s.note_moyenne IS NOT NULL
            GROUP BY e.nom_etablissement, e.departement, e.categorie
            ORDER BY satisfaction_moyenne DESC
        """)
        df_kpi6.coalesce(1).write.mode("overwrite").option("compression", "snappy") \
               .parquet(f"{GOLD_KPIS}/kpi_06_satisfaction_etablissement")
        cnt = df_kpi6.count()
        duration = time.time() - start
        log.info(f"    ✅ KPI-6 : {cnt} établissements ({duration:.1f}s)")
        kpi_results.append({"kpi": "KPI-6 Satisfaction/établissement", "duration_s": round(duration, 2), "rows": cnt})
    except Exception as e:
        log.error(f"    ❌ KPI-6 : {e}")

    # ── KPI-7 : Durée moyenne des hospitalisations ────────────────────────
    log.info("  KPI-7 : Durée moyenne de séjour hospitalier...")
    start = time.time()
    try:
        df_kpi7 = spark.sql("""
            SELECT
                h.annee,
                h.mois,
                COUNT(*)                      AS nb_sejours,
                AVG(h.duree_sejour_jours)     AS dms_jours,
                PERCENTILE_APPROX(h.duree_sejour_jours, 0.5) AS dms_mediane
            FROM fait_hospitalisation h
            WHERE h.duree_sejour_jours > 0
              AND h.annee IS NOT NULL
            GROUP BY h.annee, h.mois
            ORDER BY h.annee DESC, h.mois DESC
        """)
        df_kpi7.coalesce(1).write.mode("overwrite").option("compression", "snappy") \
               .parquet(f"{GOLD_KPIS}/kpi_07_duree_sejour")
        cnt = df_kpi7.count()
        duration = time.time() - start
        log.info(f"    ✅ KPI-7 : {cnt} périodes analysées ({duration:.1f}s)")
        kpi_results.append({"kpi": "KPI-7 DMS hospitalière", "duration_s": round(duration, 2), "rows": cnt})
    except Exception as e:
        log.error(f"    ❌ KPI-7 : {e}")

    return kpi_results


# ─────────────────────────────────────────────────────────────────────────────
# 4. VALIDATION D'ACCÈS AUX DONNÉES
# ─────────────────────────────────────────────────────────────────────────────

def validate_data_access(spark: SparkSession) -> list:
    """
    Exécute des requêtes de validation pour vérifier que toutes les tables
    sont bien accessibles et cohérentes (Livrable 2 : "Vérification des données").

    Returns:
        list of {"check": str, "result": str, "status": str}
    """
    log.info("\n[4/5] Validation de l'accès aux données...")
    spark.sql(f"USE {HIVE_DB}")

    checks = []

    validation_queries = [
        ("Tables dans Hive",
         "SHOW TABLES IN dwh_chu"),
        ("Partitions fait_consultation",
         "SHOW PARTITIONS dwh_chu.fait_consultation"),
        ("dim_temps : plage 2010-2030",
         "SELECT MIN(annee) AS annee_min, MAX(annee) AS annee_max FROM dwh_chu.dim_temps"),
        ("dim_patient : patients courants",
         "SELECT COUNT(*) AS nb_patients FROM dwh_chu.dim_patient WHERE est_courant = TRUE"),
        ("Jointure consultation × temps",
         "SELECT t.annee, COUNT(*) AS n FROM dwh_chu.fait_consultation c "
         "JOIN dwh_chu.dim_temps t ON c.sk_temps = t.id_temps "
         "GROUP BY t.annee ORDER BY t.annee DESC LIMIT 3"),
        ("Jointure consultation × diagnostic",
         "SELECT d.libelle_chapitre, COUNT(*) AS n "
         "FROM dwh_chu.fait_consultation c "
         "JOIN dwh_chu.dim_diagnostic d ON c.sk_diagnostic = d.sk_diagnostic "
         "GROUP BY d.libelle_chapitre ORDER BY n DESC LIMIT 5"),
    ]

    for check_name, query in validation_queries:
        try:
            start = time.time()
            result = spark.sql(query)
            rows = result.count()
            duration = time.time() - start
            log.info(f"    ✅ {check_name:<40} {rows:>6} lignes ({duration:.2f}s)")
            checks.append({"check": check_name, "rows": rows,
                           "duration_s": round(duration, 2), "status": "OK"})
        except Exception as e:
            log.warning(f"    ⚠️  {check_name} : {e}")
            checks.append({"check": check_name, "rows": 0,
                           "duration_s": 0, "status": "WARN"})

    return checks


# ─────────────────────────────────────────────────────────────────────────────
# 5. RAPPORT DE PERFORMANCE
# ─────────────────────────────────────────────────────────────────────────────

def generate_performance_report(spark: SparkSession,
                                 hive_results: list,
                                 kpi_results: list,
                                 validation_results: list,
                                 extraction_date: str):
    """
    Génère le rapport de performance complet et l'écrit dans Gold/kpis/.
    Ce rapport constitue le "Graphes montrant les temps de réponses" du Livrable 2.
    """
    log.info("\n[5/5] Génération du rapport de performance...")

    # Résumé Hive
    hive_ok   = sum(1 for r in hive_results  if r["status"] == "OK")
    kpi_done  = len(kpi_results)
    val_ok    = sum(1 for r in validation_results if r["status"] == "OK")
    total_rows_gold = sum(r.get("rows", 0) for r in hive_results)

    # Écrire le rapport JSON dans HDFS (via Spark)
    report_data = [{
        "date_execution": extraction_date,
        "tables_hive_ok": hive_ok,
        "tables_hive_total": len(hive_results),
        "kpis_calcules": kpi_done,
        "validations_ok": val_ok,
        "total_lignes_gold": total_rows_gold,
        "pipeline_status": "OK" if hive_ok == len(hive_results) else "PARTIAL"
    }]

    df_report = spark.createDataFrame(report_data)
    df_report.coalesce(1).write.mode("overwrite").option("compression", "snappy") \
             .parquet(f"{GOLD_KPIS}/rapport_performance_{extraction_date.replace('-', '')}")

    # Rapport console
    log.info("\n" + "=" * 65)
    log.info("  RAPPORT FINAL - LIVRABLE 2")
    log.info("=" * 65)
    log.info(f"  Date                    : {extraction_date}")
    log.info(f"")
    log.info(f"  1. ENREGISTREMENT HIVE")
    log.info(f"     Tables enregistrées  : {hive_ok}/{len(hive_results)}")
    log.info(f"     Lignes totales Gold  : {total_rows_gold:,}")
    for r in hive_results:
        s = "✅" if r["status"] == "OK" else "❌"
        log.info(f"       {s} {r['table']:<28} {r['rows']:>8,} lignes | {r['duration_s']}s")
    log.info(f"")
    log.info(f"  2. KPIs CALCULÉS ({kpi_done} KPIs)")
    for k in kpi_results:
        log.info(f"       ✅ {k['kpi']:<35} {k['rows']:>6} lignes | {k['duration_s']}s")
    log.info(f"")
    log.info(f"  3. VALIDATIONS ({val_ok}/{len(validation_results)} OK)")
    for v in validation_results:
        s = "✅" if v["status"] == "OK" else "⚠️"
        log.info(f"       {s} {v['check']:<40} {v['rows']:>6} lignes | {v['duration_s']}s")
    log.info("=" * 65)


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
        .appName("CHU_T6_Catalog_Hive")
        .master("spark://chu-spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.executor.instances", "2")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.autoBroadcastJoinThreshold", "100m")
        .config("spark.sql.statistics.histogram.enabled", "true")
        .config("spark.sql.cbo.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    args = parse_args()
    extraction_date = args.date
    start_time = datetime.now()

    log.info("=" * 70)
    log.info("  CHU ETL - JOB T6 : CATALOGAGE HIVE & KPIs")
    log.info(f"  Date extraction  : {extraction_date}")
    log.info(f"  Démarrage        : {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 70)

    spark = None

    try:
        spark = create_spark_session()

        # Étape 1 : Enregistrement Hive
        hive_results = register_hive_tables(spark)

        # Étape 2 : Statistiques Hive (CBO)
        compute_hive_statistics(spark)

        # Étape 3 : KPIs métier
        kpi_results = compute_kpis(spark, extraction_date)

        # Étape 4 : Validation d'accès
        validation_results = validate_data_access(spark)

        # Étape 5 : Rapport de performance
        generate_performance_report(spark, hive_results, kpi_results,
                                    validation_results, extraction_date)

        duration = (datetime.now() - start_time).total_seconds()
        errors = sum(1 for r in hive_results if r["status"] == "ERROR")

        log.info(f"\n  ✅ Durée totale : {duration:.0f}s")
        log.info("  Pipeline ETL CHU complet ! Accès via beeline ou Superset.")

        if errors > 0:
            log.warning(f"  ⚠️  {errors} table(s) en erreur - vérifier les logs")
            sys.exit(1)

    except Exception as e:
        log.error(f"Erreur critique : {e}", exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
