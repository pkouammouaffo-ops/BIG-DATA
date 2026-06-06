#!/usr/bin/env python3
# =============================================================================
# LIVRABLE 2 - Job T3 : PSEUDONYMISATION RGPD (SHA-256)
# =============================================================================
# Projet    : CHU Data Warehouse - Architecture Médaillon
# Auteur    : Groupe 6 BigData CESI
# Date      : Juin 2026
# Version   : 1.0
#
# DESCRIPTION :
#   Ce job applique la pseudonymisation RGPD sur les données de la couche
#   Silver conformément au RGPD (Règlement Général sur la Protection des
#   Données, UE 2016/679) et aux recommandations CNIL pour les données
#   de santé (Health Data Hub).
#
#   DONNÉES DIRECTEMENT IDENTIFIANTES supprimées :
#     - Nom, Prénom (→ retirés du Data Warehouse)
#     - Date de naissance exacte (→ conservée en Silver, retirée du Gold)
#     - Adresse complète (→ seul code_postal conservé en Gold)
#
#   DONNÉES PSEUDONYMISÉES (identifiant de substitution) :
#     - id_patient → SHA-256(id_patient || SALT) → id_patient_hash
#     - id_professionnel → SHA-256(id_professionnel || SALT)
#
#   DONNÉES CONSERVÉES (non directement identifiantes) :
#     - Sexe (M/F)
#     - Tranche d'âge (< 18 / 18-39 / 40-64 / 65-79 / 80+)
#     - Code postal (sans adresse)
#     - Département / Région
#
#   ALGORITHME DE PSEUDONYMISATION :
#     hash = SHA-256(valeur_originale || "CHU_RGPD_SALT_2026")
#     Déterministe : un même id_patient aura toujours le même hash
#     Non réversible sans le sel secret (stocké hors du Data Warehouse)
#
# PRÉREQUIS :
#   - Job T2_deduplicate_silver.py exécuté avec succès
#
# EXÉCUTION :
#   docker exec chu-spark-master spark-submit \
#     --master spark://chu-spark-master:7077 \
#     --conf spark.executor.memory=1g \
#     /opt/airflow/dags/jobs/T3_pseudonymize_rgpd.py \
#     --date 2024-06-01 \
#     --salt "CHU_RGPD_SALT_2026"
# =============================================================================

import sys
import argparse
import logging
import hashlib
from datetime import datetime, date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DateType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("T3_pseudonymize_rgpd")

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTES
# ─────────────────────────────────────────────────────────────────────────────
HDFS_NAMENODE = "hdfs://chu-namenode:9000"
SILVER_BASE   = f"{HDFS_NAMENODE}/data/silver"

# Sel par défaut (IMPORTANT : en production, passer via --salt ou variable env)
DEFAULT_SALT = "CHU_RGPD_SALT_2026"

# Colonnes DIRECTEMENT IDENTIFIANTES à supprimer des sorties Gold
COLONNES_A_SUPPRIMER_GOLD = [
    "nom",
    "prenom",
    "date_naissance",   # Remplacée par tranche_age dans Gold
    "adresse",
    "adresse_complete",
    "telephone",
    "email",
    "numero_securite_sociale",
    "iban",
]


# ─────────────────────────────────────────────────────────────────────────────
# UDF SHA-256
# ─────────────────────────────────────────────────────────────────────────────

def make_sha256_udf(salt: str):
    """
    Crée une UDF PySpark qui calcule SHA-256(valeur || salt).
    
    Retourne une chaîne hexadécimale de 64 caractères.
    Si la valeur est NULL, retourne NULL (pas de hash de NULL).

    Args:
        salt: Sel cryptographique secret (à garder hors Data Warehouse)

    Returns:
        UDF PySpark (StringType → StringType)
    """
    _salt = salt  # Capture dans la closure

    def sha256_hash(value) -> str:
        if value is None:
            return None
        combined = f"{str(value)}{_salt}"
        return hashlib.sha256(combined.encode("utf-8")).hexdigest()

    return F.udf(sha256_hash, StringType())


# ─────────────────────────────────────────────────────────────────────────────
# PSEUDONYMISATION TABLE PATIENT
# ─────────────────────────────────────────────────────────────────────────────

def pseudonymize_patient(spark: SparkSession, salt: str, extraction_date: str) -> dict:
    """
    Pseudonymise la table PATIENT :

    Transformations :
    1. Génération de id_patient_hash = SHA-256(id_patient || salt)
    2. Suppression des colonnes directement identifiantes (nom, prenom, date_naissance)
    3. Conservation de :
       - tranche_age (non identifiante)
       - sexe (non identifiante)
       - code_postal (non directement identifiante)
       - departement, region (agrégés)
       - mutuelle_adherent (booléen, non identifiant)
    4. Ajout des colonnes SCD Type 2 (pour Gold dim_patient)

    Returns:
        {"before": int, "after": int, "status": str}
    """
    path_in  = f"{SILVER_BASE}/patient_dedup"
    path_out = f"{SILVER_BASE}/patient_rgpd"

    log.info("  Pseudonymisation PATIENT...")
    log.info(f"    Algorithme : SHA-256 avec sel '{salt[:6]}...' (tronqué)")

    sha256_udf = make_sha256_udf(salt)

    df = spark.read.parquet(path_in)
    before = df.count()
    log.info(f"    Lignes reçues : {before:,}")

    # 1. Générer le hash pseudonymisé
    df_pseudo = df.withColumn("id_patient_hash", sha256_udf(F.col("id_patient").cast(StringType())))

    # 2. Extraire la région et département depuis code_postal (approximation)
    #    Note : en production, faire la jointure avec le référentiel La Poste
    df_pseudo = df_pseudo \
        .withColumn("departement",
            F.when(F.col("code_postal").isNotNull(),
                   F.when(F.col("code_postal").startswith("97"),
                          F.substring(F.col("code_postal"), 1, 3))
                    .otherwise(F.substring(F.col("code_postal"), 1, 2))
            ).otherwise(F.lit("INCONNU"))
        )

    # 3. Ajouter colonnes SCD Type 2 (pour la dimension patient en Gold)
    df_pseudo = df_pseudo \
        .withColumn("date_debut_validite", F.lit(extraction_date).cast(DateType())) \
        .withColumn("date_fin_validite", F.lit(None).cast(DateType())) \
        .withColumn("est_courant", F.lit(True))

    # 4. Supprimer les colonnes directement identifiantes
    cols_to_drop = [c for c in COLONNES_A_SUPPRIMER_GOLD if c in df_pseudo.columns]
    df_pseudo = df_pseudo.drop(*cols_to_drop)

    if cols_to_drop:
        log.info(f"    Colonnes supprimées (RGPD) : {cols_to_drop}")

    # 5. Écriture
    df_pseudo.write.mode("overwrite").option("compression", "snappy").parquet(path_out)

    after = df_pseudo.count()
    log.info(f"    Lignes pseudonymisées : {after:,} ✅")
    log.info(f"    Colonnes restantes : {len(df_pseudo.columns)}")

    return {"before": before, "after": after, "status": "OK"}


# ─────────────────────────────────────────────────────────────────────────────
# PSEUDONYMISATION TABLE PROFESSIONNEL_DE_SANTE
# ─────────────────────────────────────────────────────────────────────────────

def pseudonymize_professionnel(spark: SparkSession, salt: str, extraction_date: str) -> dict:
    """
    Pseudonymise la table PROFESSIONNEL_DE_SANTE.
    
    L'identifiant RPPS est public (accessible sur Ameli), donc moins critique,
    mais on pseudonymise id_professionnel interne (clé SIH) par cohérence.

    Returns:
        {"before": int, "after": int, "status": str}
    """
    path_in  = f"{SILVER_BASE}/professionnel_sante_dedup"
    path_out = f"{SILVER_BASE}/professionnel_sante_rgpd"

    log.info("  Pseudonymisation PROFESSIONNEL_DE_SANTE...")

    sha256_udf = make_sha256_udf(salt)

    df = spark.read.parquet(path_in)
    before = df.count()

    df_pseudo = df \
        .withColumn("id_professionnel_hash",
                    sha256_udf(F.col("id_professionnel").cast(StringType()))) \
        .withColumn("date_debut_validite", F.lit(extraction_date).cast(DateType())) \
        .withColumn("date_fin_validite", F.lit(None).cast(DateType())) \
        .withColumn("est_courant", F.lit(True))

    # Les professionnels de santé sont semi-publics (RPPS/ADELI = registres publics)
    # On conserve : nom, prenom (non supprimés pour les professionnels)
    # On supprime uniquement : adresse complète, téléphone direct
    cols_to_drop_pro = [c for c in ["adresse", "telephone", "email"]
                        if c in df_pseudo.columns]
    df_pseudo = df_pseudo.drop(*cols_to_drop_pro)

    df_pseudo.write.mode("overwrite").option("compression", "snappy").parquet(path_out)

    after = df_pseudo.count()
    log.info(f"    {before:,} → {after:,} lignes ✅")

    return {"before": before, "after": after, "status": "OK"}


# ─────────────────────────────────────────────────────────────────────────────
# PSEUDONYMISATION TABLE CONSULTATION
# ─────────────────────────────────────────────────────────────────────────────

def pseudonymize_consultation(spark: SparkSession, salt: str, extraction_date: str) -> dict:
    """
    Pseudonymise la table CONSULTATION.
    
    Remplace id_patient et id_professionnel par leurs hashes correspondants
    (même sel → même hash → jointure possible dans Gold).

    Returns:
        {"before": int, "after": int, "status": str}
    """
    path_in  = f"{SILVER_BASE}/consultation_dedup"
    path_out = f"{SILVER_BASE}/consultation_rgpd"

    log.info("  Pseudonymisation CONSULTATION...")

    sha256_udf = make_sha256_udf(salt)

    df = spark.read.parquet(path_in)
    before = df.count()

    df_pseudo = df \
        .withColumn("id_patient_hash",
                    sha256_udf(F.col("id_patient").cast(StringType()))) \
        .withColumn("id_professionnel_hash",
                    sha256_udf(F.col("id_professionnel").cast(StringType()))) \
        .drop("id_patient", "id_professionnel")   # Suppression des IDs directs

    df_pseudo.write.mode("overwrite").option("compression", "snappy").parquet(path_out)

    after = df_pseudo.count()
    log.info(f"    {before:,} → {after:,} lignes ✅")

    return {"before": before, "after": after, "status": "OK"}


# ─────────────────────────────────────────────────────────────────────────────
# RAPPORT RGPD (pour le DPO - Délégué Protection Données)
# ─────────────────────────────────────────────────────────────────────────────

def generate_rgpd_report(results: list, salt_hash: str, extraction_date: str):
    """
    Génère un rapport RGPD résumant les actions de pseudonymisation.
    Ce rapport est destiné au DPO (Délégué à la Protection des Données).
    """
    log.info("\n" + "=" * 60)
    log.info("  RAPPORT RGPD - PSEUDONYMISATION")
    log.info("=" * 60)
    log.info(f"  Date d'exécution     : {extraction_date}")
    log.info(f"  Algorithme           : SHA-256 (HMAC)")
    log.info(f"  Empreinte du sel     : {salt_hash[:16]}... (tronqué)")
    log.info(f"  Conformité           : RGPD Art. 4(5) - Pseudonymisation")
    log.info(f"  Recommandation CNIL  : AIPD santé - Niveau 3")
    log.info(f"")
    log.info(f"  Données concernées   :")
    log.info(f"    - id_patient         → SHA-256 (irréversible sans sel)")
    log.info(f"    - id_professionnel   → SHA-256 (irréversible sans sel)")
    log.info(f"    - nom, prenom        → SUPPRIMÉS du Data Warehouse Gold")
    log.info(f"    - date_naissance     → SUPPRIMÉE (conservé : tranche_age)")
    log.info(f"    - adresse complète   → SUPPRIMÉE (conservé : code_postal)")
    log.info(f"    - téléphone / email  → SUPPRIMÉS")
    log.info(f"")
    log.info(f"  Résultats :")
    for r in results:
        status = "✅ OK" if r["status"] == "OK" else "❌ ERREUR"
        log.info(f"    {status} {r['table']:<30} {r['before']:>8,} lignes")
    log.info("=" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# SCRIPT PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--salt", default=DEFAULT_SALT,
                        help="Sel cryptographique pour SHA-256 (garder secret!)")
    return parser.parse_args()


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("CHU_T3_Pseudonymize_RGPD")
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
    salt = args.salt
    start_time = datetime.now()

    # Hash du sel pour le rapport (on ne log PAS le sel en clair)
    salt_fingerprint = hashlib.sha256(salt.encode()).hexdigest()

    log.info("=" * 70)
    log.info("  CHU ETL - JOB T3 : PSEUDONYMISATION RGPD")
    log.info(f"  Date              : {extraction_date}")
    log.info(f"  Sel (empreinte)   : {salt_fingerprint[:16]}...")
    log.info(f"  Algorithme        : SHA-256 + sel")
    log.info(f"  Démarrage         : {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 70)

    spark = None
    results = []

    try:
        spark = create_spark_session()

        # ── PATIENT ───────────────────────────────────────────────────────
        log.info("\n[1/3] PATIENT...")
        try:
            r = pseudonymize_patient(spark, salt, extraction_date)
            results.append({"table": "patient", **r})
        except Exception as e:
            log.error(f"  ❌ {e}")
            results.append({"table": "patient", "before": 0, "after": 0, "status": "ERROR"})

        # ── PROFESSIONNEL ─────────────────────────────────────────────────
        log.info("\n[2/3] PROFESSIONNEL_DE_SANTE...")
        try:
            r = pseudonymize_professionnel(spark, salt, extraction_date)
            results.append({"table": "professionnel_sante", **r})
        except Exception as e:
            log.error(f"  ❌ {e}")
            results.append({"table": "professionnel_sante", "before": 0, "after": 0, "status": "ERROR"})

        # ── CONSULTATION ──────────────────────────────────────────────────
        log.info("\n[3/3] CONSULTATION (remplacement IDs)...")
        try:
            r = pseudonymize_consultation(spark, salt, extraction_date)
            results.append({"table": "consultation", **r})
        except Exception as e:
            log.error(f"  ❌ {e}")
            results.append({"table": "consultation", "before": 0, "after": 0, "status": "ERROR"})

        # ── RAPPORT RGPD ──────────────────────────────────────────────────
        generate_rgpd_report(results, salt_fingerprint, extraction_date)

        total_duration = (datetime.now() - start_time).total_seconds()
        ok_count = sum(1 for r in results if r["status"] == "OK")
        err_count = len(results) - ok_count

        log.info(f"\n  Statut final : {'✅ SUCCÈS' if err_count == 0 else '❌ PARTIEL'}")
        log.info(f"  Durée totale : {total_duration:.0f}s")
        log.info("\nProchaine étape : Exécuter T4_build_dimensions.py")

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
