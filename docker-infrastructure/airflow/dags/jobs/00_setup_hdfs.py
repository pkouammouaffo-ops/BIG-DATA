#!/usr/bin/env python3
# =============================================================================
# LIVRABLE 2 - Script 00 : INITIALISATION DE LA STRUCTURE HDFS
# =============================================================================
# Projet    : CHU Data Warehouse - Architecture Médaillon
# Auteur    : Groupe 6 BigData CESI
# Date      : Juin 2026
# Version   : 1.0
#
# DESCRIPTION :
#   Ce script crée et vérifie la structure complète de dossiers HDFS
#   nécessaire à l'architecture Médaillon (Bronze / Silver / Gold).
#   Il doit être exécuté UNE SEULE FOIS avant le premier lancement du pipeline.
#
# PRÉREQUIS :
#   - Conteneur chu-namenode démarré et en état "healthy"
#   - Conteneur chu-datanode1/2/3 démarrés et connectés
#   - PySpark 3.5.0 installé
#
# EXÉCUTION (depuis docker) :
#   docker exec chu-spark-master spark-submit \
#     --master spark://chu-spark-master:7077 \
#     /opt/airflow/dags/jobs/00_setup_hdfs.py
#
# EXÉCUTION (depuis PowerShell hôte) :
#   docker cp airflow/dags/jobs/00_setup_hdfs.py chu-spark-master:/tmp/
#   docker exec chu-spark-master spark-submit /tmp/00_setup_hdfs.py
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
log = logging.getLogger("00_setup_hdfs")

# ─────────────────────────────────────────────────────────────────────────────
# PARAMÈTRES DE CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

# Adresse du NameNode HDFS
HDFS_NAMENODE = "hdfs://chu-namenode:9000"

# Arborescence complète de l'architecture Médaillon
# Structure : { "dossier": "description" }
HDFS_DIRECTORIES = {

    # ── COUCHE BRONZE : Données brutes extraites de PostgreSQL (SIH) ──────────
    f"{HDFS_NAMENODE}/data/bronze/postgres/patient":
        "Bronze - Table PATIENT extraite du SIH (brut, aucune transformation)",
    f"{HDFS_NAMENODE}/data/bronze/postgres/consultation":
        "Bronze - Table CONSULTATION extraite du SIH",
    f"{HDFS_NAMENODE}/data/bronze/postgres/diagnostic":
        "Bronze - Table DIAGNOSTIC extraite du SIH (codes CIM-10)",
    f"{HDFS_NAMENODE}/data/bronze/postgres/professionnel_sante":
        "Bronze - Table PROFESSIONNEL_DE_SANTE extraite du SIH",
    f"{HDFS_NAMENODE}/data/bronze/postgres/specialites":
        "Bronze - Table SPECIALITES extraite du SIH",
    f"{HDFS_NAMENODE}/data/bronze/postgres/prescription":
        "Bronze - Table PRESCRIPTION extraite du SIH",
    f"{HDFS_NAMENODE}/data/bronze/postgres/medicaments":
        "Bronze - Table MEDICAMENTS extraite du SIH",
    f"{HDFS_NAMENODE}/data/bronze/postgres/mutuelle":
        "Bronze - Table MUTUELLE extraite du SIH",
    f"{HDFS_NAMENODE}/data/bronze/postgres/adher":
        "Bronze - Table ADHER (adhésions mutuelles) extraite du SIH",
    f"{HDFS_NAMENODE}/data/bronze/postgres/salle":
        "Bronze - Table SALLE extraite du SIH",
    f"{HDFS_NAMENODE}/data/bronze/postgres/laboratoire":
        "Bronze - Table LABORATOIRE extraite du SIH",

    # ── COUCHE BRONZE : Données brutes provenant de fichiers CSV externes ─────
    f"{HDFS_NAMENODE}/data/bronze/csv/etablissements":
        "Bronze - Fichiers CSV FINESS : établissements de santé",
    f"{HDFS_NAMENODE}/data/bronze/csv/hospitalisations":
        "Bronze - Fichiers CSV ATIH : données d'hospitalisation",
    f"{HDFS_NAMENODE}/data/bronze/csv/deces":
        "Bronze - Fichiers CSV INSEE : décès en France",
    f"{HDFS_NAMENODE}/data/bronze/csv/satisfaction":
        "Bronze - Fichiers CSV HAS : satisfaction patients (ESATIS 2013-2020)",

    # ── COUCHE SILVER : Données nettoyées + RGPD ─────────────────────────────
    f"{HDFS_NAMENODE}/data/silver/patient":
        "Silver - Patients nettoyés, dédupliqués et pseudonymisés (SHA-256)",
    f"{HDFS_NAMENODE}/data/silver/consultation":
        "Silver - Consultations nettoyées (types, dates, nulls corrigés)",
    f"{HDFS_NAMENODE}/data/silver/diagnostic":
        "Silver - Diagnostics normalisés (codes CIM-10 validés)",
    f"{HDFS_NAMENODE}/data/silver/professionnel_sante":
        "Silver - Professionnels de santé dédupliqués (RPPS/ADELI)",
    f"{HDFS_NAMENODE}/data/silver/etablissements":
        "Silver - Établissements de santé nettoyés (FINESS validé)",
    f"{HDFS_NAMENODE}/data/silver/hospitalisations":
        "Silver - Hospitalisations nettoyées (dates, durées calculées)",
    f"{HDFS_NAMENODE}/data/silver/deces":
        "Silver - Décès nettoyés (coordonnées géographiques enrichies)",
    f"{HDFS_NAMENODE}/data/silver/satisfaction":
        "Silver - Satisfaction patients consolidée (tous millésimes 2013-2020)",

    # ── COUCHE GOLD : Dimensions du modèle en constellation ──────────────────
    f"{HDFS_NAMENODE}/data/gold/dimensions/dim_temps":
        "Gold - DIM_TEMPS : calendrier complet 2010-2030 (SCD Type 1)",
    f"{HDFS_NAMENODE}/data/gold/dimensions/dim_patient":
        "Gold - DIM_PATIENT : patients pseudonymisés (SCD Type 2 - historique)",
    f"{HDFS_NAMENODE}/data/gold/dimensions/dim_etablissement":
        "Gold - DIM_ETABLISSEMENT : établissements FINESS (SCD Type 1)",
    f"{HDFS_NAMENODE}/data/gold/dimensions/dim_diagnostic":
        "Gold - DIM_DIAGNOSTIC : nomenclature CIM-10 complète (SCD Type 1)",
    f"{HDFS_NAMENODE}/data/gold/dimensions/dim_professionnel":
        "Gold - DIM_PROFESSIONNEL : professionnels RPPS/ADELI (SCD Type 2)",
    f"{HDFS_NAMENODE}/data/gold/dimensions/dim_geographie":
        "Gold - DIM_GEOGRAPHIE : codes postaux → départements → régions",
    f"{HDFS_NAMENODE}/data/gold/dimensions/dim_question":
        "Gold - DIM_QUESTION : 7 questions ESATIS satisfaction patients",

    # ── COUCHE GOLD : Tables de faits du modèle en constellation ─────────────
    f"{HDFS_NAMENODE}/data/gold/faits/fait_consultation":
        "Gold - FAIT_CONSULTATION : actes médicaux (clé: temps×patient×médecin×diagnostic)",
    f"{HDFS_NAMENODE}/data/gold/faits/fait_hospitalisation":
        "Gold - FAIT_HOSPITALISATION : séjours hospitaliers avec durée et mode sortie",
    f"{HDFS_NAMENODE}/data/gold/faits/fait_deces":
        "Gold - FAIT_DECES : décès avec cause, âge et localisation géographique",
    f"{HDFS_NAMENODE}/data/gold/faits/fait_satisfaction":
        "Gold - FAIT_SATISFACTION : scores ESATIS par établissement et question",

    # ── KPIs précalculés ──────────────────────────────────────────────────────
    f"{HDFS_NAMENODE}/data/gold/kpis":
        "Gold - KPIs précalculés : agrégations mensuelles/trimestrielles pour BI",

    # ── Warehouse Hive ────────────────────────────────────────────────────────
    f"{HDFS_NAMENODE}/warehouse/dwh_chu.db":
        "Warehouse Hive : base dwh_chu (externe, pointe vers /data/gold/)",
}


# ─────────────────────────────────────────────────────────────────────────────
# FONCTIONS UTILITAIRES
# ─────────────────────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """
    Crée et retourne une session Spark connectée au cluster CHU.
    Configuration minimale pour les opérations HDFS uniquement.
    """
    log.info("Initialisation de la session Spark...")
    spark = (
        SparkSession.builder
        .appName("CHU_00_Setup_HDFS")
        .master("spark://chu-spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
        # Pas besoin d'exécuteurs pour ce job (opérations HDFS légères)
        .config("spark.executor.instances", "1")
        .config("spark.executor.memory", "512m")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info(f"Session Spark créée - App ID : {spark.sparkContext.applicationId}")
    return spark


def hdfs_mkdir(spark: SparkSession, path: str) -> bool:
    """
    Crée un dossier HDFS via l'API Hadoop FileSystem.
    Idempotent : ne lève pas d'erreur si le dossier existe déjà.

    Args:
        spark: Session Spark active
        path : Chemin HDFS complet (ex: hdfs://chu-namenode:9000/data/bronze/...)

    Returns:
        True si créé ou déjà existant, False en cas d'erreur
    """
    try:
        # Accès à l'API Java Hadoop FileSystem via JVM Spark
        jvm = spark._jvm
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(
            jvm.java.net.URI(HDFS_NAMENODE), hadoop_conf
        )
        hdfs_path = jvm.org.apache.hadoop.fs.Path(path)

        if fs.exists(hdfs_path):
            log.info(f"  [EXISTE] {path}")
            return True
        else:
            fs.mkdirs(hdfs_path)
            log.info(f"  [CRÉÉ]   {path}")
            return True
    except Exception as e:
        log.error(f"  [ERREUR] {path} → {e}")
        return False


def hdfs_set_permissions(spark: SparkSession, path: str, permissions: str = "777") -> bool:
    """
    Définit les permissions Unix sur un dossier HDFS.

    Args:
        spark      : Session Spark active
        path       : Chemin HDFS cible
        permissions: Permissions en octal (défaut: 777 pour dev)

    Returns:
        True si succès, False sinon
    """
    try:
        jvm = spark._jvm
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(
            jvm.java.net.URI(HDFS_NAMENODE), hadoop_conf
        )
        hdfs_path = jvm.org.apache.hadoop.fs.Path(path)
        perm = jvm.org.apache.hadoop.fs.permission.FsPermission(permissions)
        fs.setPermission(hdfs_path, perm)
        return True
    except Exception as e:
        log.warning(f"Permission {permissions} non appliquée sur {path} : {e}")
        return False


def verify_hdfs_structure(spark: SparkSession) -> dict:
    """
    Vérifie la structure HDFS en listant les dossiers racines créés.
    Retourne un dictionnaire avec les statistiques de la vérification.

    Returns:
        {
            "total_dirs": int,
            "existing_dirs": int,
            "missing_dirs": list,
            "hdfs_capacity_bytes": int,
            "hdfs_used_bytes": int
        }
    """
    log.info("\n" + "="*60)
    log.info("VÉRIFICATION DE LA STRUCTURE HDFS")
    log.info("="*60)

    try:
        jvm = spark._jvm
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(
            jvm.java.net.URI(HDFS_NAMENODE), hadoop_conf
        )

        existing = 0
        missing = []

        for path in HDFS_DIRECTORIES.keys():
            hdfs_path = jvm.org.apache.hadoop.fs.Path(path)
            if fs.exists(hdfs_path):
                existing += 1
            else:
                missing.append(path)

        # Statistiques de capacité du cluster HDFS
        status = fs.getStatus()
        capacity = status.getCapacity()
        used = status.getUsed()
        remaining = status.getRemaining()

        stats = {
            "total_dirs": len(HDFS_DIRECTORIES),
            "existing_dirs": existing,
            "missing_dirs": missing,
            "hdfs_capacity_gb": round(capacity / (1024**3), 2),
            "hdfs_used_gb": round(used / (1024**3), 2),
            "hdfs_remaining_gb": round(remaining / (1024**3), 2),
        }

        log.info(f"  Dossiers créés     : {existing}/{len(HDFS_DIRECTORIES)}")
        log.info(f"  Dossiers manquants : {len(missing)}")
        log.info(f"  Capacité HDFS      : {stats['hdfs_capacity_gb']} GB")
        log.info(f"  Espace utilisé     : {stats['hdfs_used_gb']} GB")
        log.info(f"  Espace disponible  : {stats['hdfs_remaining_gb']} GB")

        if missing:
            log.warning(f"  Dossiers manquants : {missing}")

        return stats

    except Exception as e:
        log.error(f"Erreur lors de la vérification HDFS : {e}")
        return {"error": str(e)}


def print_tree(spark: SparkSession, root_path: str, depth: int = 3):
    """
    Affiche l'arborescence HDFS de manière lisible (style 'tree').

    Args:
        spark     : Session Spark active
        root_path : Chemin racine à explorer
        depth     : Profondeur maximale d'exploration
    """
    try:
        jvm = spark._jvm
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(
            jvm.java.net.URI(HDFS_NAMENODE), hadoop_conf
        )

        def _list_recursive(path, current_depth, prefix=""):
            if current_depth > depth:
                return
            try:
                statuses = fs.listStatus(jvm.org.apache.hadoop.fs.Path(path))
                for i, status in enumerate(statuses):
                    is_last = (i == len(statuses) - 1)
                    connector = "└── " if is_last else "├── "
                    name = status.getPath().getName()
                    file_type = "[DIR]" if status.isDirectory() else f"[{status.getLen()} bytes]"
                    log.info(f"  {prefix}{connector}{name} {file_type}")
                    if status.isDirectory():
                        extension = "    " if is_last else "│   "
                        _list_recursive(str(status.getPath()), current_depth + 1, prefix + extension)
            except Exception:
                pass

        log.info(f"\nArborescence HDFS : {root_path}")
        log.info("─" * 50)
        _list_recursive(root_path, 1)

    except Exception as e:
        log.warning(f"Impossible d'afficher l'arborescence : {e}")


# ─────────────────────────────────────────────────────────────────────────────
# SCRIPT PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def main():
    """
    Point d'entrée principal du script de setup HDFS.
    Étapes :
      1. Création session Spark
      2. Création de tous les dossiers HDFS
      3. Application des permissions
      4. Vérification de la structure
      5. Affichage de l'arborescence
    """
    start_time = datetime.now()

    log.info("=" * 70)
    log.info("  CHU DATA WAREHOUSE - INITIALISATION STRUCTURE HDFS")
    log.info("  Architecture Médaillon : Bronze / Silver / Gold")
    log.info(f"  Démarrage : {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 70)

    spark = None
    errors = 0
    created = 0

    try:
        # ── ÉTAPE 1 : Connexion Spark ──────────────────────────────────────
        log.info("\n[ÉTAPE 1/4] Connexion au cluster Spark...")
        spark = create_spark_session()
        log.info(f"  Spark Master : spark://chu-spark-master:7077")
        log.info(f"  HDFS NameNode : {HDFS_NAMENODE}")

        # ── ÉTAPE 2 : Création des dossiers ───────────────────────────────
        log.info(f"\n[ÉTAPE 2/4] Création de {len(HDFS_DIRECTORIES)} dossiers HDFS...")
        log.info("-" * 60)

        for path, description in HDFS_DIRECTORIES.items():
            success = hdfs_mkdir(spark, path)
            if success:
                created += 1
                hdfs_set_permissions(spark, path, "777")
            else:
                errors += 1

        log.info(f"\n  Résultat : {created} dossiers OK, {errors} erreurs")

        # ── ÉTAPE 3 : Vérification ─────────────────────────────────────────
        log.info("\n[ÉTAPE 3/4] Vérification de la structure HDFS...")
        stats = verify_hdfs_structure(spark)

        # ── ÉTAPE 4 : Affichage de l'arborescence ─────────────────────────
        log.info("\n[ÉTAPE 4/4] Arborescence complète HDFS /data :")
        print_tree(spark, f"{HDFS_NAMENODE}/data", depth=3)

        # ── RÉSUMÉ FINAL ──────────────────────────────────────────────────
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        log.info("\n" + "=" * 70)
        log.info("  RÉSUMÉ DE L'INITIALISATION HDFS")
        log.info("=" * 70)
        log.info(f"  Dossiers créés/vérifiés : {created}/{len(HDFS_DIRECTORIES)}")
        log.info(f"  Erreurs rencontrées      : {errors}")
        log.info(f"  Durée totale             : {duration:.1f} secondes")
        log.info(f"  Statut                   : {'✅ SUCCÈS' if errors == 0 else '⚠️ PARTIEL'}")
        log.info("=" * 70)
        log.info("\nProchaine étape : Exécuter 01_create_hive_schema.py")
        log.info("  Commande : spark-submit /opt/airflow/dags/jobs/01_create_hive_schema.py")

        if errors > 0:
            sys.exit(1)

    except KeyboardInterrupt:
        log.warning("\nInterruption par l'utilisateur.")
        sys.exit(130)

    except Exception as e:
        log.error(f"\nErreur critique : {e}", exc_info=True)
        sys.exit(1)

    finally:
        if spark:
            spark.stop()
            log.info("\nSession Spark fermée.")


if __name__ == "__main__":
    main()
