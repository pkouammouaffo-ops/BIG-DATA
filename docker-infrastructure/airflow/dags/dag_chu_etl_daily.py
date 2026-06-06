#!/usr/bin/env python3
# =============================================================================
# LIVRABLE 2 - DAG AIRFLOW : ORCHESTRATION DU PIPELINE ETL CHU
# =============================================================================
# Projet    : CHU Data Warehouse - Architecture Médaillon
# Auteur    : Groupe 6 BigData CESI
# Date      : Juin 2026
# Version   : 1.0
#
# DESCRIPTION :
#   DAG Airflow orchestrant l'ensemble du pipeline ETL du Data Warehouse CHU.
#   Ce DAG suit l'architecture médaillon : Bronze → Silver → Gold → Hive.
#
#   PLANIFICATION :
#     - Quotidienne à 02h00 UTC (hors heures de pointe hospitalières)
#     - catchup=False (pas de rétro-exécution automatique)
#     - Exécution : 02:00 → ~04:30 UTC (~2h30 estimé)
#
#   GRAPHE DE DÉPENDANCES :
#
#     start
#       │
#       ├─── setup_hdfs ─────────────────────────────────────────────────┐
#       │                                                                 │
#       └─── create_hive_schema ──────────────────────────────────────────┤
#                                                                         │
#                                                        wait_both_setup ─┘
#                                                               │
#                                                     extract_postgres (E1)
#                                                               │
#                                                       clean_bronze (T1)
#                                                               │
#                                                     deduplicate_silver (T2)
#                                                               │
#                                                    pseudonymize_rgpd (T3)
#                                                               │
#                                                   build_dimensions (T4)
#                                                               │
#                                                      build_facts (T5)
#                                                               │
#                                                     catalog_hive (T6)
#                                                               │
#                                                            end
#
#   RETRY POLICY :
#     - 3 tentatives automatiques
#     - Délai entre tentatives : 5 minutes
#     - Alertes email en cas d'échec (configurable)
#
#   VARIABLES AIRFLOW REQUISES :
#     - CHU_RGPD_SALT : Sel cryptographique pour SHA-256 (secret)
#     - CHU_SPARK_MASTER : URL Spark Master (défaut: spark://chu-spark-master:7077)
#
# ACCÈS :
#   http://localhost:8080 (Airflow UI)
#   Connexion Spark : spark_default (à configurer dans Airflow Connections)
# =============================================================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import logging

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION GLOBALE
# ─────────────────────────────────────────────────────────────────────────────

# Répertoire des jobs PySpark (dans le container Spark master)
JOBS_DIR = "/tmp/jobs"

# Driver JDBC PostgreSQL
JDBC_JAR = "/opt/spark/jars/postgresql-jdbc.jar"

# Commande de base spark-submit via docker exec
SPARK_SUBMIT = "docker exec chu-spark-master /opt/spark/bin/spark-submit"
SPARK_MASTER  = "spark://chu-spark-master:7077"
SPARK_MEM     = "1g"

# Récupération du sel RGPD depuis les variables Airflow
# En production : stocker dans Airflow Variables (chiffrées) ou Vault
def get_rgpd_salt() -> str:
    try:
        return Variable.get("CHU_RGPD_SALT", default_var="CHU_RGPD_SALT_2026")
    except Exception:
        return "CHU_RGPD_SALT_2026"


# ─────────────────────────────────────────────────────────────────────────────
# ARGUMENTS PAR DÉFAUT
# ─────────────────────────────────────────────────────────────────────────────

default_args = {
    "owner":              "chu-data-team",
    "depends_on_past":    False,
    "start_date":         datetime(2024, 1, 1),
    "email":              ["data-ops@chu.fr"],
    "email_on_failure":   True,
    "email_on_retry":     False,
    "retries":            3,
    "retry_delay":        timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout":  timedelta(hours=4),
}


# ─────────────────────────────────────────────────────────────────────────────
# CALLBACKS
# ─────────────────────────────────────────────────────────────────────────────

def on_failure_callback(context):
    """Callback exécuté en cas d'échec d'une tâche."""
    task_id    = context["task_instance"].task_id
    dag_id     = context["task_instance"].dag_id
    exec_date  = context["execution_date"]
    exception  = context.get("exception")
    log.error(
        f"❌ TÂCHE ÉCHOUÉE\n"
        f"   DAG        : {dag_id}\n"
        f"   Tâche      : {task_id}\n"
        f"   Date       : {exec_date}\n"
        f"   Exception  : {exception}"
    )


def on_success_callback(context):
    """Callback exécuté en cas de succès du DAG complet."""
    dag_id    = context["task_instance"].dag_id
    exec_date = context["execution_date"]
    log.info(f"✅ PIPELINE TERMINÉ : {dag_id} le {exec_date}")


def log_xcom_stats(**context):
    """Récupère et log les statistiques via XCom pour le monitoring."""
    ti = context["task_instance"]
    # Log des informations d'exécution pour le monitoring
    log.info(f"Exécution du DAG CHU : {context['ds']}")
    log.info(f"Execution date : {context['execution_date']}")
    return {"execution_date": str(context["ds"]), "status": "running"}


# ─────────────────────────────────────────────────────────────────────────────
# DÉFINITION DU DAG
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="dag_chu_etl_daily",
    default_args=default_args,
    description="Pipeline ETL quotidien CHU - Architecture Médaillon Bronze→Silver→Gold→Hive",
    schedule_interval="0 2 * * *",    # Chaque jour à 02h00 UTC
    catchup=False,
    max_active_runs=1,                # Pas d'exécutions concurrentes
    tags=["chu", "etl", "healthcare", "medallion", "livrable2"],
    doc_md="""
## DAG ETL CHU - Data Warehouse Architecture Médaillon

### Description
Pipeline de données quotidien transformant les données brutes du SIH (Système d'Information Hospitalier)
en données analytiques accessibles dans le Data Warehouse Gold.

### Architecture
**Bronze** → **Silver** → **Gold** → **Hive**

### Planification
Exécution quotidienne à **02:00 UTC** (hors heures de pointe hospitalières).
Durée estimée : **2h30** (extraction PostgreSQL ~30min + transformations ~1h30 + catalogage ~30min).

### Monitoring
- Airflow UI : http://localhost:8080
- Spark UI   : http://localhost:4040 (pendant l'exécution)
- HDFS UI    : http://localhost:9870

### Contact
Équipe Data : data-ops@chu.fr
    """,
) as dag:

    # ── DÉBUT ─────────────────────────────────────────────────────────────
    start = EmptyOperator(
        task_id="start",
        doc_md="Point d'entrée du pipeline ETL CHU."
    )

    # ── LOG XCom ──────────────────────────────────────────────────────────
    log_execution = PythonOperator(
        task_id="log_execution_context",
        python_callable=log_xcom_stats,
        doc_md="Enregistre le contexte d'exécution pour le monitoring."
    )

    # ── SETUP HDFS ────────────────────────────────────────────────────────
    setup_hdfs = BashOperator(
        task_id="setup_hdfs_structure",
        bash_command=(
            f"{SPARK_SUBMIT} --master {SPARK_MASTER} "
            f"--conf spark.executor.memory={SPARK_MEM} "
            f"{JOBS_DIR}/00_setup_hdfs.py --date {{{{ ds }}}}"
        ),
        on_failure_callback=on_failure_callback,
        doc_md="Crée la structure HDFS Bronze/Silver/Gold si elle n'existe pas encore.",
    )

    # ── CREATE HIVE SCHEMA ────────────────────────────────────────────────
    create_schema = BashOperator(
        task_id="create_hive_schema",
        bash_command=(
            f"{SPARK_SUBMIT} --master {SPARK_MASTER} "
            f"--conf spark.executor.memory={SPARK_MEM} "
            f"--conf spark.hadoop.hive.metastore.uris=thrift://chu-hive-server:9083 "
            f"{JOBS_DIR}/01_create_hive_schema.py --date {{{{ ds }}}}"
        ),
        on_failure_callback=on_failure_callback,
        doc_md="Crée la base Hive dwh_chu avec les 11 tables DDL si elles n'existent pas.",
    )

    # ── EXTRACT POSTGRESQL (E1) ───────────────────────────────────────────
    extract_postgres = BashOperator(
        task_id="extract_postgres",
        bash_command=(
            f"{SPARK_SUBMIT} --master {SPARK_MASTER} "
            f"--jars {JDBC_JAR} "
            f"--conf spark.executor.memory={SPARK_MEM} "
            f"{JOBS_DIR}/E1_extract_postgres.py --date {{{{ ds }}}}"
        ),
        on_failure_callback=on_failure_callback,
        doc_md="Extrait 11 tables SIH PostgreSQL → Bronze HDFS (Parquet/Snappy).",
    )

    # ── LOAD CSV BRONZE (E2) ─────────────────────────────────────────────
    load_csv_bronze = BashOperator(
        task_id="load_csv_bronze",
        bash_command=(
            f"{SPARK_SUBMIT} --master {SPARK_MASTER} "
            f"--conf spark.executor.memory={SPARK_MEM} "
            f"{JOBS_DIR}/E2_load_csv_bronze.py --date {{{{ ds }}}}"
        ),
        on_failure_callback=on_failure_callback,
        doc_md="Charge 5 sources CSV Open Data → Bronze HDFS (Parquet/Snappy).",
    )

    # ── CLEAN BRONZE (T1) ─────────────────────────────────────────────────
    clean_bronze = BashOperator(
        task_id="clean_bronze",
        bash_command=(
            f"{SPARK_SUBMIT} --master {SPARK_MASTER} "
            f"--conf spark.executor.memory={SPARK_MEM} "
            f"{JOBS_DIR}/T1_clean_bronze.py --date {{{{ ds }}}}"
        ),
        on_failure_callback=on_failure_callback,
        doc_md="Nettoie les données Bronze → Silver (dates, formats CIM-10, doublons techniques).",
    )

    # ── DEDUPLICATE SILVER (T2) ───────────────────────────────────────────
    deduplicate_silver = BashOperator(
        task_id="deduplicate_silver",
        bash_command=(
            f"{SPARK_SUBMIT} --master {SPARK_MASTER} "
            f"--conf spark.executor.memory=2g "
            f"{JOBS_DIR}/T2_deduplicate_silver.py --date {{{{ ds }}}}"
        ),
        on_failure_callback=on_failure_callback,
        doc_md="Déduplique les patients (Jaro-Winkler 0.92) et les autres tables (row_number).",
    )

    # ── PSEUDONYMIZE RGPD (T3) ────────────────────────────────────────────
    pseudonymize_rgpd = BashOperator(
        task_id="pseudonymize_rgpd",
        bash_command=(
            f"{SPARK_SUBMIT} --master {SPARK_MASTER} "
            f"--conf spark.executor.memory={SPARK_MEM} "
            f"{JOBS_DIR}/T3_pseudonymize_rgpd.py --date {{{{ ds }}}} "
            "--salt CHU_RGPD_SALT_2026"
        ),
        on_failure_callback=on_failure_callback,
        doc_md="Pseudonymise les données patient RGPD (SHA-256 + sel).",
    )

    # ── BUILD DIMENSIONS (T4) ─────────────────────────────────────────────
    build_dimensions = BashOperator(
        task_id="build_dimensions",
        bash_command=(
            f"{SPARK_SUBMIT} --master {SPARK_MASTER} "
            f"--conf spark.executor.memory={SPARK_MEM} "
            f"{JOBS_DIR}/T4_build_dimensions.py --date {{{{ ds }}}}"
        ),
        on_failure_callback=on_failure_callback,
        doc_md="Construit les 7 dimensions Gold (SCD Type 1 & 2).",
    )

    # ── BUILD FACTS (T5) ──────────────────────────────────────────────────
    build_facts = BashOperator(
        task_id="build_facts",
        bash_command=(
            f"{SPARK_SUBMIT} --master {SPARK_MASTER} "
            f"--conf spark.executor.memory=2g "
            f"--conf spark.sql.autoBroadcastJoinThreshold=100m "
            f"{JOBS_DIR}/T5_build_facts.py --date {{{{ ds }}}}"
        ),
        on_failure_callback=on_failure_callback,
        doc_md="Construit les 4 tables de faits Gold (partitionnées annee/mois).",
    )

    # ── CATALOG HIVE + KPIs (T6) ──────────────────────────────────────────
    catalog_hive = BashOperator(
        task_id="catalog_hive_and_kpis",
        bash_command=(
            f"{SPARK_SUBMIT} --master {SPARK_MASTER} "
            f"--conf spark.executor.memory=2g "
            f"--conf spark.hadoop.hive.metastore.uris=thrift://chu-hive-server:9083 "
            f"--conf spark.sql.cbo.enabled=true "
            f"{JOBS_DIR}/T6_catalog_hive.py --date {{{{ ds }}}}"
        ),
        on_failure_callback=on_failure_callback,
        doc_md="Enregistre les tables Gold dans Hive Metastore et calcule 7 KPIs métier.",
    )

    # ── FIN ───────────────────────────────────────────────────────────────
    end = EmptyOperator(
        task_id="end",
        on_success_callback=on_success_callback,
        doc_md="Fin du pipeline ETL CHU. Données disponibles dans Hive dwh_chu."
    )

    # ─────────────────────────────────────────────────────────────────────
    # DÉFINITION DU GRAPHE DE DÉPENDANCES
    # ─────────────────────────────────────────────────────────────────────
    #
    # start
    #   ├── log_execution
    #   ├── setup_hdfs ──────────────────┐
    #   └── create_schema ──────────────┴── extract_postgres (E1) ──┐
    #                                                                 ├── clean_bronze (T1)
    #                                         load_csv_bronze (E2) ──┘
    #                                                   │
    #                                       deduplicate_silver (T2)
    #                                                   │
    #                                       pseudonymize_rgpd (T3)
    #                                                   │
    #                                       build_dimensions (T4)
    #                                                   │
    #                                          build_facts (T5)
    #                                                   │
    #                                       catalog_hive + KPIs (T6)
    #                                                   │
    #                                                  end

    start >> log_execution
    start >> [setup_hdfs, create_schema] >> extract_postgres
    # E2 démarre en parallèle de E1 (sources indépendantes)
    start >> load_csv_bronze
    # T1 attend que les deux extractions (E1 PostgreSQL + E2 CSV) soient terminées
    [extract_postgres, load_csv_bronze] >> clean_bronze
    (
        clean_bronze
        >> deduplicate_silver
        >> pseudonymize_rgpd
        >> build_dimensions
        >> build_facts
        >> catalog_hive
        >> end
    )
