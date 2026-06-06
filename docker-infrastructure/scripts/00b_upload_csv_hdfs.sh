#!/usr/bin/env bash
# =============================================================================
# SCRIPT 00b - UPLOAD DES FICHIERS CSV VERS HDFS (ZONE STAGING)
# =============================================================================
# Projet : CHU Data Warehouse - Architecture Médaillon
# Usage  : Ce script tourne DANS le conteneur chu-namenode.
#
# Il crée les dossiers de staging dans HDFS et y place les CSV
# copiés au préalable dans /tmp/chu_csv/ du conteneur.
#
# ÉTAPES D'EXÉCUTION (depuis Windows/PowerShell) :
#
#   1. Créer les dossiers locaux temporaires et copier les CSV dans le namenode :
#      (adapter les chemins source vers votre DATA 2024)
#
#   $DATA = "C:\Users\Administrateur\Desktop\CESI\Stockage_BigData\Projet_cloud_healthcare_unit\DATA 2024\DATA 2024"
#
#   docker exec chu-namenode mkdir -p /tmp/chu_csv/hospitalisations
#   docker exec chu-namenode mkdir -p /tmp/chu_csv/deces
#   docker exec chu-namenode mkdir -p /tmp/chu_csv/etablissements
#   docker exec chu-namenode mkdir -p /tmp/chu_csv/professionnel_sante_open
#   docker exec chu-namenode mkdir -p /tmp/chu_csv/satisfaction
#
#   docker cp "$DATA\Hospitalisation\Hospitalisations.csv"                         chu-namenode:/tmp/chu_csv/hospitalisations/
#   docker cp "$DATA\DECES EN FRANCE\deces.csv"                                    chu-namenode:/tmp/chu_csv/deces/
#   docker cp "$DATA\Etablissement de SANTE\etablissement_sante.csv"               chu-namenode:/tmp/chu_csv/etablissements/
#   docker cp "$DATA\Etablissement de SANTE\professionnel_sante.csv"               chu-namenode:/tmp/chu_csv/professionnel_sante_open/
#   docker cp "$DATA\Satisfaction\ESATIS48H_MCO_recueil2017_donnees.csv"           chu-namenode:/tmp/chu_csv/satisfaction/
#
#   2. Copier et exécuter ce script dans le namenode :
#      docker cp docker-infrastructure/scripts/00b_upload_csv_hdfs.sh chu-namenode:/tmp/
#      docker exec chu-namenode bash /tmp/00b_upload_csv_hdfs.sh
#
# =============================================================================

set -e  # Arrêter à la première erreur

HDFS_STAGING="/data/staging/csv"
LOCAL_CSV="/tmp/chu_csv"

echo "============================================================"
echo "  00b - UPLOAD CSV VERS HDFS STAGING"
echo "============================================================"
echo ""

# ─── Fonction utilitaire ──────────────────────────────────────────────────────
upload_csv() {
    local source_name="$1"
    local local_file="$2"
    local hdfs_folder="$3"

    echo "  [${source_name}]"
    echo "    Source locale : ${local_file}"
    echo "    Destination   : ${HDFS_STAGING}/${hdfs_folder}/"

    # Vérifier que le fichier local existe
    if [ ! -f "${local_file}" ]; then
        echo "    ❌ ERREUR : fichier introuvable → ${local_file}"
        echo "       Vérifiez que le docker cp a bien été exécuté."
        return 1
    fi

    # Créer le dossier HDFS si nécessaire
    hdfs dfs -mkdir -p "${HDFS_STAGING}/${hdfs_folder}"

    # Supprimer l'ancien fichier s'il existe (remplacement complet)
    hdfs dfs -rm -f "${HDFS_STAGING}/${hdfs_folder}/$(basename ${local_file})" 2>/dev/null || true

    # Uploader le CSV
    hdfs dfs -put "${local_file}" "${HDFS_STAGING}/${hdfs_folder}/"

    # Vérifier l'upload
    local nb_bytes
    nb_bytes=$(hdfs dfs -du -s "${HDFS_STAGING}/${hdfs_folder}/" | awk '{print $1}')
    echo "    ✅ Upload OK (${nb_bytes} octets dans HDFS)"
    echo ""
}

# ─── Upload de chaque source CSV ─────────────────────────────────────────────

echo "Démarrage des uploads CSV → HDFS..."
echo ""

upload_csv \
    "hospitalisations" \
    "${LOCAL_CSV}/hospitalisations/Hospitalisations.csv" \
    "hospitalisations"

upload_csv \
    "deces" \
    "${LOCAL_CSV}/deces/deces.csv" \
    "deces"

upload_csv \
    "etablissements" \
    "${LOCAL_CSV}/etablissements/etablissement_sante.csv" \
    "etablissements"

upload_csv \
    "professionnel_sante_open" \
    "${LOCAL_CSV}/professionnel_sante_open/professionnel_sante.csv" \
    "professionnel_sante_open"

upload_csv \
    "satisfaction" \
    "${LOCAL_CSV}/satisfaction/ESATIS48H_MCO_recueil2017_donnees.csv" \
    "satisfaction"

# ─── Récapitulatif HDFS ───────────────────────────────────────────────────────
echo "============================================================"
echo "  RÉCAPITULATIF HDFS STAGING"
echo "============================================================"
hdfs dfs -ls -R "${HDFS_STAGING}" | grep -v "^d"
echo ""
echo "✅ Upload terminé. Vous pouvez maintenant lancer E2 :"
echo "   docker exec chu-spark-master spark-submit \\"
echo "     --master spark://chu-spark-master:7077 \\"
echo "     --conf spark.executor.memory=1g \\"
echo "     /opt/airflow/dags/jobs/E2_load_csv_bronze.py"
echo "============================================================"
