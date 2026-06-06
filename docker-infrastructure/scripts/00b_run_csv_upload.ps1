# =============================================================================
# SCRIPT 00b - UPLOAD DES FICHIERS CSV OPEN DATA VERS HDFS
# =============================================================================
# Projet : CHU Data Warehouse - Architecture Médaillon
# Usage  : Exécuter depuis la racine du projet (Projet_cloud_healthcare_unit)
#
#   cd C:\Users\Administrateur\Desktop\CESI\Stockage_BigData\Projet_cloud_healthcare_unit
#   .\docker-infrastructure\scripts\00b_run_csv_upload.ps1
#
# Ce script :
#   1. Vérifie que les fichiers CSV sources existent sur le disque
#   2. Vérifie que le conteneur chu-namenode est démarré
#   3. Crée les dossiers temporaires dans le conteneur
#   4. Copie les CSV dans le conteneur (docker cp)
#   5. Lance le script bash 00b_upload_csv_hdfs.sh dans le namenode
#      (qui crée les dossiers HDFS et fait le hdfs dfs -put)
#   6. Valide la présence des fichiers dans HDFS
# =============================================================================

$ErrorActionPreference = "Stop"

# ─── Configuration ────────────────────────────────────────────────────────────
$DATA_ROOT  = "DATA 2024\DATA 2024"
$CONTAINER  = "chu-namenode"
$HDFS_STAGING = "/data/staging/csv"

# Correspondance : [nom logique] → [chemin source local] → [dossier dans conteneur]
$CSV_SOURCES = @(
    @{
        Nom         = "hospitalisations"
        FichierLocal = "$DATA_ROOT\Hospitalisation\Hospitalisations.csv"
        DossierLocal = "/tmp/chu_csv/hospitalisations"
    },
    @{
        Nom         = "deces"
        FichierLocal = "$DATA_ROOT\DECES EN FRANCE\deces.csv"
        DossierLocal = "/tmp/chu_csv/deces"
    },
    @{
        Nom         = "etablissements"
        FichierLocal = "$DATA_ROOT\Etablissement de SANTE\etablissement_sante.csv"
        DossierLocal = "/tmp/chu_csv/etablissements"
    },
    @{
        Nom         = "professionnel_sante_open"
        FichierLocal = "$DATA_ROOT\Etablissement de SANTE\professionnel_sante.csv"
        DossierLocal = "/tmp/chu_csv/professionnel_sante_open"
    },
    @{
        Nom         = "satisfaction"
        FichierLocal = "$DATA_ROOT\Satisfaction\ESATIS48H_MCO_recueil2017_donnees.csv"
        DossierLocal = "/tmp/chu_csv/satisfaction"
    }
)

# ─── Fonctions ────────────────────────────────────────────────────────────────

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "  ► $Message" -ForegroundColor Cyan
}

function Write-OK {
    param([string]$Message)
    Write-Host "    ✅ $Message" -ForegroundColor Green
}

function Write-Fail {
    param([string]$Message)
    Write-Host "    ❌ $Message" -ForegroundColor Red
}

# ─── Début ────────────────────────────────────────────────────────────────────

Write-Host ""
Write-Host "============================================================" -ForegroundColor White
Write-Host "  00b - UPLOAD CSV OPEN DATA VERS HDFS STAGING"              -ForegroundColor White
Write-Host "============================================================" -ForegroundColor White

# ── Étape 1 : Vérifier les fichiers CSV locaux ───────────────────────────────
Write-Step "Vérification des fichiers CSV locaux..."

$erreurs = 0
foreach ($source in $CSV_SOURCES) {
    if (Test-Path $source.FichierLocal) {
        $taille = [math]::Round((Get-Item $source.FichierLocal).Length / 1KB, 0)
        Write-OK "$($source.Nom) - $($source.FichierLocal) ($taille KB)"
    } else {
        Write-Fail "Fichier introuvable : $($source.FichierLocal)"
        $erreurs++
    }
}

if ($erreurs -gt 0) {
    Write-Host ""
    Write-Host "  $erreurs fichier(s) manquant(s). Vérifiez le dossier DATA 2024." -ForegroundColor Red
    exit 1
}

# ── Étape 2 : Vérifier le conteneur namenode ─────────────────────────────────
Write-Step "Vérification du conteneur $CONTAINER..."

$containerStatus = docker inspect --format "{{.State.Status}}" $CONTAINER 2>$null
if ($LASTEXITCODE -ne 0 -or $containerStatus -ne "running") {
    Write-Fail "Conteneur '$CONTAINER' non démarré. Lancez : docker-compose up -d"
    exit 1
}

Write-OK "Conteneur $CONTAINER en cours d'exécution"

# ── Étape 3 : Créer les dossiers temporaires dans le conteneur ───────────────
Write-Step "Création des dossiers temporaires dans le conteneur..."

$dossiers = ($CSV_SOURCES | ForEach-Object { $_.DossierLocal }) -join " "
docker exec $CONTAINER bash -c "mkdir -p $dossiers"
Write-OK "Dossiers /tmp/chu_csv/* créés"

# ── Étape 4 : Copier les CSV dans le conteneur ───────────────────────────────
Write-Step "Copie des CSV dans le conteneur (docker cp)..."

foreach ($source in $CSV_SOURCES) {
    $nomFichier = Split-Path $source.FichierLocal -Leaf
    Write-Host "    Copie : $nomFichier..." -ForegroundColor White
    docker cp $source.FichierLocal "${CONTAINER}:$($source.DossierLocal)/"
    if ($LASTEXITCODE -ne 0) {
        Write-Fail "Erreur lors de la copie de $($source.FichierLocal)"
        exit 1
    }
    Write-OK "$nomFichier → $($source.DossierLocal)/"
}

# ── Étape 5 : Copier et lancer le script bash dans le namenode ───────────────
Write-Step "Upload des CSV de /tmp vers HDFS (hdfs dfs -put)..."

$scriptBash = "docker-infrastructure\scripts\00b_upload_csv_hdfs.sh"
docker cp $scriptBash "${CONTAINER}:/tmp/00b_upload_csv_hdfs.sh"
docker exec $CONTAINER bash /tmp/00b_upload_csv_hdfs.sh

if ($LASTEXITCODE -ne 0) {
    Write-Fail "Erreur lors de l'upload HDFS"
    exit 1
}

# ── Étape 6 : Validation HDFS ─────────────────────────────────────────────────
Write-Step "Validation des fichiers dans HDFS..."

foreach ($source in $CSV_SOURCES) {
    $hdfsPath = "$HDFS_STAGING/$($source.Nom)"
    $result = docker exec $CONTAINER hdfs dfs -count $hdfsPath 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-OK "$hdfsPath présent dans HDFS"
    } else {
        Write-Fail "$hdfsPath introuvable dans HDFS"
    }
}

# ── Récapitulatif ─────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "============================================================" -ForegroundColor White
Write-Host "  RÉCAPITULATIF HDFS /data/staging/csv"                       -ForegroundColor White
Write-Host "============================================================" -ForegroundColor White
docker exec $CONTAINER hdfs dfs -ls -R /data/staging/csv

Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "  ✅ Upload CSV terminé !"                                    -ForegroundColor Green
Write-Host "  Prochaine étape : lancer le job Spark E2"                  -ForegroundColor Green
Write-Host "  → docker exec chu-spark-master spark-submit \"             -ForegroundColor Green
Write-Host "      --master spark://chu-spark-master:7077 \"              -ForegroundColor Green
Write-Host "      --conf spark.executor.memory=1g \"                     -ForegroundColor Green
Write-Host "      /opt/airflow/dags/jobs/E2_load_csv_bronze.py"          -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""
