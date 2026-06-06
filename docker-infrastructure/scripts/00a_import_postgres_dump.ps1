# =============================================================================
# SCRIPT 00a - IMPORT DU DUMP POSTGRESQL SIH (DATA2023) DANS CHU-POSTGRES
# =============================================================================
# Projet : CHU Data Warehouse - Architecture Médaillon
# Usage  : Exécuter depuis la racine du projet (Projet_cloud_healthcare_unit)
#
#   cd C:\Users\Administrateur\Desktop\CESI\Stockage_BigData\Projet_cloud_healthcare_unit
#   .\docker-infrastructure\scripts\00a_import_postgres_dump.ps1
#
# Ce script :
#   1. Vérifie que le conteneur chu-postgres est bien démarré
#   2. Vérifie si les tables SIH sont déjà présentes (évite un double import)
#   3. Copie le dump DATA2023 dans le conteneur
#   4. Importe le dump via pg_restore
#   5. Valide le chargement (13 tables, nombre de lignes)
# =============================================================================

$ErrorActionPreference = "Stop"

# ─── Configuration ────────────────────────────────────────────────────────────
$DUMP_PATH      = "DATA 2024\DATA 2024\BDD PostgreSQL\DATA2023"
$CONTAINER      = "chu-postgres"
$DB             = "sih_data"
$PG_USER        = "postgres"
$CONTAINER_DUMP = "/tmp/DATA2023.sql"

# Tables attendues après l'import (pour validation)
$TABLES_ATTENDUES = 13
$LIGNES_MIN_CONSULTATION = 1_000_000   # Seuil minimum pour valider l'import

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

function Write-Warn {
    param([string]$Message)
    Write-Host "    ⚠️  $Message" -ForegroundColor Yellow
}

function Write-Fail {
    param([string]$Message)
    Write-Host "    ❌ $Message" -ForegroundColor Red
}

# ─── Début ────────────────────────────────────────────────────────────────────

Write-Host ""
Write-Host "============================================================" -ForegroundColor White
Write-Host "  00a - IMPORT DUMP POSTGRESQL SIH (DATA2023)"               -ForegroundColor White
Write-Host "============================================================" -ForegroundColor White

# ── Étape 1 : Vérifier le fichier dump ───────────────────────────────────────
Write-Step "Vérification du fichier dump..."

if (-not (Test-Path $DUMP_PATH)) {
    Write-Fail "Fichier dump introuvable : $DUMP_PATH"
    Write-Host "    Vérifiez que vous exécutez le script depuis la racine du projet :" -ForegroundColor Red
    Write-Host "    cd C:\Users\Administrateur\Desktop\CESI\Stockage_BigData\Projet_cloud_healthcare_unit" -ForegroundColor Yellow
    exit 1
}

$dumpSize = (Get-Item $DUMP_PATH).Length / 1MB
Write-OK "Dump trouvé : $DUMP_PATH ($([math]::Round($dumpSize, 1)) MB)"

# ── Étape 2 : Vérifier que le conteneur tourne ───────────────────────────────
Write-Step "Vérification du conteneur $CONTAINER..."

$containerStatus = docker inspect --format "{{.State.Status}}" $CONTAINER 2>$null
if ($LASTEXITCODE -ne 0 -or $containerStatus -ne "running") {
    Write-Fail "Conteneur '$CONTAINER' non démarré (statut: $containerStatus)"
    Write-Host "    Lancez l'infrastructure : docker-compose up -d" -ForegroundColor Yellow
    exit 1
}

Write-OK "Conteneur $CONTAINER en cours d'exécution"

# ── Étape 3 : Vérifier si les tables sont déjà présentes ─────────────────────
Write-Step "Vérification des tables SIH existantes..."

$nbTables = docker exec $CONTAINER psql -U $PG_USER -d $DB -t -c `
    "SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public';" 2>$null
$nbTables = $nbTables.Trim()

if ([int]$nbTables -ge $TABLES_ATTENDUES) {
    Write-OK "$nbTables tables déjà présentes dans sih_data.public"
    Write-Host ""
    Write-Host "    Les tables SIH sont déjà chargées. Détail :" -ForegroundColor White
    docker exec $CONTAINER psql -U $PG_USER -d $DB -c `
        "SELECT relname AS table, n_live_tup AS lignes FROM pg_stat_user_tables WHERE schemaname = 'public' ORDER BY n_live_tup DESC;"
    Write-Host ""
    Write-Warn "Import ignoré (données déjà présentes). Utilisez -Force pour forcer."
    Write-Host "    .\docker-infrastructure\scripts\00a_import_postgres_dump.ps1 -Force" -ForegroundColor Yellow

    # Vérifier si le paramètre -Force est passé
    if ($args -notcontains "-Force") {
        exit 0
    }
    Write-Warn "Mode -Force activé : remplacement des données existantes..."
} else {
    Write-Warn "$nbTables table(s) trouvée(s) — import nécessaire"
}

# ── Étape 4 : Copier le dump dans le conteneur ───────────────────────────────
Write-Step "Copie du dump dans le conteneur ($CONTAINER_DUMP)..."

docker cp $DUMP_PATH "${CONTAINER}:${CONTAINER_DUMP}"
if ($LASTEXITCODE -ne 0) {
    Write-Fail "Erreur lors du docker cp"
    exit 1
}

Write-OK "Dump copié dans le conteneur"

# ── Étape 5 : Importer le dump ────────────────────────────────────────────────
Write-Step "Import du dump via pg_restore (peut prendre 1-3 minutes)..."

docker exec $CONTAINER pg_restore `
    -U $PG_USER `
    -d $DB `
    --no-owner `
    --no-privileges `
    --no-comments `
    $CONTAINER_DUMP

# pg_restore retourne exit code 1 même en cas de succès partiel (warnings normaux)
# On valide via le compte de tables plutôt que via $LASTEXITCODE

Write-OK "pg_restore terminé"

# ── Étape 6 : Validation ──────────────────────────────────────────────────────
Write-Step "Validation du chargement..."

$nbTablesApres = docker exec $CONTAINER psql -U $PG_USER -d $DB -t -c `
    "SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public';" 2>$null
$nbTablesApres = [int]$nbTablesApres.Trim()

if ($nbTablesApres -lt $TABLES_ATTENDUES) {
    Write-Fail "Seulement $nbTablesApres tables trouvées (attendu : $TABLES_ATTENDUES)"
    Write-Host "    Vérifiez les logs : docker logs $CONTAINER" -ForegroundColor Yellow
    exit 1
}

Write-OK "$nbTablesApres tables importées"

# Vérifier la table principale
$lignesConsultation = docker exec $CONTAINER psql -U $PG_USER -d $DB -t -c `
    "SELECT n_live_tup FROM pg_stat_user_tables WHERE relname = 'Consultation';" 2>$null
$lignesConsultation = [int]$lignesConsultation.Trim()

if ($lignesConsultation -lt $LIGNES_MIN_CONSULTATION) {
    Write-Fail "Table Consultation : $lignesConsultation lignes (attendu : > $LIGNES_MIN_CONSULTATION)"
    exit 1
}

Write-OK "Table Consultation : $lignesConsultation lignes"

# ── Récapitulatif final ───────────────────────────────────────────────────────
Write-Host ""
Write-Host "============================================================" -ForegroundColor White
Write-Host "  RÉCAPITULATIF DES TABLES IMPORTÉES"                         -ForegroundColor White
Write-Host "============================================================" -ForegroundColor White

docker exec $CONTAINER psql -U $PG_USER -d $DB -c `
    "SELECT relname AS table, n_live_tup AS lignes FROM pg_stat_user_tables WHERE schemaname = 'public' ORDER BY n_live_tup DESC;"

Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "  ✅ Import terminé avec succès !"                            -ForegroundColor Green
Write-Host "  Prochaine étape : upload des CSV vers HDFS"                 -ForegroundColor Green
Write-Host "  → .\docker-infrastructure\scripts\00b_run_csv_upload.ps1"  -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""
