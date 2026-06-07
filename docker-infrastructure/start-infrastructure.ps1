# ========================================
# SCRIPT DE DÉMARRAGE - Infrastructure CHU
# ========================================
# Ce script démarre l'infrastructure Docker de manière progressive
# pour éviter les problèmes de dépendances et permettre l'initialisation complète

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  DEMARRAGE INFRASTRUCTURE CHU" -ForegroundColor Cyan
Write-Host "  Architecture Medaillon Big Data" -ForegroundColor Cyan
Write-Host "======================================" -ForegroundColor Cyan
Write-Host ""

# Verification Docker
Write-Host "[CHECK] Verification de Docker..." -ForegroundColor Yellow
try {
    docker --version | Out-Null
    docker-compose --version | Out-Null
    Write-Host "[OK] Docker et Docker Compose sont installes" -ForegroundColor Green
} catch {
    Write-Host "[ERREUR] Docker n'est pas installe ou non demarre" -ForegroundColor Red
    Write-Host "   Veuillez demarrer Docker Desktop et reessayer" -ForegroundColor Red
    exit 1
}

Write-Host ""

# Vérification du dossier
$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptPath

if (-not (Test-Path "docker-compose.yml")) {
    Write-Host "❌ ERREUR : Fichier docker-compose.yml introuvable" -ForegroundColor Red
    Write-Host "   Assurez-vous d'être dans le dossier docker-infrastructure" -ForegroundColor Red
    exit 1
}

Write-Host "[INFO] Dossier de travail : $scriptPath" -ForegroundColor Cyan
Write-Host ""

# Menu de choix
Write-Host "Choisissez une option :" -ForegroundColor Yellow
    Write-Host "  [1] Demarrage complet (tous les services)" -ForegroundColor White
    Write-Host "  [2] Demarrage progressif (recommande pour la premiere fois)" -ForegroundColor White
    Write-Host "  [3] Arreter tous les services" -ForegroundColor White
    Write-Host "  [4] Redemarrer tous les services" -ForegroundColor White
    Write-Host "  [5] Etat des conteneurs" -ForegroundColor White
    Write-Host "  [6] Supprimer tout (ATTENTION : donnees comprises)" -ForegroundColor Red
Write-Host ""

$choice = Read-Host "Votre choix (1-6)"

switch ($choice) {
    "1" {
        Write-Host "`n[LANCEMENT] Demarrage complet de tous les services..." -ForegroundColor Cyan
        docker-compose up -d
        Write-Host ""
        Write-Host "[ATTENTE] Attente de 30 secondes pour l'initialisation..." -ForegroundColor Yellow
        Start-Sleep -Seconds 30
        docker-compose ps
    }
    
    "2" {
        Write-Host "`n[LANCEMENT] Demarrage progressif (recommande)..." -ForegroundColor Cyan
        Write-Host ""
        
        # Phase 1 : HDFS
        Write-Host "[ETAPE] Phase 1/4 : Demarrage HDFS (NameNode + DataNodes)..." -ForegroundColor Yellow
        docker-compose up -d chu-namenode chu-datanode1 chu-datanode2 chu-datanode3
        Write-Host "[ATTENTE] Attente de 30 secondes..." -ForegroundColor Gray
        Start-Sleep -Seconds 30
        
        # Verification HDFS
        $hdfsStatus = docker ps --filter "name=chu-namenode" --filter "status=running" --format "{{.Names}}"
        if ($hdfsStatus) {
            Write-Host "[OK] HDFS demarre avec succes" -ForegroundColor Green
            Write-Host "   Acces : http://localhost:9870" -ForegroundColor Cyan
        } else {
            Write-Host "[ERREUR] Probleme avec HDFS" -ForegroundColor Red
            docker logs chu-namenode --tail 20
            exit 1
        }
        Write-Host ""
        
        # Phase 2 : Spark
        Write-Host "[ETAPE] Phase 2/4 : Demarrage Spark (Master + Workers)..." -ForegroundColor Yellow
        docker-compose up -d chu-spark-master chu-spark-worker1 chu-spark-worker2
        Write-Host "[ATTENTE] Attente de 20 secondes..." -ForegroundColor Gray
        Start-Sleep -Seconds 20
        
        $sparkStatus = docker ps --filter "name=chu-spark-master" --filter "status=running" --format "{{.Names}}"
        if ($sparkStatus) {
            Write-Host "[OK] Spark demarre avec succes" -ForegroundColor Green
            Write-Host "   Acces : http://localhost:8888" -ForegroundColor Cyan
        } else {
            Write-Host "[ERREUR] Probleme avec Spark" -ForegroundColor Red
        }
        Write-Host ""
        
        # Phase 3 : PostgreSQL
        Write-Host "[ETAPE] Phase 3/4 : Demarrage PostgreSQL..." -ForegroundColor Yellow
        docker-compose up -d chu-postgres
        Write-Host "[ATTENTE] Attente de 20 secondes (initialisation SQL)..." -ForegroundColor Gray
        Start-Sleep -Seconds 20
        
        $pgStatus = docker ps --filter "name=chu-postgres" --filter "status=running" --format "{{.Names}}"
        if ($pgStatus) {
            Write-Host "[OK] PostgreSQL demarre avec succes" -ForegroundColor Green
            Write-Host "   Port : localhost:5433" -ForegroundColor Cyan
            
            # Verification de l'initialisation
            $initCheck = docker logs chu-postgres 2>&1 | Select-String "database system is ready"
            if ($initCheck) {
                Write-Host "[OK] Bases de donnees initialisees (sih_data, airflow_db, metastore_db)" -ForegroundColor Green
            }
        } else {
            Write-Host "[ERREUR] Probleme avec PostgreSQL" -ForegroundColor Red
        }
        Write-Host ""
        
        # Phase 4 : Hive, NiFi, Airflow
        Write-Host "[ETAPE] Phase 4/4 : Demarrage Hive, NiFi et Airflow..." -ForegroundColor Yellow
        docker-compose up -d chu-hive-server chu-nifi chu-airflow
        Write-Host "[ATTENTE] Attente de 60 secondes (ces services sont plus lents au demarrage)..." -ForegroundColor Gray
        Start-Sleep -Seconds 60
        
        Write-Host "[OK] Tous les services ont ete demarres" -ForegroundColor Green
        Write-Host ""
        
        # Affichage de l'etat
        Write-Host "======================================" -ForegroundColor Cyan
        Write-Host "  ETAT DES CONTENEURS" -ForegroundColor Cyan
        Write-Host "======================================" -ForegroundColor Cyan
        docker-compose ps
        
        Write-Host ""
        Write-Host "======================================" -ForegroundColor Cyan
        Write-Host "  INTERFACES WEB DISPONIBLES" -ForegroundColor Cyan
        Write-Host "======================================" -ForegroundColor Cyan
        Write-Host "  HDFS NameNode    : http://localhost:9870" -ForegroundColor White
        Write-Host "  Spark Master     : http://localhost:8888" -ForegroundColor White
        Write-Host "  Airflow          : http://localhost:8085 (admin / chu_airflow_2026)" -ForegroundColor White
        Write-Host "  NiFi             : http://localhost:9090/nifi (admin / chu_nifi_admin_2026)" -ForegroundColor White
        Write-Host "  HiveServer2 UI   : http://localhost:10002" -ForegroundColor White
        Write-Host "======================================" -ForegroundColor Cyan
    }
    
    "3" {
        Write-Host "`n[ARRET] Arret de tous les services..." -ForegroundColor Yellow
        docker-compose down
        Write-Host "[OK] Tous les services ont ete arretes" -ForegroundColor Green
        Write-Host "[INFO] Les donnees sont conservees dans les volumes Docker" -ForegroundColor Cyan
    }
    
    "4" {
        Write-Host "`n[RESTART] Redemarrage de tous les services..." -ForegroundColor Yellow
        docker-compose restart
        Write-Host "[OK] Tous les services ont ete redemarres" -ForegroundColor Green
        docker-compose ps
    }
    
    "5" {
        Write-Host "`n[STATS] Etat des conteneurs :" -ForegroundColor Cyan
        docker-compose ps
        Write-Host ""
        Write-Host "[STATS] Ressources utilisees :" -ForegroundColor Cyan
        docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"
    }
    
    "6" {
    Write-Host "`n[ATTENTION] Cette action va supprimer TOUTES LES DONNEES" -ForegroundColor Red
        Write-Host "   (Volumes Docker, bases de donnees, fichiers HDFS, etc.)" -ForegroundColor Red
        $confirm = Read-Host "`nEtes-vous sur ? Tapez 'OUI' pour confirmer"
        
        if ($confirm -eq "OUI") {
            Write-Host "`n[SUPPRESSION] Suppression de tous les services et volumes..." -ForegroundColor Yellow
            docker-compose down -v
            Write-Host "[OK] Tous les services et donnees ont ete supprimes" -ForegroundColor Green
        } else {
            Write-Host "[ANNULE] Suppression annulee" -ForegroundColor Yellow
        }
    }
    
    default {
        Write-Host "`n[ERREUR] Choix invalide. Veuillez choisir une option entre 1 et 6." -ForegroundColor Red
        exit 1
    }
}

Write-Host ""
Write-Host "[OK] Operation terminee !" -ForegroundColor Green
Write-Host ""
