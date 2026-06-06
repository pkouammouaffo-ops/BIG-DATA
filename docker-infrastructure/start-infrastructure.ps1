# ========================================
# SCRIPT DE DÉMARRAGE - Infrastructure CHU
# ========================================
# Ce script démarre l'infrastructure Docker de manière progressive
# pour éviter les problèmes de dépendances et permettre l'initialisation complète

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  DÉMARRAGE INFRASTRUCTURE CHU" -ForegroundColor Cyan
Write-Host "  Architecture Médaillon Big Data" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Vérification Docker
Write-Host "🔍 Vérification de Docker..." -ForegroundColor Yellow
try {
    docker --version | Out-Null
    docker-compose --version | Out-Null
    Write-Host "✅ Docker et Docker Compose sont installés" -ForegroundColor Green
} catch {
    Write-Host "❌ ERREUR : Docker n'est pas installé ou non démarré" -ForegroundColor Red
    Write-Host "   Veuillez démarrer Docker Desktop et réessayer" -ForegroundColor Red
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

Write-Host "📁 Dossier de travail : $scriptPath" -ForegroundColor Cyan
Write-Host ""

# Menu de choix
Write-Host "Choisissez une option :" -ForegroundColor Yellow
Write-Host "  [1] Démarrage complet (tous les services)" -ForegroundColor White
Write-Host "  [2] Démarrage progressif (recommandé pour la première fois)" -ForegroundColor White
Write-Host "  [3] Arrêter tous les services" -ForegroundColor White
Write-Host "  [4] Redémarrer tous les services" -ForegroundColor White
Write-Host "  [5] État des conteneurs" -ForegroundColor White
Write-Host "  [6] Supprimer tout (⚠️  données comprises)" -ForegroundColor Red
Write-Host ""

$choice = Read-Host "Votre choix (1-6)"

switch ($choice) {
    "1" {
        Write-Host "`n🚀 Démarrage complet de tous les services..." -ForegroundColor Cyan
        docker-compose up -d
        Write-Host ""
        Write-Host "⏳ Attente de 30 secondes pour l'initialisation..." -ForegroundColor Yellow
        Start-Sleep -Seconds 30
        docker-compose ps
    }
    
    "2" {
        Write-Host "`n🚀 Démarrage progressif (recommandé)..." -ForegroundColor Cyan
        Write-Host ""
        
        # Phase 1 : HDFS
        Write-Host "📦 Phase 1/4 : Démarrage HDFS (NameNode + DataNodes)..." -ForegroundColor Yellow
        docker-compose up -d chu-namenode chu-datanode1 chu-datanode2 chu-datanode3
        Write-Host "⏳ Attente de 30 secondes..." -ForegroundColor Gray
        Start-Sleep -Seconds 30
        
        # Vérification HDFS
        $hdfsStatus = docker ps --filter "name=chu-namenode" --filter "status=running" --format "{{.Names}}"
        if ($hdfsStatus) {
            Write-Host "✅ HDFS démarré avec succès" -ForegroundColor Green
            Write-Host "   Accès : http://localhost:9870" -ForegroundColor Cyan
        } else {
            Write-Host "❌ Problème avec HDFS" -ForegroundColor Red
            docker logs chu-namenode --tail 20
            exit 1
        }
        Write-Host ""
        
        # Phase 2 : Spark
        Write-Host "📦 Phase 2/4 : Démarrage Spark (Master + Workers)..." -ForegroundColor Yellow
        docker-compose up -d chu-spark-master chu-spark-worker1 chu-spark-worker2
        Write-Host "⏳ Attente de 20 secondes..." -ForegroundColor Gray
        Start-Sleep -Seconds 20
        
        $sparkStatus = docker ps --filter "name=chu-spark-master" --filter "status=running" --format "{{.Names}}"
        if ($sparkStatus) {
            Write-Host "✅ Spark démarré avec succès" -ForegroundColor Green
            Write-Host "   Accès : http://localhost:8888" -ForegroundColor Cyan
        } else {
            Write-Host "❌ Problème avec Spark" -ForegroundColor Red
        }
        Write-Host ""
        
        # Phase 3 : PostgreSQL
        Write-Host "📦 Phase 3/4 : Démarrage PostgreSQL..." -ForegroundColor Yellow
        docker-compose up -d chu-postgres
        Write-Host "⏳ Attente de 20 secondes (initialisation SQL)..." -ForegroundColor Gray
        Start-Sleep -Seconds 20
        
        $pgStatus = docker ps --filter "name=chu-postgres" --filter "status=running" --format "{{.Names}}"
        if ($pgStatus) {
            Write-Host "✅ PostgreSQL démarré avec succès" -ForegroundColor Green
            Write-Host "   Port : localhost:5433" -ForegroundColor Cyan
            
            # Vérification de l'initialisation
            $initCheck = docker logs chu-postgres 2>&1 | Select-String "INITIALISATION POSTGRESQL TERMINÉE"
            if ($initCheck) {
                Write-Host "✅ Bases de données initialisées (sih_data, airflow_db, metastore_db)" -ForegroundColor Green
            }
        } else {
            Write-Host "❌ Problème avec PostgreSQL" -ForegroundColor Red
        }
        Write-Host ""
        
        # Phase 4 : Hive, NiFi, Airflow
        Write-Host "📦 Phase 4/4 : Démarrage Hive, NiFi et Airflow..." -ForegroundColor Yellow
        docker-compose up -d chu-hive-server chu-nifi chu-airflow
        Write-Host "⏳ Attente de 60 secondes (ces services sont plus lents au démarrage)..." -ForegroundColor Gray
        Start-Sleep -Seconds 60
        
        Write-Host "✅ Tous les services ont été démarrés" -ForegroundColor Green
        Write-Host ""
        
        # Affichage de l'état
        Write-Host "========================================" -ForegroundColor Cyan
        Write-Host "  ÉTAT DES CONTENEURS" -ForegroundColor Cyan
        Write-Host "========================================" -ForegroundColor Cyan
        docker-compose ps
        
        Write-Host ""
        Write-Host "========================================" -ForegroundColor Cyan
        Write-Host "  INTERFACES WEB DISPONIBLES" -ForegroundColor Cyan
        Write-Host "========================================" -ForegroundColor Cyan
        Write-Host "  HDFS NameNode    : http://localhost:9870" -ForegroundColor White
        Write-Host "  Spark Master     : http://localhost:8888" -ForegroundColor White
        Write-Host "  Airflow          : http://localhost:8085 (admin / chu_airflow_2026)" -ForegroundColor White
        Write-Host "  NiFi             : http://localhost:9090/nifi (admin / chu_nifi_admin_2026)" -ForegroundColor White
        Write-Host "  HiveServer2 UI   : http://localhost:10002" -ForegroundColor White
        Write-Host "========================================" -ForegroundColor Cyan
    }
    
    "3" {
        Write-Host "`n🛑 Arrêt de tous les services..." -ForegroundColor Yellow
        docker-compose down
        Write-Host "✅ Tous les services ont été arrêtés" -ForegroundColor Green
        Write-Host "💾 Les données sont conservées dans les volumes Docker" -ForegroundColor Cyan
    }
    
    "4" {
        Write-Host "`n🔄 Redémarrage de tous les services..." -ForegroundColor Yellow
        docker-compose restart
        Write-Host "✅ Tous les services ont été redémarrés" -ForegroundColor Green
        docker-compose ps
    }
    
    "5" {
        Write-Host "`n📊 État des conteneurs :" -ForegroundColor Cyan
        docker-compose ps
        Write-Host ""
        Write-Host "📊 Ressources utilisées :" -ForegroundColor Cyan
        docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"
    }
    
    "6" {
        Write-Host "`n⚠️  ATTENTION : Cette action va supprimer TOUTES LES DONNÉES" -ForegroundColor Red
        Write-Host "   (Volumes Docker, bases de données, fichiers HDFS, etc.)" -ForegroundColor Red
        $confirm = Read-Host "`nÊtes-vous sûr ? Tapez 'OUI' pour confirmer"
        
        if ($confirm -eq "OUI") {
            Write-Host "`n🗑️  Suppression de tous les services et volumes..." -ForegroundColor Yellow
            docker-compose down -v
            Write-Host "✅ Tous les services et données ont été supprimés" -ForegroundColor Green
        } else {
            Write-Host "❌ Suppression annulée" -ForegroundColor Yellow
        }
    }
    
    default {
        Write-Host "`n❌ Choix invalide. Veuillez choisir une option entre 1 et 6." -ForegroundColor Red
        exit 1
    }
}

Write-Host ""
Write-Host "✅ Opération terminée !" -ForegroundColor Green
Write-Host ""
