# RECAP — Pipeline ETL Big Data CHU (Branche : Bertrand)

> **Auteur :** Bertrand  
> **Date :** 07 juin 2026  
> **Dépôt :** https://github.com/pkouammouaffo-ops/BIG-DATA  
> **Branche :** `Bertrand`

---

## 1. Ce qui a été fait

### Infrastructure Docker (11 conteneurs)

| Conteneur | Image | Port(s) | Rôle |
|---|---|---|---|
| `chu-namenode` | hadoop:3.3.x | 9870, 9000 | HDFS NameNode |
| `chu-datanode1/2/3` | hadoop:3.3.x | 9864/9865/9866 | HDFS DataNodes |
| `chu-spark-master` | spark:3.5.x | 8888, 7077 | Spark Master |
| `chu-spark-worker1/2` | spark:3.5.x | 8081, 8082 | Spark Workers |
| `chu-postgres` | postgres:15 | 5433 | SIH + Metastore + Airflow |
| `chu-hive-server` | chu-hive-custom:4.0.0 | 10000, 10002 | HiveServer2 |
| `chu-nifi` | apache/nifi:1.23.2 | 9090 | Ingestion fichiers |
| `chu-airflow` | apache/airflow:2.9.1 | 8085 | Orchestration DAG |

**Démarrage :**
```powershell
cd docker-infrastructure
docker-compose up -d
```

---

## 2. Architecture Médaillon — État du pipeline

```
[PostgreSQL SIH]  [CSV HDFS Staging]
       |                  |
   E1 ✅              E2 ❌ NON EXÉCUTÉ (voir section 5)
       |                  |
       ▼                  ▼
  /data/bronze/postgres   /data/bronze/csv (partiel)
       |
   T1 ✅ Silver Nettoyage
       |
   T2 ✅ Silver Déduplication
       |
   T3 ✅ RGPD Pseudonymisation
       |
   T4 ✅ Gold Dimensions (7)
       |
   T5 ✅ Gold Faits (4)
       |
   T6 ✅ KPIs + Catalogue
       |
   Hive ⚠️ dwh_chu créée, tables en attente
```

---

## 3. Détail des scripts ETL

### E1 — Extraction PostgreSQL → Bronze HDFS ✅
- **Script :** `airflow/dags/jobs/E1_extract_postgres.py`
- **Résultat HDFS :** `/data/bronze/postgres/` (11 tables, ~3.5M lignes)
- **Partition :** `_extraction_date=2026-06-02`
- **Tables extraites :** `patient`, `consultation`, `hospitalisation`, `diagnostic`, `medecin`, `etablissement`, `service`, `acte_medical`, `prescription`, `resultat_exam`, `antecedent`

**Commande :**
```bash
docker exec chu-spark-master spark-submit \
  --master spark://chu-spark-master:7077 \
  --jars /opt/spark/jars/postgresql-42.7.3.jar \
  /tmp/jobs/E1_extract_postgres.py --date 2026-06-02
```

---

### E2 — Chargement CSV → Bronze HDFS ❌ NON EXÉCUTÉ

> ⚠️ **STATUT : NON EXÉCUTÉ**
>
> **Raison :** Le fichier `deces.csv` (1.9 GB) sature la RAM WSL2 de Docker Desktop, provoquant un crash du pipe Docker (`500 Internal Server Error`).
>
> **Fichiers en staging HDFS** (prêts à traiter) :
> - `/data/staging/csv/deces/deces.csv` → **1.9 GB** → 💥 crash RAM
> - `/data/staging/csv/etablissements/etablissement_sante.csv` → 82 MB
> - `/data/staging/csv/professionnel_sante_open/professionnel_sante.csv` → 76 MB
> - `/data/staging/csv/satisfaction/ESATIS48H_MCO_recueil2017_donnees.csv` → 204 KB
> - `/data/staging/csv/hospitalisations/Hospitalisations.csv` → 261 KB ✅ (déjà en Bronze)
>
> **Seule la table hospitalisations a été chargée** (`/data/bronze/csv/hospitalisations/`)
>
> **Pour exécuter E2 sur une machine avec plus de RAM (≥16 GB WSL2 allouée) :**
> ```bash
> docker exec chu-spark-master spark-submit \
>   --master spark://chu-spark-master:7077 \
>   --conf spark.executor.memory=2g \
>   --conf spark.driver.memory=1g \
>   /tmp/jobs/E2_load_csv_bronze.py --date 2026-06-07
> ```
>
> **Alternative recommandée :** exclure `deces.csv` du périmètre initial ou augmenter la RAM WSL2 dans `%USERPROFILE%\.wslconfig` :
> ```ini
> [wsl2]
> memory=12GB
> ```

---

### T1 — Nettoyage Silver ✅
- **Script :** `airflow/dags/jobs/T1_clean_bronze.py`
- **Résultat :** `/data/silver/` → 10 tables nettoyées
- **Lignes traitées :** 2 507 107
- **Opérations :** cast types, suppression nulls critiques, normalisation texte

---

### T2 — Déduplication Silver ✅
- **Script :** `airflow/dags/jobs/T2_deduplicate_silver.py`
- **Résultat :** `/data/silver/*_dedup/` → 10 tables dédupliquées
- **Méthode finale :** `dropDuplicates(["nom","prenom","date_naissance","sexe","code_postal"])` (Jaro-Winkler supprimé car trop lent — 277 min → 50 sec)
- **Colonne marqueur :** `_dedup_method = "exact"`

---

### T3 — Pseudonymisation RGPD ✅
- **Script :** `airflow/dags/jobs/T3_pseudonymize_rgpd.py`
- **Résultat :** `/data/silver/patient_rgpd/`
- **Champs pseudonymisés :** `nom`, `prenom` → hash SHA-256, `id_patient` → conservé hashé

---

### T4 — Construction Dimensions Gold ✅
- **Script :** `airflow/dags/jobs/T4_build_dimensions.py`
- **Résultat :** `/data/gold/dimensions/`
- **7 dimensions créées :**

| Dimension | HDFS path |
|---|---|
| `dim_temps` | `/data/gold/dimensions/dim_temps/` |
| `dim_patient` | `/data/gold/dimensions/dim_patient/` |
| `dim_etablissement` | `/data/gold/dimensions/dim_etablissement/` |
| `dim_diagnostic` | `/data/gold/dimensions/dim_diagnostic/` |
| `dim_professionnel` | `/data/gold/dimensions/dim_professionnel/` |
| `dim_geographie` | `/data/gold/dimensions/dim_geographie/` |
| `dim_question` | `/data/gold/dimensions/dim_question/` |

---

### T5 — Construction Faits Gold ✅
- **Script :** `airflow/dags/jobs/T5_build_facts.py`
- **Résultat :** `/data/gold/faits/`
- **4 tables de faits créées :**

| Fait | Partitionné par | HDFS path |
|---|---|---|
| `fait_consultation` | annee/mois | `/data/gold/faits/fait_consultation/` |
| `fait_hospitalisation` | annee/mois | `/data/gold/faits/fait_hospitalisation/` |
| `fait_deces` | annee/mois | `/data/gold/faits/fait_deces/` |
| `fait_satisfaction` | annee | `/data/gold/faits/fait_satisfaction/` |

---

### T6 — KPIs + Catalogue ✅
- **Script :** `airflow/dags/jobs/T6_catalog_hive.py`
- **Résultat :**
  - `/data/gold/kpis/kpi_01_volumetrie/`
  - `/data/gold/kpis/rapport_performance_20260602/`

---

## 4. Tables Hive — dwh_chu ⚠️ EN ATTENTE

- **Base de données :** `dwh_chu` → **CRÉÉE** dans HiveServer2
- **Tables :** **NON ENCORE CRÉÉES** (script prêt, bloqué par instabilité Docker pipe WSL2)
- **Script DDL :** `create_hive_tables.hql` (à la racine de `docker-infrastructure/`)
- **Tables prévues (11) :** `dim_temps`, `dim_patient`, `dim_etablissement`, `dim_diagnostic`, `dim_professionnel`, `dim_geographie`, `dim_question`, `fait_consultation`, `fait_hospitalisation`, `fait_deces`, `fait_satisfaction`

**Pour créer les tables (une fois Docker stable) :**
```bash
# Option 1 : via beeline dans le conteneur
docker cp create_hive_tables.hql chu-hive-server:/tmp/
docker exec chu-hive-server beeline -u "jdbc:hive2://localhost:10000" -f /tmp/create_hive_tables.hql

# Option 2 : via Python + pyhive (connexion TCP directe port 10000)
pip install pyhive thrift thrift-sasl
python run_hive_ddl.py
```

---

## 5. Problème E2 — Détails techniques

**Symptôme :** `docker exec` retourne `500 Internal Server Error` pour l'API Docker Desktop
**Cause :** WSL2 sature la RAM (8 GB machine) lors du traitement de `deces.csv` (1.9 GB)
**Log :** `request returned 500 Internal Server Error for API route ... /pipe/dockerDesktopLinuxEngine`

**Workaround appliqué :**
1. `wsl --shutdown` pour libérer la RAM
2. Relancer Docker Desktop
3. Éviter le traitement de `deces.csv` jusqu'à avoir plus de RAM disponible

---

## 6. Accès aux conteneurs — Pour l'équipe

### Prérequis
- Docker Desktop installé (≥ 4.x) avec WSL2 activé
- **16 GB RAM recommandés** (8 GB minimum mais instable sur gros fichiers)
- Git cloné : `git clone https://github.com/pkouammouaffo-ops/BIG-DATA.git`

### Démarrage
```powershell
# 1. Cloner et aller dans le bon dossier
git clone https://github.com/pkouammouaffo-ops/BIG-DATA.git
cd BIG-DATA
git checkout Bertrand
cd docker-infrastructure

# 2. Copier et configurer les variables d'environnement
Copy-Item .env.example .env
# Éditer .env : adapter PROJECT_ROOT à votre chemin local

# 3. Démarrer l'infrastructure
docker-compose up -d

# 4. Vérifier que tous les conteneurs sont UP
docker ps
```

### Interfaces Web (après démarrage)

| Interface | URL | Identifiants |
|---|---|---|
| HDFS NameNode | http://localhost:9870 | — |
| Spark Master | http://localhost:8888 | — |
| Spark Worker 1 | http://localhost:8081 | — |
| Spark Worker 2 | http://localhost:8082 | — |
| Airflow | http://localhost:8085 | admin / voir .env |
| NiFi | http://localhost:9090 | admin / voir .env |
| Hive WebUI | http://localhost:10002 | — |

### Copier les scripts Spark dans le conteneur
```powershell
# Copier tous les jobs ETL
docker cp airflow\dags\jobs\E1_extract_postgres.py chu-spark-master:/tmp/jobs/
docker cp airflow\dags\jobs\E2_load_csv_bronze.py chu-spark-master:/tmp/jobs/
docker cp airflow\dags\jobs\T1_clean_bronze.py chu-spark-master:/tmp/jobs/
docker cp airflow\dags\jobs\T2_deduplicate_silver.py chu-spark-master:/tmp/jobs/
docker cp airflow\dags\jobs\T3_pseudonymize_rgpd.py chu-spark-master:/tmp/jobs/
docker cp airflow\dags\jobs\T4_build_dimensions.py chu-spark-master:/tmp/jobs/
docker cp airflow\dags\jobs\T5_build_facts.py chu-spark-master:/tmp/jobs/
docker cp airflow\dags\jobs\T6_catalog_hive.py chu-spark-master:/tmp/jobs/
```

### Exécuter le pipeline dans l'ordre
```powershell
$DATE = "2026-06-07"
$SPARK_SUBMIT = "docker exec chu-spark-master /opt/spark/bin/spark-submit --master spark://chu-spark-master:7077 --jars /opt/spark/jars/postgresql-42.7.3.jar"

# Étape 1 — Bronze PostgreSQL
Invoke-Expression "$SPARK_SUBMIT /tmp/jobs/E1_extract_postgres.py --date $DATE"

# Étape 2 — Bronze CSV (⚠️ nécessite ≥16GB RAM pour deces.csv)
# Invoke-Expression "$SPARK_SUBMIT /tmp/jobs/E2_load_csv_bronze.py --date $DATE"

# Étapes 3 à 8 — Silver → Gold
foreach ($job in @("T1_clean_bronze","T2_deduplicate_silver","T3_pseudonymize_rgpd","T4_build_dimensions","T5_build_facts","T6_catalog_hive")) {
    Write-Host "Lancement $job ..."
    Invoke-Expression "docker exec chu-spark-master /opt/spark/bin/spark-submit --master spark://chu-spark-master:7077 /tmp/jobs/$job.py"
}
```

---

## 7. Fichiers importants dans ce commit

```
docker-infrastructure/
├── docker-compose.yml              ← Infrastructure 11 conteneurs
├── .env.example                    ← Variables à copier en .env
├── .gitignore                      ← Exclusions git
├── create_hive_tables.hql          ← DDL Hive (dwh_chu — 7 dims + 4 faits)
├── RECAP_PIPELINE.md               ← CE FICHIER
├── airflow/dags/
│   ├── dag_chu_etl_daily.py        ← DAG Airflow orchestration
│   └── jobs/
│       ├── E1_extract_postgres.py  ✅ Exécuté
│       ├── E2_load_csv_bronze.py   ❌ NON exécuté (deces.csv trop lourd)
│       ├── T1_clean_bronze.py      ✅ Exécuté
│       ├── T2_deduplicate_silver.py ✅ Exécuté (Jaro-Winkler → exact)
│       ├── T3_pseudonymize_rgpd.py ✅ Exécuté
│       ├── T4_build_dimensions.py  ✅ Exécuté
│       ├── T5_build_facts.py       ✅ Exécuté
│       └── T6_catalog_hive.py      ✅ Exécuté
├── hive/
│   └── Dockerfile                  ← Image Hive custom 4.0
├── scripts/
│   ├── 00a_import_postgres_dump.ps1
│   ├── 00b_run_csv_upload.ps1
│   └── 00b_upload_csv_hdfs.sh
└── Architetcture_médaillon_CHU/    ← Documentation architecture
```

---

## 8. Prochaines étapes

- [ ] **E2** — Relancer sur machine avec ≥16 GB RAM (traiter deces.csv, etablissements, satisfaction)
- [ ] **Hive tables** — Exécuter `create_hive_tables.hql` via beeline une fois Docker stable
- [ ] **Airflow DAG** — Activer et tester le DAG `dag_chu_etl_daily`
- [ ] **Validation données** — COUNT(*) sur les 11 tables dwh_chu
