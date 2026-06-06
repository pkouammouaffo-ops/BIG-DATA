# 🏥 **Infrastructure Docker - Projet CHU**

Infrastructure Big Data conteneurisée pour le projet **Cloud Healthcare Unit (CHU)** - Entrepôt de données décisionnel.

## 📋 **Sommaire**

1. [Architecture](#architecture)
2. [Prérequis](#prérequis)
3. [Démarrage rapide](#démarrage-rapide)
4. [Gestion des services](#gestion-des-services)
5. [Accès aux interfaces Web](#accès-aux-interfaces-web)
6. [Commandes utiles](#commandes-utiles)
7. [Résolution de problèmes](#résolution-de-problèmes)
8. [Livrable 2 — Pipeline ETL complet](#livrable-2--pipeline-etl-complet)

---

## 🏗️ **Architecture**

### **Composants déployés (11 conteneurs)**

| Service | Conteneur | Port(s) | Description |
|---------|-----------|---------|-------------|
| **HDFS NameNode** | `chu-namenode` | 9870, 9000 | Gestionnaire du système de fichiers distribué |
| **HDFS DataNode 1** | `chu-datanode1` | 9864 | Nœud de stockage 1 |
| **HDFS DataNode 2** | `chu-datanode2` | 9865 | Nœud de stockage 2 |
| **HDFS DataNode 3** | `chu-datanode3` | 9866 | Nœud de stockage 3 |
| **Spark Master** | `chu-spark-master` | 8888, 7077 | Moteur de traitement distribué (master) |
| **Spark Worker 1** | `chu-spark-worker1` | 8081 | Worker Spark 1 (2 cores, 2GB RAM) |
| **Spark Worker 2** | `chu-spark-worker2` | 8082 | Worker Spark 2 (2 cores, 2GB RAM) |
| **PostgreSQL** | `chu-postgres` | 5433 | Base de données (SIH + Airflow + Metastore) |
| **Hive Server** | `chu-hive-server` | 10000, 10002 | Catalogue de données + Interface SQL |
| **Apache NiFi** | `chu-nifi` | 9090 | Ingestion de fichiers CSV/FTP |
| **Apache Airflow** | `chu-airflow` | 8085 | Orchestration des jobs ETL |

### **Réseau Docker**

Tous les conteneurs sont connectés au réseau isolé **`chu_network`** (bridge) pour éviter les conflits avec d'autres projets (ex: Solvy).

---

## ✅ **Prérequis**

### **Logiciels requis**

- ✅ **Docker Desktop** : Version 4.x ou supérieure
- ✅ **Docker Compose** : Version 2.x ou supérieure
- ✅ **Windows 10/11** : Avec WSL2 activé (recommandé)
- ✅ **8 GB RAM minimum** : 16 GB recommandés pour performances optimales
- ✅ **20 GB d'espace disque** : Pour les volumes Docker

### **Vérification**

```powershell
# Vérifier Docker
docker --version
docker-compose --version

# Vérifier que Docker Desktop est démarré
docker ps
```

---

## 🚀 **Démarrage rapide**

### **Étape 1 : Se positionner dans le dossier**

```powershell
cd "C:\Users\Administrateur\Desktop\CESI\Stockage_BigData\Projet_cloud_healthcare_unit\docker-infrastructure"
```

### **Étape 2 : Vérifier les ports disponibles**

⚠️ **Important** : Avant de démarrer, vérifiez qu'aucun autre service n'utilise les ports suivants :

```powershell
# Lister les ports utilisés
netstat -ano | findstr "9870 9000 8888 7077 5433 10000 9090 8085"
```

Si des ports sont occupés, vous pouvez les modifier dans `docker-compose.yml`.

### **Étape 3 : Démarrage progressif (recommandé)**

**🔵 Phase 1 : Démarrer HDFS (NameNode + DataNodes)**

```powershell
docker-compose up -d chu-namenode chu-datanode1 chu-datanode2 chu-datanode3
```

⏳ **Attendre 30 secondes** que HDFS soit complètement initialis é.

Vérifier : http://localhost:9870 (HDFS NameNode Web UI)

---

**🟢 Phase 2 : Démarrer Spark (Master + Workers)**

```powershell
docker-compose up -d chu-spark-master chu-spark-worker1 chu-spark-worker2
```

⏳ **Attendre 20 secondes**.

Vérifier : http://localhost:8888 (Spark Master Web UI)

---

**🟡 Phase 3 : Démarrer PostgreSQL**

```powershell
docker-compose up -d chu-postgres
```

⏳ **Attendre 15 secondes** que l'initialisation SQL s'exécute.

Vérifier les logs :
```powershell
docker logs chu-postgres | Select-String "INITIALISATION POSTGRESQL TERMINÉE"
```

---

**🟠 Phase 4 : Démarrer Hive, NiFi, Airflow**

```powershell
docker-compose up -d chu-hive-server chu-nifi chu-airflow
```

⏳ **Attendre 60-90 secondes** (ces services mettent plus de temps au premier démarrage).

---

### **Étape 4 : Vérifier l'état de tous les conteneurs**

```powershell
docker-compose ps
```

✅ **Tous les conteneurs doivent être en état `Up (healthy)` ou `Up`.**

---

## 🎛️ **Gestion des services**

### **Démarrer tous les services (après le premier démarrage)**

```powershell
docker-compose up -d
```

### **Arrêter tous les services**

```powershell
docker-compose down
```

⚠️ **Attention** : Cette commande arrête les conteneurs mais **conserve les données** (volumes persistants).

### **Arrêter et SUPPRIMER toutes les données**

```powershell
# ⚠️ DANGER : Supprime tous les volumes (perte de données)
docker-compose down -v
```

### **Redémarrer un service spécifique**

```powershell
docker-compose restart chu-airflow
```

### **Voir les logs d'un service**

```powershell
# Logs en temps réel
docker-compose logs -f chu-airflow

# Derniers 100 logs
docker logs --tail 100 chu-postgres
```

---

## 🌐 **Accès aux interfaces Web**

| Service | URL | Identifiants |
|---------|-----|--------------|
| **HDFS NameNode** | http://localhost:9870 | *(pas d'auth)* |
| **Spark Master** | http://localhost:8888 | *(pas d'auth)* |
| **Spark Worker 1** | http://localhost:8081 | *(pas d'auth)* |
| **Spark Worker 2** | http://localhost:8082 | *(pas d'auth)* |
| **HiveServer2 UI** | http://localhost:10002 | *(pas d'auth)* |
| **NiFi** | http://localhost:9090/nifi | `admin` / `chu_nifi_admin_2026` |
| **Airflow** | http://localhost:8085 | `admin` / `chu_airflow_2026` |

---

## 💻 **Commandes utiles**

### **Connexion PostgreSQL depuis l'hôte**

```powershell
# Via psql (si installé)
psql -h localhost -p 5433 -U postgres -d sih_data

# Via Docker exec
docker exec -it chu-postgres psql -U postgres -d sih_data
```

### **Connexion Beeline (Hive)**

```powershell
docker exec -it chu-hive-server beeline -u "jdbc:hive2://localhost:10000"
```

### **Exécuter des commandes HDFS**

```powershell
# Lister les fichiers HDFS
docker exec chu-namenode hdfs dfs -ls /

# Créer un répertoire HDFS
docker exec chu-namenode hdfs dfs -mkdir -p /data/bronze

# Uploader un fichier
docker exec chu-namenode hdfs dfs -put /opt/data/test.csv /data/bronze/
```

### **Soumettre un job Spark**

```powershell
docker exec chu-spark-master spark-submit `
  --master spark://chu-spark-master:7077 `
  --deploy-mode client `
  /path/to/script.py
```

### **Monitorer les ressources**

```powershell
# Ressources utilisées par les conteneurs
docker stats

# Espace disque des volumes
docker system df -v
```

---

## 🔧 **Résolution de problèmes**

### **Problème 1 : Port déjà utilisé**

**Erreur** : `Bind for 0.0.0.0:5433 failed: port is already allocated`

**Solution** :
1. Identifier le processus :
   ```powershell
   netstat -ano | findstr ":5433"
   ```
2. Modifier le port dans `docker-compose.yml` (ex: 5434)

---

### **Problème 2 : Conteneur ne démarre pas**

**Solution** :
```powershell
# Voir les logs détaillés
docker logs chu-namenode

# Redémarrer le conteneur
docker-compose restart chu-namenode
```

---

### **Problème 3 : HDFS NameNode en "Safe Mode"**

**Erreur** : `Name node is in safe mode`

**Solution** :
```powershell
docker exec chu-namenode hdfs dfsadmin -safemode leave
```

---

### **Problème 4 : Airflow ne se connecte pas à PostgreSQL**

**Solution** :
1. Vérifier que PostgreSQL est démarré :
   ```powershell
   docker logs chu-postgres
   ```
2. Réinitialiser la base Airflow :
   ```powershell
   docker exec chu-airflow airflow db reset
   docker exec chu-airflow airflow db migrate
   ```

---

### **Problème 5 : Manque de mémoire**

**Symptôme** : Conteneurs Spark crashent

**Solution** :
1. Augmenter la RAM allouée à Docker Desktop (Paramètres → Resources → Memory)
2. Réduire la mémoire des Workers dans `docker-compose.yml` :
   ```yaml
   SPARK_WORKER_MEMORY=1g  # Au lieu de 2g
   ```

---

## 📊 **Tests de validation**

### **Test 1 : HDFS est fonctionnel**

```powershell
docker exec chu-namenode hdfs dfs -ls /
# Expected: Liste vide ou répertoires existants
```

### **Test 2 : Spark peut se connecter à HDFS**

```powershell
docker exec chu-spark-master spark-shell --master spark://chu-spark-master:7077
# Dans le shell Spark :
# scala> spark.read.text("hdfs://chu-namenode:9000/").count
```

### **Test 3 : PostgreSQL a les 3 bases de données**

```powershell
docker exec chu-postgres psql -U postgres -c "\l"
# Expected: sih_data, airflow_db, metastore_db
```

### **Test 4 : Airflow est opérationnel**

1. Ouvrir http://localhost:8085
2. Se connecter avec `admin` / `chu_airflow_2026`
3. Vérifier qu'aucune erreur n'apparaît dans le dashboard

---

## 🔐 **Sécurité**

⚠️ **Mots de passe par défaut** : Les mots de passe sont définis dans `.env` et doivent être changés en production.

**Fichiers à protéger :**
- `.env` → Ne JAMAIS commiter sur Git (à ajouter dans `.gitignore`)
- `postgres/init/*.sql` → Peut contenir des credentials

---

## 📚 **Documentation complémentaire**

- **Architecture Médaillon** : Voir `Livrable1/groupe_6_Livrable_1_Projet_Big_Data.pdf`
- **Modèle dimensionnel** : Voir `Préparation/Livrable_1_Referentiel_Donnees_CHU.md`
- **Jobs ETL** : Voir section 5 du Livrable 1

---

## 👥 **Support**

- **Équipe** : Groupe 6 - BigData CESI
- **Date** : Juin 2026
- **Version** : Infrastructure v1.0

---

**🎯 Statut actuel : Infrastructure prête — Pipeline ETL Livrable 2 implémenté !**

---

## 🔄 **Livrable 2 — Pipeline ETL complet**

> **Objectif** : Modèle physique et optimisation — chargement, partitionnement, KPIs et évaluation des performances du Data Warehouse CHU.

---

### 🗺️ **Vue d'ensemble — Architecture Médaillon**

Le pipeline transforme les données brutes du SIH en données analytiques à travers **3 couches successives** stockées dans HDFS :

```
PostgreSQL (SIH)
      │
      ▼
  ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
  │  BRONZE  │────▶│  SILVER  │────▶│   GOLD   │────▶│   HIVE   │
  │  Données │     │  Données │     │  Données │     │  Tables  │
  │  brutes  │     │ nettoyées│     │analytique│     │  + KPIs  │
  └──────────┘     └──────────┘     └──────────┘     └──────────┘
  (Parquet raw)   (Parquet clean)  (Parquet star)   (Metastore SQL)
```

---

### 🗂️ **Structure des fichiers ETL**

```
airflow/dags/
├── dag_chu_etl_daily.py          ← DAG Airflow (orchestration)
└── jobs/
    ├── 00_setup_hdfs.py          ← Initialisation HDFS
    ├── 01_create_hive_schema.py  ← DDL Hive (schéma en étoile)
    ├── E1_extract_postgres.py    ← Extraction PostgreSQL → Bronze
    ├── T1_clean_bronze.py        ← Nettoyage Bronze → Silver
    ├── T2_deduplicate_silver.py  ← Déduplication Silver
    ├── T3_pseudonymize_rgpd.py   ← Pseudonymisation RGPD
    ├── T4_build_dimensions.py    ← Construction 7 dimensions Gold
    ├── T5_build_facts.py         ← Construction 4 tables de faits Gold
    └── T6_catalog_hive.py        ← Catalogage Hive + 7 KPIs
```

---

### 🔢 **Graphe de dépendances du pipeline**

```
start ──┬── 00_setup_hdfs ────────────┐
        └── 01_create_hive_schema ────┴── E1_extract_postgres
                                                  │
                                          T1_clean_bronze
                                                  │
                                      T2_deduplicate_silver
                                                  │
                                      T3_pseudonymize_rgpd
                                                  │
                                      T4_build_dimensions
                                                  │
                                         T5_build_facts
                                                  │
                                       T6_catalog_hive + KPIs
                                                  │
                                                 end
```

> `setup_hdfs` et `create_hive_schema` s'exécutent **en parallèle** car ils sont indépendants.
> Tous les jobs suivants sont **séquentiels** — chacun dépend du résultat du précédent.

---

### 📄 **Description détaillée de chaque script**

---

#### `00_setup_hdfs.py` — Initialisation de l'arborescence HDFS

**Conteneur :** `chu-namenode`
**Exécuté par :** `chu-spark-master` via `spark-submit`

Crée l'ensemble des répertoires HDFS avant toute autre opération. Équivalent d'un `mkdir -p` massif sur le système de fichiers distribué.

**Arborescence créée (35+ répertoires) :**

```
hdfs://chu-namenode:9000/data/
├── bronze/
│   ├── postgres/
│   │   ├── patient/           consultation/    prescription/
│   │   ├── diagnostic/        professionnel_sante/  specialites/
│   │   ├── medicaments/       mutuelle/        salle/
│   │   ├── laboratoire/       adher/
│   └── csv/
│       ├── etablissements_sante/   deces/
│       ├── hospitalisations/       satisfaction/
├── silver/
│   ├── patient_dedup/         consultation_dedup/   diagnostic_dedup/
│   ├── professionnel_sante_dedup/
│   ├── patient_rgpd/          consultation_rgpd/    professionnel_sante_rgpd/
└── gold/
    ├── dimensions/   (7 sous-dossiers : dim_temps, dim_patient, ...)
    ├── faits/        (4 sous-dossiers : fait_consultation, ...)
    └── kpis/
```

**Pourquoi c'est indispensable :** HDFS ne crée pas les répertoires intermédiaires automatiquement. Sans ce script, tous les jobs d'écriture échoueraient.

---

#### `01_create_hive_schema.py` — Création du schéma Hive DDL

**Conteneurs :** `chu-hive-server` + `chu-hive-metastore`
**Exécuté par :** `chu-spark-master`

Crée la base de données Hive `dwh_chu` avec le **schéma en étoile** (star schema) complet : 7 dimensions + 4 tables de faits + 3 vues analytiques.

**7 Dimensions (tables de référence) :**

| Table | SCD | Description |
|---|---|---|
| `dim_temps` | Type 1 | Calendrier 2010-2030 (7 306 jours) |
| `dim_patient` | **Type 2** | Patients pseudonymisés, avec historique |
| `dim_etablissement` | Type 1 | Établissements de santé (FINESS) |
| `dim_diagnostic` | Type 1 | Codes CIM-10 avec chapitres hiérarchiques |
| `dim_professionnel` | **Type 2** | Professionnels de santé (RPPS), avec historique |
| `dim_geographie` | Type 1 | Codes postaux → département → région |
| `dim_question` | Type 1 | 7 thèmes ESATIS (HAS) |

> **SCD Type 2** = on conserve l'**historique des changements** via les colonnes `date_debut_validite`, `date_fin_validite`, `est_courant`. Exemple : un médecin qui change de spécialité aura deux lignes dans `dim_professionnel`.

**4 Tables de faits (mesures) :**

| Table | Volumétrie estimée | Partitionnement |
|---|---|---|
| `fait_consultation` | ~5 millions/an | `annee` / `mois` |
| `fait_hospitalisation` | ~2 millions/an | `annee` / `mois` |
| `fait_deces` | ~300 000/an | `annee` |
| `fait_satisfaction` | ~150 000/an | Non partitionnée |

**3 Vues analytiques pré-calculées :**
- `v_consultations_completes` — jointure de toutes les dimensions
- `v_satisfaction_etablissement` — notes ESATIS par établissement
- `v_mortalite_region_pathologie` — croisement décès × CIM-10 × région

**Format :** Toutes les tables sont `EXTERNAL` Parquet + compression Snappy. Les données restent dans HDFS ; Hive ne stocke que les métadonnées (schéma, chemin, format).

---

#### `E1_extract_postgres.py` — Extraction PostgreSQL → Bronze

**Conteneurs :** `chu-postgres` (source) → `chu-namenode` / `chu-datanode` (destination HDFS)
**Exécuté par :** `chu-spark-master`
**Driver JDBC :** `postgresql-42.7.3.jar`

Extrait les 11 tables du SIH vers la couche Bronze HDFS. C'est le **seul point de contact direct avec la base de données source**.

**Connexion JDBC :**
```
jdbc:postgresql://chu-postgres:5432/sih_data
```

**Parallélisation pour les grandes tables :**
Spark ouvre plusieurs connexions simultanées sur des plages d'IDs différentes pour accélérer l'extraction :

```
PATIENT (600 000 lignes) → 4 connexions parallèles
  Connexion 1 : id_patient [1 → 150 000]
  Connexion 2 : id_patient [150 001 → 300 000]
  Connexion 3 : id_patient [300 001 → 450 000]
  Connexion 4 : id_patient [450 001 → 600 000]

CONSULTATION (6 000 000 lignes) → 8 connexions parallèles
```

**Colonnes de traçabilité ajoutées automatiquement :**

| Colonne | Exemple | Rôle |
|---|---|---|
| `_extraction_date` | `2026-06-01` | Audit de la date d'extraction |
| `_extraction_ts` | `2026-06-01 02:15:43` | Timestamp précis |
| `_source_system` | `CHU_SIH_POSTGRESQL` | Système d'origine |
| `_source_table` | `PATIENT` | Table d'origine |
| `_source_db` | `sih_data` | Base de données source |

**Validation :** Le nombre de lignes extrait est comparé à une estimation. Un écart > 20% déclenche une alerte dans les logs (peut signaler une suppression massive ou un problème réseau).

**Sortie HDFS :**
```
/data/bronze/postgres/patient/extraction_date=2026-06-01/part-00001.snappy.parquet
/data/bronze/postgres/consultation/extraction_date=2026-06-01/part-00001.snappy.parquet
```

---

#### `T1_clean_bronze.py` — Nettoyage Bronze → Silver

**Conteneurs :** `chu-namenode` / `chu-datanode` (lecture Bronze + écriture Silver)
**Exécuté par :** `chu-spark-master`

Applique des règles de nettoyage **spécifiques par table** pour obtenir des données fiables dans la couche Silver.

**Règles appliquées par table :**

*Table PATIENT :*
- Suppression des doublons exacts sur `id_patient`
- Dates de naissance invalides rejetées : avant 1900 ou dans le futur
- Sexe normalisé : toute valeur → `M`, `F`, ou `I` (inconnu)
- Code postal validé par regex : `[0-9]{5}`
- `nom` mis en MAJUSCULES, `prenom` en Initiales Majuscules
- Ajout de `age_calcule` = aujourd'hui − date_naissance
- Ajout de `tranche_age` : `< 18` / `18-39` / `40-64` / `65-79` / `80+`

*Table CONSULTATION :*
- Rejet si `id_patient` ou `id_professionnel` est NULL (données inutilisables)
- Durée de consultation : entre 0 et 480 minutes (8h max)
- Coût : entre 0 € et 2 000 €
- Ajout des colonnes `annee` et `mois` pour le partitionnement futur

*Table DIAGNOSTIC :*
- Validation format CIM-10 par regex : `[A-Z][0-9]{2}(\.[0-9]{1,2})?`
- Exemples valides : `J18.0`, `I10`, `C34.1`

*Table PROFESSIONNEL_DE_SANTE :*
- Numéro RPPS validé : exactement 11 chiffres
- Mode d'exercice normalisé : `LIBERAL`, `SALARIE`, ou `BENEVOLE`

**Sortie :** `/data/silver/{table}/` (Parquet Snappy, repartition en 2 fichiers)

---

#### `T2_deduplicate_silver.py` — Déduplication Silver

**Conteneurs :** `chu-namenode` / `chu-datanode`
**Exécuté par :** `chu-spark-master`
**Bibliothèque :** `jellyfish` (avec fallback pur Python)

Élimine les doublons de la couche Silver selon deux stratégies.

**Stratégie 1 — Déduplication exacte (toutes les tables sauf PATIENT) :**

Utilise une fonction de fenêtre (`Window + row_number()`). Pour chaque groupe de lignes identiques sur les colonnes clés, on conserve uniquement la ligne la plus récente :

```python
# Exemple pour CONSULTATION
# clé = (id_patient, id_professionnel, date_consultation)
Window.partitionBy("id_patient", "id_professionnel", "date_consultation")
      .orderBy(F.col("id_consultation").desc())
→ row_number() == 1  →  conserver
→ row_number() > 1   →  supprimer
```

**Stratégie 2 — Déduplication floue PATIENT (algorithme Jaro-Winkler) :**

Détecte les patients saisis plusieurs fois avec des variantes (fautes de frappe, abréviations) :
```
id=1 | Jean   | DUPONT | 1985-03-15 | M | 75001
id=2 | Jean   | DUPON  | 1985-03-15 | M | 75001
      → Même personne ! Score JW = 0.94 ≥ seuil 0.92 → doublon détecté
```

Algorithme en 2 passes :
1. **Passe exacte** : doublon strict sur `nom + prenom + date_naissance + sexe + code_postal`
2. **Passe floue** :
   - **Blocage** : regroupement par `sexe + 1ère lettre du nom + année de naissance` pour limiter les comparaisons (ne pas tout comparer avec tout)
   - Calcul du score : $\text{score} = 0.6 \times \text{JW}(\text{nom}) + 0.4 \times \text{JW}(\text{prenom})$
   - Si $\text{score} \geq 0.92$ → doublon → on supprime le patient avec le plus grand `id_patient` (le plus récent)

**Sortie :** `/data/silver/{table}_dedup/` (suffixe `_dedup` pour distinguer du Silver nettoyé)

---

#### `T3_pseudonymize_rgpd.py` — Pseudonymisation RGPD

**Conteneurs :** `chu-namenode` / `chu-datanode`
**Exécuté par :** `chu-spark-master`
**Conformité :** RGPD Art. 4(5) + recommandations CNIL pour les données de santé

Remplace les données directement identifiantes par des identifiants de substitution non réversibles.

**Algorithme SHA-256 avec sel :**

```
id_patient = 12345
sel secret = "CHU_RGPD_SALT_2026"   ← stocké hors HDFS (variable Airflow)

id_patient_hash = SHA-256("12345CHU_RGPD_SALT_2026")
               = "a3f2c9d1e8b47..."  (64 caractères hexadécimaux)
```

- **Déterministe** : même `id_patient` → même hash → les jointures entre tables restent cohérentes
- **Non réversible** : impossible de retrouver `12345` sans connaître le sel secret
- **Le sel ne doit jamais être stocké dans HDFS ou Hive**

**Données supprimées définitivement des couches Gold/Hive :**

| Champ original | Action | Remplacé par |
|---|---|---|
| `nom`, `prenom` | **Supprimé** | — |
| `date_naissance` exacte | **Supprimé** | `tranche_age` (< 18, 18-39, ...) |
| `adresse` complète | **Supprimé** | `code_postal` uniquement |
| `telephone`, `email` | **Supprimé** | — |
| `id_patient` brut | **Remplacé** | `id_patient_hash` (SHA-256) |
| `id_professionnel` brut | **Remplacé** | `id_professionnel_hash` (SHA-256) |

**Tables traitées :**
- `silver/patient_dedup` → `silver/patient_rgpd`
- `silver/professionnel_sante_dedup` → `silver/professionnel_sante_rgpd`
- `silver/consultation_dedup` → `silver/consultation_rgpd` (IDs remplacés par les hashes)

---

#### `T4_build_dimensions.py` — Construction des dimensions Gold

**Conteneurs :** `chu-namenode` / `chu-datanode`
**Exécuté par :** `chu-spark-master`

Construit les 7 tables de dimensions du schéma en étoile avec des **clés de substitution** (surrogate keys — entiers séquentiels générés par Spark) qui remplacent les identifiants métier dans les faits.

**`dim_temps`** — Entièrement calculée, aucune source Silver :
```
id_temps | date_complete | annee | mois | jour_semaine | nom_jour  | est_week_end | saison
       1 | 2010-01-01    |  2010 |    1 |            6 | Vendredi  |        False | Hiver
       2 | 2010-01-02    |  2010 |    1 |            7 | Samedi    |         True | Hiver
    7306 | 2029-12-31    |  2029 |   12 |            2 | Lundi     |        False | Hiver
```

**`dim_diagnostic`** — Codes CIM-10 enrichis avec la hiérarchie par chapitres :
```
sk | code_cim10 | libelle_diagnostic        | chapitre | libelle_chapitre
 1 | J18.0      | Pneumonie à Streptococcus | J        | Maladies de l'appareil respiratoire
 2 | I10        | Hypertension artérielle   | I        | Maladies de l'appareil circulatoire
 3 | C34.1      | Cancer du poumon          | C        | Tumeurs malignes
```

**`dim_geographie`** — Jointure broadcast avec le mapping statique département → région (13 régions métropolitaines + 5 DOM) :
```
sk | code_postal | departement | region
 1 | 75001       | 75          | Île-de-France
 2 | 13000       | 13          | Provence-Alpes-Côte d'Azur
```

**`dim_question`** — Table statique des 7 thèmes ESATIS (HAS) :
```
sk | code    | libelle_court
 1 | ACCOMP  | Accompagnement
 2 | CHAMBRE | Chambre et environnement
 7 | GLOBAL  | Satisfaction globale
```

---

#### `T5_build_facts.py` — Construction des tables de faits Gold

**Conteneurs :** `chu-namenode` / `chu-datanode`
**Exécuté par :** `chu-spark-master`

Construit les 4 tables de faits en joignant les données Silver avec les surrogate keys des dimensions.

**Stratégie Broadcast Join :**
Les dimensions (< 100 MB) sont envoyées entières à chaque nœud Spark pour éviter les shuffles réseau coûteux :

```
fait_consultation (5M lignes)
  JOIN BROADCAST dim_patient      (600K lignes)  → envoyée à chaque worker
  JOIN BROADCAST dim_professionnel (100K lignes) → envoyée à chaque worker
  JOIN BROADCAST dim_diagnostic   (50K lignes)   → envoyée à chaque worker
  JOIN BROADCAST dim_temps        (7306 lignes)  → très petite
  → Aucun shuffle réseau pour la table de faits volumineuse
```

**Structure de `fait_consultation` :**
```
sk_consultation | sk_temps | sk_patient | sk_professionnel | sk_diagnostic | duree_minutes | cout_euros | type_consultation | annee | mois
              1 |     5480 |      12043 |             4521 |           891 |            20 |      25.00 | CABINET           |  2024 |    3
```

**Partitionnement physique dans HDFS :**
```
/data/gold/faits/fait_consultation/
    annee=2022/mois=1/part-00001.snappy.parquet   ← seulement les données de Jan 2022
    annee=2022/mois=2/part-00001.snappy.parquet
    ...
    annee=2024/mois=12/part-00001.snappy.parquet
```

**Avantage :** Une requête `WHERE annee=2024 AND mois=3` ne lit **que les fichiers de mars 2024**, pas les 5 millions de lignes totales → gain de performance majeur.

---

#### `T6_catalog_hive.py` — Catalogage Hive + KPIs

**Conteneurs :** `chu-hive-server`, `chu-hive-metastore`, `chu-namenode`
**Exécuté par :** `chu-spark-master`

Finalise le pipeline en rendant les données Gold **accessibles via SQL standard** et en calculant les KPIs métier.

**Étape 1 — Enregistrement dans Hive Metastore :**
```sql
-- Découverte automatique des partitions pour les tables de faits
MSCK REPAIR TABLE dwh_chu.fait_consultation;
-- → Hive enregistre : annee=2022/mois=1, annee=2022/mois=2, etc.
```

**Étape 2 — Statistiques CBO (Cost-Based Optimizer) :**
```sql
ANALYZE TABLE dwh_chu.dim_patient COMPUTE STATISTICS;
ANALYZE TABLE dwh_chu.fait_consultation COMPUTE STATISTICS FOR COLUMNS sk_temps, sk_patient;
```
Permet à Spark SQL de choisir automatiquement la meilleure stratégie de jointure.

**Étape 3 — 7 KPIs métier calculés et stockés dans `gold/kpis/` :**

| KPI | Description | Chemin de sortie |
|---|---|---|
| KPI-1 | Volumétrie de chaque table Gold | `kpis/kpi_01_volumetrie` |
| KPI-2 | Consultations par mois avec durée et coût moyens | `kpis/kpi_02_consultations_mois` |
| KPI-3 | Durée moyenne + médiane par spécialité médicale | `kpis/kpi_03_duree_par_specialite` |
| KPI-4 | Top 20 des pathologies CIM-10 avec % du total | `kpis/kpi_04_top_pathologies` |
| KPI-5 | Évolution des décès par année | `kpis/kpi_05_deces_annee` |
| KPI-6 | Score de satisfaction moyen par établissement | `kpis/kpi_06_satisfaction_etablissement` |
| KPI-7 | Durée Moyenne de Séjour (DMS) hospitalière par mois | `kpis/kpi_07_duree_sejour` |

**Étape 4 — Validations d'accès (vérification des données) :**
Le script exécute 6 requêtes de contrôle et log leur résultat avec le temps de réponse :
- `SHOW TABLES IN dwh_chu` → 11 tables enregistrées
- `SELECT MIN/MAX(annee) FROM dim_temps` → plage 2010-2030
- `COUNT(*) FROM dim_patient WHERE est_courant = TRUE` → patients actifs
- Jointure `fait_consultation × dim_temps` → clés FK résolues
- Jointure `fait_consultation × dim_diagnostic` → intégrité référentielle

---

#### `dag_chu_etl_daily.py` — DAG Airflow d'orchestration

**Conteneurs :** `chu-airflow-webserver` + `chu-airflow-scheduler`
**Interface :** http://localhost:8085

Orchestre l'exécution quotidienne de tous les scripts ci-dessus avec gestion automatique des échecs.

**Planification :** `0 2 * * *` = tous les jours à **02h00 UTC** (hors heures d'activité hospitalière)
**Durée estimée :** ~2h30

**Politique de retry :**
```
Tâche échoue → attendre 5 min → retry #1
             → attendre 5 min → retry #2
             → attendre 5 min → retry #3
             → si toujours échec → email data-ops@chu.fr + DAG en FAILED
```

**Variable Airflow requise :**
Dans Airflow UI → Admin → Variables, créer :
- `CHU_RGPD_SALT` : le sel cryptographique secret pour SHA-256 (ne jamais le commiter sur Git)

---

### 🚀 **Lancement du pipeline**

#### Option 1 — Interface Airflow (recommandée)

```
1. Ouvrir http://localhost:8085
2. Se connecter : admin / chu_airflow_2026
3. Rechercher "dag_chu_etl_daily"
4. Cliquer sur le bouton ▶ "Trigger DAG"
5. Renseigner la date d'exécution si nécessaire
```

#### Option 2 — Ligne de commande

```powershell
# Déclencher le DAG complet
docker exec chu-airflow airflow dags trigger dag_chu_etl_daily --exec-date 2026-06-01
```

#### Option 3 — Lancer un job Spark individuellement (pour debug)

```powershell
# Exemple : relancer uniquement l'extraction PostgreSQL
docker exec chu-spark-master spark-submit `
  --master spark://chu-spark-master:7077 `
  --jars /opt/airflow/jars/postgresql-42.7.3.jar `
  /opt/airflow/dags/jobs/E1_extract_postgres.py --date 2026-06-01

# Exemple : relancer uniquement les KPIs
docker exec chu-spark-master spark-submit `
  --master spark://chu-spark-master:7077 `
  /opt/airflow/dags/jobs/T6_catalog_hive.py --date 2026-06-01
```

---

### 📊 **Surveillance pendant l'exécution**

| Interface | URL | Ce qu'on y voit |
|---|---|---|
| **Airflow UI** | http://localhost:8085 | Statut de chaque tâche, logs en temps réel, durées |
| **Spark UI** | http://localhost:4040 | Jobs en cours, stages, mémoire, shuffles (disponible pendant un job) |
| **HDFS UI** | http://localhost:9870 | Fichiers créés dans Bronze/Silver/Gold, espace disque |
| **Spark Master UI** | http://localhost:8888 | Workers actifs, applications soumises |

---

### 🔍 **Vérifier les données après exécution**

```powershell
# Vérifier la structure HDFS créée
docker exec chu-namenode hdfs dfs -ls -R /data/gold

# Vérifier les tables Hive enregistrées
docker exec chu-hive-server beeline -u "jdbc:hive2://localhost:10000" `
  -e "SHOW TABLES IN dwh_chu;"

# Compter les lignes d'une table de faits
docker exec chu-hive-server beeline -u "jdbc:hive2://localhost:10000" `
  -e "SELECT COUNT(*) FROM dwh_chu.fait_consultation;"

# Lancer une requête KPI directement
docker exec chu-hive-server beeline -u "jdbc:hive2://localhost:10000" -e "
  SELECT t.annee, t.mois, COUNT(*) AS nb_consultations
  FROM dwh_chu.fait_consultation c
  JOIN dwh_chu.dim_temps t ON c.sk_temps = t.id_temps
  GROUP BY t.annee, t.mois
  ORDER BY t.annee DESC, t.mois DESC
  LIMIT 12;"
```

---

### 📦 **Flux de données de bout en bout**

```
chu-postgres (SIH)                 HDFS (chu-namenode + chu-datanode)
    │                         Bronze            Silver              Gold
    │                            │                 │                  │
    ├─E1─▶ PATIENT      ──T1─▶ patient_clean ──T2─▶ patient_dedup    │
    │                                          ──T3─▶ patient_rgpd    │
    │                                                        ──T4─▶ dim_patient
    │                                                                  │
    ├─E1─▶ CONSULTATION ──T1─▶ consult_clean  ──T2─▶ consult_dedup   │
    │                                          ──T3─▶ consult_rgpd    │
    │                                                        ──T5─▶ fait_consultation
    │                                                                  │
    ├─E1─▶ DIAGNOSTIC   ──T1─▶ diag_clean     ──T2─▶ diag_dedup      │
    │                                                        ──T4─▶ dim_diagnostic
    │                                                                  │
    └─E1─▶ (8 autres tables)                                          │
                                                                       │
                                                        T6─▶ chu-hive-server
                                                              dwh_chu.*
                                                                       │
                                                              chu-superset
                                                              (dashboards KPIs)
```

---

**🎯 Statut actuel : Infrastructure prête — Pipeline ETL Livrable 2 implémenté !**
