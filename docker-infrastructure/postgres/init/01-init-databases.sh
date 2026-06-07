#!/bin/bash
# =============================================================
# INIT PostgreSQL - Projet CHU
# Crée les bases airflow_db et metastore_db si elles n'existent pas
# IMPORTANT : script .sh pour éviter le problème "CREATE DATABASE
# ne peut pas s'exécuter dans un bloc de transaction" (.sql interdit)
# =============================================================
set -e

echo "=== INIT CHU : Création des bases de données ==="

# Créer airflow_db si elle n'existe pas
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
    SELECT 'CREATE DATABASE airflow_db OWNER $POSTGRES_USER ENCODING ''UTF8'''
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_db')\gexec
EOSQL
echo "[OK] airflow_db prête"

# Créer metastore_db si elle n'existe pas
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
    SELECT 'CREATE DATABASE metastore_db OWNER $POSTGRES_USER ENCODING ''UTF8'''
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metastore_db')\gexec
EOSQL
echo "[OK] metastore_db prête"

# Créer les schémas dans sih_data
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS sih_source;
    CREATE SCHEMA IF NOT EXISTS etl_metadata;

    CREATE TABLE IF NOT EXISTS etl_metadata.job_runs (
        id              SERIAL PRIMARY KEY,
        job_name        VARCHAR(100) NOT NULL,
        status          VARCHAR(20)  NOT NULL,
        started_at      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
        finished_at     TIMESTAMP,
        rows_processed  BIGINT,
        error_message   TEXT
    );
EOSQL
echo "[OK] Schémas sih_source + etl_metadata créés dans $POSTGRES_DB"

echo "=== INIT CHU : Terminé (sih_data + airflow_db + metastore_db) ==="
