# ========================================
# DOCKERFILE HIVE PERSONNALISÉ
# Projet CHU - Ajout du driver PostgreSQL JDBC
# ========================================

FROM apache/hive:4.0.0

# Informations du build
LABEL maintainer="Equipe BigData CESI"
LABEL description="Apache Hive 4.0.0 avec driver PostgreSQL JDBC pour Metastore"
LABEL project="CHU Data Warehouse"

# Passer en root pour installer des packages
USER root

# Télécharger le driver PostgreSQL JDBC directement via ADD (Docker télécharge l'URL)
ADD --chown=hive:hive https://jdbc.postgresql.org/download/postgresql-42.7.3.jar /opt/hive/lib/postgresql-jdbc.jar

# Ajuster les permissions
RUN chmod 644 /opt/hive/lib/postgresql-jdbc.jar

# Créer le répertoire de configuration s'il n'existe pas
RUN mkdir -p /opt/hive/conf

# Vérifier que le driver est bien présent
RUN ls -lh /opt/hive/lib/postgresql-jdbc.jar

# Retour à l'utilisateur hive
USER hive

# Message de confirmation
RUN echo "✅ Driver PostgreSQL JDBC ajouté avec succès"

# Point d'entrée par défaut (hérite de l'image de base)
CMD ["/entrypoint.sh"]
