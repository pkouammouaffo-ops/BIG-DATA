"""
Job E2 : Ingestion fichiers CSV vers HDFS Bronze
Lit les CSV (avec leur séparateur propre), conserve tout en String (inferSchema=False), ajoute des colonnes de traçabilité, et écrit en Parquet sur HDFS (couche Bronze).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, current_date

# Chemins (dans le conteneur : ./data monté sur /opt/data)
CSV_DIR = "file:///opt/data/source/csv"
HDFS_BRONZE = "hdfs://namenode:9000/data/bronze/csv"

# Chaque fichier avec son séparateur propre
CSV_FILES = {
    "deces": {"file": "deces.csv", "sep": ","},
    "etablissement_sante": {"file": "etablissement_sante.csv", "sep": ";"},
    "hospitalisations": {"file": "Hospitalisations.csv", "sep": ";"},
    "activite_professionnel_sante": {"file": "activite_professionnel_sante.csv", "sep": ";"}, 
    "professionnel_sante": {"file": "professionnel_sante.csv", "sep": ";"}
}

def main():
    # Initialiser SparkSession
    spark = SparkSession.builder.appName("E2_Extract_CSV").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")


    for name, cfg in CSV_FILES.items():
        print(f"=== Ingestion du fichier : {cfg['file']} ===")

        df = (
            spark.read
            .option("header", "true")
            .option("sep", cfg["sep"])
            .option("encoding", "UTF-8")
            .option("inferSchema", "false")   # Bronze : tout en String
            .option("multiLine", "true")      # gère les champs avec retours ligne
            .option("quote", '"')
            .csv(f"{CSV_DIR}/{cfg['file']}")
        )

        # Colonnes de traçabilité (Bronze)
        df = (
            df.withColumn("_ingestion_ts", current_timestamp())
              .withColumn("_source_system", lit("csv_file"))
              .withColumn("_ingestion_date", current_date())
        )

        n = df.count()
        ncols = len(df.columns)
        print(f"    {n} lignes, {ncols} colonnes")

        out_path = f"{HDFS_BRONZE}/{name}"
        df.write.mode("overwrite").parquet(out_path)
        print(f"    Écrit dans : {out_path}")

    print("=== Job E2 terminé ===")
    spark.stop()

if __name__ == "__main__":
    main()