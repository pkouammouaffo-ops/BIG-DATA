"""
Génère le DDL HiveQL à partir des schémas Parquet réels dans HDFS Gold.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("gen_hive_ddl").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

NAMENODE = "hdfs://chu-namenode:9000"

GOLD_TABLES = {
    # Dimensions
    "dim_temps":          f"{NAMENODE}/data/gold/dimensions/dim_temps",
    "dim_patient":        f"{NAMENODE}/data/gold/dimensions/dim_patient",
    "dim_etablissement":  f"{NAMENODE}/data/gold/dimensions/dim_etablissement",
    "dim_diagnostic":     f"{NAMENODE}/data/gold/dimensions/dim_diagnostic",
    "dim_professionnel":  f"{NAMENODE}/data/gold/dimensions/dim_professionnel",
    "dim_geographie":     f"{NAMENODE}/data/gold/dimensions/dim_geographie",
    "dim_question":       f"{NAMENODE}/data/gold/dimensions/dim_question",
    # Faits partitionnés
    "fait_consultation":    (f"{NAMENODE}/data/gold/faits/fait_consultation",    ["annee", "mois"]),
    "fait_hospitalisation": (f"{NAMENODE}/data/gold/faits/fait_hospitalisation", ["annee", "mois"]),
    "fait_deces":           (f"{NAMENODE}/data/gold/faits/fait_deces",           []),
    "fait_satisfaction":    (f"{NAMENODE}/data/gold/faits/fait_satisfaction",    []),
}

TYPE_MAP = {
    "LongType":      "BIGINT",
    "IntegerType":   "INT",
    "StringType":    "STRING",
    "DoubleType":    "DOUBLE",
    "FloatType":     "FLOAT",
    "BooleanType":   "BOOLEAN",
    "DateType":      "DATE",
    "TimestampType": "TIMESTAMP",
    "ShortType":     "SMALLINT",
    "ByteType":      "TINYINT",
    "BinaryType":    "BINARY",
}

def spark_type_to_hive(t):
    type_name = type(t).__name__
    return TYPE_MAP.get(type_name, "STRING")

print("-- ============================================================")
print("-- CHU DWH - DDL Hive généré automatiquement depuis Parquet Gold")
print("-- ============================================================")
print()
print("CREATE DATABASE IF NOT EXISTS dwh_chu;")
print("USE dwh_chu;")
print()

for table_name, config in GOLD_TABLES.items():
    if isinstance(config, tuple):
        path, partitions = config
    else:
        path = config
        partitions = []

    try:
        df = spark.read.parquet(path)
        schema = df.schema
    except Exception as e:
        print(f"-- SKIP {table_name}: {e}")
        continue

    # Colonnes non-partition
    non_part_cols = [f for f in schema.fields if f.name not in partitions]
    part_cols = [f for f in schema.fields if f.name in partitions]

    print(f"DROP TABLE IF EXISTS dwh_chu.{table_name};")
    print(f"CREATE EXTERNAL TABLE dwh_chu.{table_name} (")
    lines = []
    for f in non_part_cols:
        hive_type = spark_type_to_hive(f.dataType)
        lines.append(f"    {f.name} {hive_type}")
    print(",\n".join(lines))
    print(")")

    if part_cols:
        print("PARTITIONED BY (")
        plines = []
        for f in part_cols:
            hive_type = spark_type_to_hive(f.dataType)
            plines.append(f"    {f.name} {hive_type}")
        print(",\n".join(plines))
        print(")")

    print("STORED AS PARQUET")
    print(f"LOCATION '{path}'")
    print("TBLPROPERTIES ('parquet.compression'='SNAPPY');")
    print()
    print(f"-- {table_name}: {df.count()} lignes")
    print()

spark.stop()
