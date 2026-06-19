from pyspark.sql import SparkSession

print("Iniciando motor do Spark...")
spark = SparkSession.builder \
    .appName("Upload-Silver-Neon") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 1. Lê a tabela Silver do Docker local
print("⏳ Lendo tabela Silver do banco local...")
df_silver = spark.read.jdbc(
    url="jdbc:postgresql://localhost:15432/maracana_gold",
    table="silver_audiencia_v3",
    properties={
        "user": "maracana_user",
        "password": "maracana_pass",
        "driver": "org.postgresql.Driver"
    }
)

# 2. Copia a tabela para a nuvem do Neon
print("🚀 Enviando tabela Silver para o Neon (Isso pode levar alguns segundos)...")
df_silver.write.jdbc(
    url="jdbc:postgresql://ep-sweet-water-ai7mshyv-pooler.c-4.us-east-1.aws.neon.tech:5432/neondb?sslmode=require",
    table="silver_audiencia_v3",
    mode="overwrite",
    properties={
        "user": "neondb_owner",
        "password": "npg_4KITysSlZz9e",
        "driver": "org.postgresql.Driver"
    }
)

print("✅ Sucesso! Tabela Silver copiada para a nuvem.")
spark.stop()