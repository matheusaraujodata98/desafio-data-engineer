"""Structured Streaming Bronze pipeline.
Lê os dados brutos do Kafka e salva de forma imutável em arquivos Parquet locais.
"""

from pathlib import Path

from pyspark.sql import SparkSession

KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
KAFKA_TOPIC = "topic_player_events"

# Usando o mesmo pacote blindado que garantiu a vitória na Silver
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
APP_NAME = "desafio-data-engineer-bronze-stream"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DATA_DIR = PROJECT_ROOT / "datalake" / "bronze" / "player_events"
BRONZE_CHECKPOINT_DIR = PROJECT_ROOT / "checkpoints" / "bronze" / "player_events"

def build_spark() -> SparkSession:
    """Cria a sessão do Spark com o conector do Kafka."""
    return (
        SparkSession.builder.appName(APP_NAME)
        .master("local[*]")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("⏳ Conectando ao Kafka e iniciando a gravação da Camada Bronze...")

    # 1. Leitura do Kafka (Pegando o JSON bruto e metadados)
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 50000) # Mantendo o limite para não engasgar
        .load()
    )

    # 2. Tombamento Imutável (Gravação em Parquet)
    bronze_query = (
        raw_stream.writeStream
        .format("parquet")
        .option("path", str(BRONZE_DATA_DIR))
        .option("checkpointLocation", str(BRONZE_CHECKPOINT_DIR))
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )
    
    print("🚀 Ingestão Bronze rodando em background!")
    print(f"Os arquivos Parquet estão sendo salvos na pasta: {BRONZE_DATA_DIR}")
    
    # Mantém o script rodando
    bronze_query.awaitTermination()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Ingestão Bronze interrompida pelo usuário com segurança.")