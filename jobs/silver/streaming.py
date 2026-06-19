"""Structured Streaming Silver pipeline for `topic_player_events`.

The job reads player events from Kafka, performs a Stream-Stream Left Join 
with SCTE-35 ad markers, applies watermarks, deduplicates events, 
and calculates QoE & CCV metrics per region/device/cdn/ad_marker.
"""

from __future__ import annotations

import argparse
import sys
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType
else:
    DataFrame = Any
    SparkSession = Any
    StructType = Any


KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
KAFKA_TOPIC_EVENTS = "topic_player_events"
KAFKA_TOPIC_SCTE = "topic_scte35_markers"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

WATERMARK_DELAY = "15 seconds"
WINDOW_DURATION = "1 minute"
APP_NAME = "desafio-data-engineer-silver-stream"


def build_spark(app_name: str = APP_NAME) -> SparkSession:
    """Create a local SparkSession with Kafka support enabled."""
    try:
        from pyspark.sql import SparkSession as PySparkSession
    except ModuleNotFoundError as exc:
        raise SystemExit("Dependência ausente: instale pyspark") from exc

    return (
        PySparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def player_event_schema() -> StructType:
    """Schema para a telemetria do player."""
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

    return StructType([
        StructField("event_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("content_id", StringType(), True), # NOVO: Para o Join
        StructField("timestamp", TimestampType(), True),
        StructField("event_type", StringType(), True),
        StructField("cdn", StringType(), True),
        StructField("geo", StructType([StructField("region", StringType(), True)]), True),
        StructField("device", StructType([StructField("type", StringType(), True)]), True),
        StructField("buffer_length_ms", IntegerType(), True),
        StructField("bitrate_kbps", IntegerType(), True),
        StructField("error_code", StringType(), True), 
        StructField("network_type", StringType(), True) 
    ])


def scte35_schema() -> StructType:
    """Schema para os marcadores de anúncio."""
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

    return StructType([
        StructField("marker_id", StringType(), True),
        StructField("channel", StringType(), True), # NOVO: Para o Join
        StructField("wallclock", TimestampType(), True),
        StructField("duration_s", IntegerType(), True),
        StructField("break_type", StringType(), True)
    ])


def read_player_events_stream(spark: SparkSession, bootstrap_servers: str) -> DataFrame:
    """Lê do Kafka e achata (flatten) as colunas aninhadas."""
    from pyspark.sql.functions import col, from_json, to_timestamp

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", KAFKA_TOPIC_EVENTS)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 50000)
        .load()
    )

    parsed = raw_stream.select(
        from_json(col("value").cast("string"), player_event_schema()).alias("event")
    )

    return (
        parsed.select(
            col("event.event_id"),
            col("event.session_id"),
            col("event.content_id"), # Lendo a coluna para o join
            to_timestamp(col("event.timestamp")).alias("timestamp"),
            col("event.event_type"),
            col("event.cdn"),
            col("event.geo.region").alias("region"),    
            col("event.device.type").alias("device"),   
            col("event.buffer_length_ms"),
            col("event.bitrate_kbps"),
            col("event.error_code"),
            col("event.network_type")
        )
        .where(col("event_id").isNotNull() & col("timestamp").isNotNull() & col("cdn").isNotNull())
    )


def read_scte35_stream(spark: SparkSession, bootstrap_servers: str) -> DataFrame:
    """Lê os marcadores de comercial SCTE-35 do Kafka."""
    from pyspark.sql.functions import col, from_json, to_timestamp

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", KAFKA_TOPIC_SCTE)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw_stream.select(
        from_json(col("value").cast("string"), scte35_schema()).alias("scte")
    )

    return parsed.select(
        col("scte.marker_id"),
        col("scte.channel"), # Lendo a coluna para o join
        to_timestamp(col("scte.wallclock")).alias("wallclock"),
        col("scte.duration_s"),
        col("scte.break_type")
    ).where(col("marker_id").isNotNull() & col("wallclock").isNotNull())


def build_silver_aggregation(events: DataFrame, scte: DataFrame) -> DataFrame:
    """Aplica o Stream-Stream Join e agrega as métricas avançadas do QoE."""
    from pyspark.sql.functions import avg, col, count, sum, when, window, approx_count_distinct, expr, coalesce, lit

    e_watermarked = events.withWatermark("timestamp", WATERMARK_DELAY).dropDuplicates(["event_id", "timestamp"])
    s_watermarked = scte.withWatermark("wallclock", WATERMARK_DELAY)

    # O JOIN CORRETO: Chave de igualdade + Janela Temporal
    joined_stream = e_watermarked.alias("e").join(
        s_watermarked.alias("s"),
        expr("""
            e.content_id = s.channel AND 
            e.timestamp >= s.wallclock AND 
            e.timestamp <= s.wallclock + INTERVAL 2 MINUTES
        """),
        "leftOuter"
    )

    enriched_stream = joined_stream.withColumn("active_ad_marker", coalesce(col("s.marker_id"), lit("conteudo_regular")))

    return (
        enriched_stream.groupBy(
            window(col("e.timestamp"), WINDOW_DURATION), 
            col("e.region"), 
            col("e.device"), 
            col("e.cdn"),
            col("active_ad_marker")
        )
        .agg(
            approx_count_distinct("e.session_id").alias("ccv"),
            avg("e.bitrate_kbps").alias("avg_bitrate_kbps"),
            avg("e.buffer_length_ms").alias("avg_buffer_length_ms"),
            (sum(when(col("e.error_code").isNotNull(), 1).otherwise(0)) / count("*")).alias("error_rate"),
            (sum(when(col("e.event_type") == "rebuffer", 1).otherwise(0)) / count("*")).alias("rebuffering_ratio")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("region"),
            col("device"),
            col("cdn"),
            col("active_ad_marker").alias("marker_id"),
            col("ccv"),
            col("avg_bitrate_kbps"),
            col("avg_buffer_length_ms"),
            col("error_rate"),
            col("rebuffering_ratio")
        )
    )


def start_postgres_sink(aggregated: DataFrame) -> None:
    """Salva no banco de dados na nova tabela V3 com a inteligência comercial."""
    
    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        from pyspark.sql.functions import col

        if batch_df.isEmpty():
            print(f"⚠️ Batch {batch_id}: nenhum registro recebido na Silver; nada será salvo.")
            return

        invalid_rows = batch_df.filter(
            col("window_start").isNull() | col("cdn").isNull()
        ).limit(1).collect()

        if invalid_rows:
            raise ValueError("Falha de QA: Dados corrompidos na Silver")

        rows = batch_df.collect()

        import psycopg2
        from psycopg2.extras import execute_values

        try:
            conn = psycopg2.connect(
                host="localhost",
                port="15432",
                dbname="maracana_gold",
                user="maracana_user",
                password="maracana_pass"
            )
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS silver_audiencia_v3 (
                    window_start TIMESTAMP,
                    window_end TIMESTAMP,
                    region VARCHAR(10),
                    device VARCHAR(20),
                    cdn VARCHAR(50),
                    marker_id VARCHAR(255),
                    ccv BIGINT,
                    avg_bitrate_kbps FLOAT,
                    avg_buffer_length_ms FLOAT,
                    error_rate FLOAT,
                    rebuffering_ratio FLOAT,
                    PRIMARY KEY (window_start, region, device, cdn, marker_id)
                );
            """)

            values = [(
                r.window_start, r.window_end, r.region, r.device, r.cdn, r.marker_id,
                r.ccv, r.avg_bitrate_kbps, r.avg_buffer_length_ms, 
                r.error_rate, r.rebuffering_ratio
            ) for r in rows]

            insert_query = """
                INSERT INTO silver_audiencia_v3 
                (window_start, window_end, region, device, cdn, marker_id, ccv, avg_bitrate_kbps, avg_buffer_length_ms, error_rate, rebuffering_ratio)
                VALUES %s
                ON CONFLICT (window_start, region, device, cdn, marker_id) 
                DO UPDATE SET 
                    ccv = EXCLUDED.ccv, 
                    avg_bitrate_kbps = EXCLUDED.avg_bitrate_kbps,
                    avg_buffer_length_ms = EXCLUDED.avg_buffer_length_ms,
                    error_rate = EXCLUDED.error_rate,
                    rebuffering_ratio = EXCLUDED.rebuffering_ratio;
            """
            
            execute_values(cursor, insert_query, values)
            conn.commit()
            
            cursor.close()
            conn.close()
            print(f"✅ Batch {batch_id}: {len(rows)} blocos analíticos atualizados no Postgres (V3).")
            
        except Exception as e:
            print(f"❌ Erro ao salvar o Batch {batch_id}: {e}")

    query = (
        aggregated.writeStream
        .outputMode("append") 
        .foreachBatch(process_batch)
        .trigger(processingTime="5 seconds")
        .start()
    )
    query.awaitTermination()


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Silver streaming pipeline V3")
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--output-mode", choices=("append", "update"), default="append")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        events = read_player_events_stream(spark, args.bootstrap_servers)
        scte = read_scte35_stream(spark, args.bootstrap_servers)
        
        silver = build_silver_aggregation(events, scte)
        
        print("⏳ Iniciando Stream-Stream Join e gravando no PostgreSQL (Tabela V3)...")
        start_postgres_sink(silver)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())