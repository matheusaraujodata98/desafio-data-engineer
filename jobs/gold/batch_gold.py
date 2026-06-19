"""Batch Gold pipeline for Ad Reconciliation and Business Intelligence."""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, first, round, trim, max as spark_max, lit, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

APP_NAME = "desafio-data-engineer-gold-batch"
POSTGRES_PKG = "org.postgresql:postgresql:42.7.3"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"

# --- 1. CONFIGURAÇÃO DE LEITURA (LOCAL DOCKER) ---
# Onde a sua Camada Silver foi salva pelo Streaming
LOCAL_DB_URL = "jdbc:postgresql://localhost:15432/maracana_gold"
LOCAL_DB_PROPERTIES = {
    "user": "maracana_user",
    "password": "maracana_pass",
    "driver": "org.postgresql.Driver"
}

# --- 2. CONFIGURAÇÃO DE ESCRITA (NEON NUVEM) ---
NEON_HOST = "ep-sweet-water-ai7mshyv-pooler.c-4.us-east-1.aws.neon.tech"
NEON_DB = "neondb"
NEON_USER = "neondb_owner"
NEON_PASSWORD = "npg_4KITysSlZz9e"

NEON_DB_URL = f"jdbc:postgresql://{NEON_HOST}:5432/{NEON_DB}?sslmode=require"
NEON_DB_PROPERTIES = {
    "user": NEON_USER,
    "password": NEON_PASSWORD,
    "driver": "org.postgresql.Driver"
}

def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName(APP_NAME)
        .master("local[*]")
        .config("spark.jars.packages", POSTGRES_PKG)
        .getOrCreate()
    )

def extract_data(spark: SparkSession):
    """Extrai os dados da Silver (Local) e arquivos Json (Raw)."""
    
    silver_df = spark.read.jdbc(
        url=LOCAL_DB_URL,
        table="silver_audiencia_v3",
        properties=LOCAL_DB_PROPERTIES
    ).where(col("marker_id") != "conteudo_regular")

    ads_schema = StructType([
        StructField("marker_id", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("advertiser_name", StringType(), True),
        StructField("price_cpm_brl", DoubleType(), True)
    ])

    content_schema = StructType([
        StructField("content_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("genre", StringType(), True)
    ])

    ads_df = spark.read.schema(ads_schema).json(str(RAW_DATA_DIR / "ad_decisions.jsonl"))
    content_df = spark.read.schema(content_schema).json(str(RAW_DATA_DIR / "content_metadata.jsonl"))
    
    return silver_df, ads_df, content_df


def transform_gold(silver_df, ads_df, content_df):
    """Executa a reconciliação limpando espaços ocultos (trim)."""
    
    ads_agg = ads_df.groupBy("marker_id", "channel").agg(
        first("advertiser_name").alias("advertiser_name"),
        spark_sum(col("price_cpm_brl") / 1000).alias("revenue_brl")
    )

    # Join 1: Anúncios + Metadados do Conteúdo (com trim)
    ads_enriched = ads_agg.alias("a").join(
        content_df.alias("c"),
        trim(col("a.channel")) == trim(col("c.content_id")),
        "left"
    ).select(
        col("a.marker_id"),
        col("a.advertiser_name"),
        col("a.revenue_brl"),
        col("c.title").alias("content_title"),
        col("c.genre")
    )

    # Join 2: Audiência Silver + Inteligência Comercial (com trim)
    gold_df = silver_df.alias("s").join(
        ads_enriched.alias("ae"),
        trim(col("s.marker_id")) == trim(col("ae.marker_id")),
        "left"
    ).select(
        col("s.window_start"),
        col("s.marker_id"),
        col("ae.content_title"),
        col("ae.genre"),
        col("ae.advertiser_name"),
        col("s.region"),
        col("s.device"),
        col("s.cdn"), # <- CORREÇÃO: Adicionado para o QA não quebrar
        col("s.ccv"),
        round("ae.revenue_brl", 2).alias("total_revenue_brl"),
        round("s.avg_bitrate_kbps", 2).alias("avg_bitrate_kbps"),
        round("s.error_rate", 4).alias("error_rate")
    )

    return gold_df


def run_data_quality_checks(df):
    print("🔍 Executando Data Quality Checks na Gold...")
    
    if df.isEmpty():
        raise ValueError("Falha de QA: tabela Gold vazia")

    # Check 1: Schema/completeness
    schema_issues = df.filter(
        col("window_start").isNull()
        | col("marker_id").isNull()
        | col("region").isNull()
        | col("device").isNull()
        | col("cdn").isNull()
        | (col("total_revenue_brl").isNotNull() & col("advertiser_name").isNull())
    ).limit(1).collect()

    if schema_issues:
        raise ValueError("Falha de QA: schema/completude inválidos na Gold")

    # Check 2: Distribution (Faturamento)
    negative_revenue = df.filter(
        col("total_revenue_brl").isNotNull() & (col("total_revenue_brl") < 0)
    ).limit(1).collect()

    if negative_revenue:
        raise ValueError("Falha de QA: faturamento negativo detectado na Gold")

    # Check 3: Distribution (Audiência)
    negative_audience = df.filter(col("ccv") < 0).limit(1).collect()

    if negative_audience:
        raise ValueError("Falha de QA: audiência negativa detectada na Gold")

    # Check 4: Freshness
    latest_window_start = df.agg(spark_max("window_start").alias("max_val")).collect()[0]["max_val"]
    latest_comm_row = df.filter(col("total_revenue_brl").isNotNull()).agg(
        spark_max("window_start").alias("max_val")
    ).collect()[0]["max_val"]

    if latest_comm_row is None:
        print("⚠️ Aviso de QA: Nenhum dado de faturamento comercial encontrado nesta rodada.")
    else:
        latest_commercial_rows = df.filter(
            (col("window_start") == lit(latest_comm_row)) & (col("total_revenue_brl").isNotNull())
        )
        freshness_issue = latest_commercial_rows.filter(col("total_revenue_brl") <= 0).limit(1).collect()
        
        if latest_window_start and latest_comm_row:
            freshness_lag_seconds = df.select(
                (unix_timestamp(lit(latest_window_start)) - unix_timestamp(lit(latest_comm_row))).alias("lag")
            ).collect()[0]["lag"]

            if freshness_issue or (freshness_lag_seconds and freshness_lag_seconds > 600):
                raise ValueError("Falha de QA: freshness/revenue defasado na Gold")

    print("✅ Todos os 4 checks de QA passaram com sucesso!")
    return df

def load_postgres(gold_df):
    gold_df = run_data_quality_checks(gold_df)

    # Escreve na nuvem (Neon)
    gold_df.write.jdbc(
        url=NEON_DB_URL,
        table="gold_faturamento_ads",
        mode="overwrite",
        properties=NEON_DB_PROPERTIES
    )

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("⏳ Extraindo dados da Silver (Local) e JSONL...")
        silver, ads, content = extract_data(spark)
        
        print("⚙️ Transformando dados e calculando faturamento (Camada Gold)...")
        gold = transform_gold(silver, ads, content)
        
        print("💾 Carregando tabela 'gold_faturamento_ads' no PostgreSQL (Neon)...")
        load_postgres(gold)
        
        print("🎉 Sucesso! Reconciliação Gold finalizada e enviada para a nuvem.")
        
    except Exception as e:
        print(f"❌ Erro na pipeline Batch: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()