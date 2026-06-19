"""Batch Gold pipeline for Ad Reconciliation and Business Intelligence."""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, first, round, trim, max as spark_max, lit, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

APP_NAME = "desafio-data-engineer-gold-batch"
POSTGRES_PKG = "org.postgresql:postgresql:42.6.0" 
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"

DB_URL = "jdbc:postgresql://localhost:15432/maracana_gold"
DB_USER = "maracana_user"
DB_PASS = "maracana_pass"
DB_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASS,
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
    """Extrai os dados forçando os Schemas corretos para evitar falhas de tipagem."""
    
    silver_df = spark.read.jdbc(
        url=DB_URL,
        table="silver_audiencia_v3",
        properties=DB_PROPERTIES
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
        sum(col("price_cpm_brl") / 1000).alias("revenue_brl")
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
        col("s.ccv"),
        round("ae.revenue_brl", 2).alias("total_revenue_brl"),
        round("s.avg_bitrate_kbps", 2).alias("avg_bitrate_kbps"),
        round("s.error_rate", 4).alias("error_rate")
    )

    return gold_df


def run_data_quality_checks(df):
    if df.isEmpty():
        raise ValueError("Falha de QA: tabela Gold vazia")

    # Schema/completeness: a tabela final precisa manter as colunas estruturais e
    # patrocinador válido nas linhas realmente comerciais.
    schema_issues = df.filter(
        col("window_start").isNull()
        | col("marker_id").isNull()
        | col("region").isNull()
        | col("device").isNull()
        | col("cdn").isNull()
        | (
            col("total_revenue_brl").isNotNull()
            & col("advertiser_name").isNull()
        )
    ).limit(1).collect()

    if schema_issues:
        raise ValueError("Falha de QA: schema/completude inválidos na Gold")

    # Distribution: faturamento não pode ser negativo.
    negative_revenue = df.filter(
        col("total_revenue_brl").isNotNull() & (col("total_revenue_brl") < 0)
    ).limit(1).collect()

    if negative_revenue:
        raise ValueError("Falha de QA: faturamento negativo detectado na Gold")

    # Distribution: a audiência impactada não pode ser negativa.
    negative_audience = df.filter(col("ccv") < 0).limit(1).collect()

    if negative_audience:
        raise ValueError("Falha de QA: audiência negativa detectada na Gold")

    latest_window_start = df.agg(spark_max("window_start").alias("latest_window_start")).collect()[0]["latest_window_start"]
    latest_commercial_window = df.filter(col("total_revenue_brl").isNotNull()).agg(
        spark_max("window_start").alias("latest_commercial_window")
    ).collect()[0]["latest_commercial_window"]

    if latest_commercial_window is None:
        raise ValueError("Falha de QA: freshness/revenue zerado na Gold")

    latest_commercial_rows = df.filter(
        (col("window_start") == lit(latest_commercial_window))
        & (col("total_revenue_brl").isNotNull())
    )
    freshness_issue = latest_commercial_rows.filter(col("total_revenue_brl") <= 0).limit(1).collect()
    freshness_lag_seconds = df.select(
        (unix_timestamp(lit(latest_window_start)) - unix_timestamp(lit(latest_commercial_window))).alias("lag_seconds")
    ).collect()[0]["lag_seconds"]

    if freshness_issue or freshness_lag_seconds > 600:
        raise ValueError("Falha de QA: freshness/revenue zerado na Gold")

    return df

def load_postgres(gold_df):
    gold_df = run_data_quality_checks(gold_df)

    gold_df.write.jdbc(
        url=DB_URL,
        table="gold_faturamento_ads",
        mode="overwrite",
        properties=DB_PROPERTIES
    )

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("⏳ Extraindo dados (Silver + Raw)...")
        silver, ads, content = extract_data(spark)
        
        print("⚙️ Transformando dados e calculando faturamento (Camada Gold)...")
        gold = transform_gold(silver, ads, content)
        
        print("💾 Carregando tabela gold_faturamento_ads no PostgreSQL...")
        load_postgres(gold)
        
        print("✅ Sucesso! Reconciliação Gold finalizada.")
        
    except Exception as e:
        print(f"❌ Erro na pipeline Batch: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()