"""Bronze layer: land raw Kafka records in append-only Delta tables.

Nothing is parsed or filtered here. Bronze keeps the exact payload plus Kafka
lineage (topic / partition / offset) so any downstream table can be rebuilt
by replaying Bronze.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from medallion.config import PipelineConfig
from medallion.logging_utils import get_logger
from medallion.streaming import await_queries, with_trigger

log = get_logger(__name__)


def to_bronze(kafka_df: DataFrame) -> DataFrame:
    """Map the Kafka source schema to the Bronze record layout."""
    return kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("payload"),
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
        F.to_date(F.col("timestamp")).alias("ingest_date"),
    )


def start_bronze_stream(
    spark: SparkSession,
    cfg: PipelineConfig,
    entity: str,
    *,
    available_now: bool = True,
) -> StreamingQuery:
    topic = cfg.topics[entity]
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", cfg.kafka_starting_offsets)
        .option("failOnDataLoss", "false")
        .options(**cfg.kafka_options)
        .load()
    )

    writer = (
        to_bronze(kafka_df)
        .writeStream.format("delta")
        .outputMode("append")
        .partitionBy("ingest_date")
        .option("checkpointLocation", cfg.checkpoint_path("bronze", entity))
        .queryName(f"bronze_{entity}")
    )
    writer = with_trigger(writer, available_now=available_now, interval="30 seconds")

    log.info("Starting bronze stream topic=%s -> %s", topic, cfg.bronze_path(entity))
    return writer.start(cfg.bronze_path(entity))


def run_bronze(spark: SparkSession, cfg: PipelineConfig, *, available_now: bool = True) -> None:
    queries = [start_bronze_stream(spark, cfg, e, available_now=available_now) for e in cfg.topics]
    await_queries(spark, queries)
