"""Silver jobs: stream Bronze Delta tables and MERGE into Silver per micro-batch."""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from medallion.config import PipelineConfig
from medallion.logging_utils import get_logger
from medallion.schemas import CAST_MAP, RAW_SCHEMAS, REQUIRED_COLUMNS, SCD2_ENTITIES
from medallion.silver.scd2 import apply_scd2
from medallion.silver.transforms import (
    cast_columns,
    clean_strings,
    deduplicate,
    parse_payload,
    split_valid_invalid,
)
from medallion.streaming import await_queries, idempotent_append, with_trigger

log = get_logger(__name__)


def conform(bronze_batch: DataFrame, entity: str) -> tuple[DataFrame, DataFrame]:
    """Parse, clean, cast and validate a Bronze batch. Returns (valid, quarantine)."""
    parsed = parse_payload(bronze_batch, RAW_SCHEMAS[entity])
    typed = cast_columns(clean_strings(parsed), CAST_MAP[entity])
    return split_valid_invalid(typed, REQUIRED_COLUMNS[entity])


def _write_quarantine(invalid: DataFrame, cfg: PipelineConfig, entity: str, batch_id: int) -> None:
    if invalid.isEmpty():
        return
    rows = invalid.select(
        F.lit(entity).alias("entity"),
        "payload",
        "dq_reason",
        "kafka_partition",
        "kafka_offset",
        "quarantined_at",
    )
    idempotent_append(rows, cfg.silver_path("_quarantine"), f"silver_{entity}_quarantine", batch_id)


def upsert_orders(spark: SparkSession, valid: DataFrame, target_path: str, batch_id: int) -> None:
    """Orders are facts with mutable status: keep the latest event per order_id."""
    latest = deduplicate(valid, ["order_id"], ["kafka_timestamp"]).drop("payload")
    latest = latest.withColumn("dw_updated_at", F.current_timestamp())

    if not DeltaTable.isDeltaTable(spark, target_path):
        idempotent_append(latest, target_path, "silver_orders", batch_id)
        return

    (
        DeltaTable.forPath(spark, target_path)
        .alias("t")
        .merge(latest.alias("s"), "t.order_id = s.order_id")
        # Never let an older event overwrite a newer one (out-of-order delivery).
        .whenMatchedUpdateAll(
            condition="s.kafka_timestamp > t.kafka_timestamp "
            "OR (s.kafka_timestamp = t.kafka_timestamp AND s.kafka_offset > t.kafka_offset)"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def process_batch(
    spark: SparkSession, cfg: PipelineConfig, entity: str, batch: DataFrame, batch_id: int
) -> None:
    batch = batch.persist()
    try:
        if batch.isEmpty():
            return
        valid, invalid = conform(batch, entity)
        _write_quarantine(invalid, cfg, entity, batch_id)

        if entity in SCD2_ENTITIES:
            spec = SCD2_ENTITIES[entity]
            apply_scd2(
                spark,
                valid,
                cfg.silver_path(f"{entity}_scd2"),
                key=spec["key"],
                event_ts=spec["event_ts"],
                tracked=spec["tracked"],
                sk_name=f"{entity[:-1]}_sk",
                batch_id=batch_id,
            )
        else:
            upsert_orders(spark, valid, cfg.silver_path(entity), batch_id)
    finally:
        batch.unpersist()


def start_silver_stream(
    spark: SparkSession, cfg: PipelineConfig, entity: str, *, available_now: bool = True
) -> StreamingQuery:
    def _foreach(batch: DataFrame, batch_id: int) -> None:
        log.info("silver entity=%s batch_id=%s", entity, batch_id)
        process_batch(spark, cfg, entity, batch, batch_id)

    writer = (
        spark.readStream.format("delta")
        .load(cfg.bronze_path(entity))
        .writeStream.foreachBatch(_foreach)
        .option("checkpointLocation", cfg.checkpoint_path("silver", entity))
        .queryName(f"silver_{entity}")
    )
    writer = with_trigger(writer, available_now=available_now, interval="1 minute")
    return writer.start()


def run_silver(spark: SparkSession, cfg: PipelineConfig, *, available_now: bool = True) -> None:
    queries = [start_silver_stream(spark, cfg, e, available_now=available_now) for e in cfg.topics]
    await_queries(spark, queries)
