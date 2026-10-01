"""Helpers shared by the streaming layers."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery


def with_trigger(writer: DataStreamWriter, *, available_now: bool, interval: str) -> DataStreamWriter:
    return writer.trigger(availableNow=True) if available_now else writer.trigger(processingTime=interval)


def await_queries(spark: SparkSession, queries: list[StreamingQuery]) -> None:
    """Block until every query finishes, re-raising the first failure.

    ``awaitAnyTermination`` returns (or raises) as soon as *any* query stops, so
    a failure in one stream is never hidden behind another that runs forever.
    """
    pending = list(queries)
    while pending:
        try:
            spark.streams.awaitAnyTermination()
        except Exception:
            for q in queries:
                if q.isActive:
                    q.stop()
            raise
        spark.streams.resetTerminated()
        pending = [q for q in pending if q.isActive]


def idempotent_append(df: DataFrame, path: str, app_id: str, batch_id: int) -> None:
    """Append from ``foreachBatch`` exactly once, even if the batch is replayed.

    Delta skips the write when (txnAppId, txnVersion) was already committed.
    """
    (
        df.write.format("delta")
        .mode("append")
        .option("txnAppId", app_id)
        .option("txnVersion", batch_id)
        .save(path)
    )
