"""Type 2 Slowly Changing Dimension merge for Delta Lake.

Target table layout::

    <entity>_sk | <key> | <tracked...> | row_hash | valid_from | valid_to | is_current | dw_updated_at

Rules applied per micro-batch:

* Several versions of one key in a batch are all kept, in event-time order.
* A version whose tracked attributes match the version before it is a no-op.
* A version at or before the key's current ``valid_from`` is late and is ignored.
  That makes replaying a batch idempotent.
* The current row is closed with ``valid_to = <first new version's valid_from>``.

The merge uses the standard "staged updates" pattern. Rows with a non-NULL
``_merge_key`` close the existing current row. Rows with a NULL ``_merge_key``
never match and are inserted as new versions.
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from medallion.silver.transforms import add_row_hash, deduplicate


def _empty_current(spark: SparkSession, updates: DataFrame, key: str) -> DataFrame:
    schema = StructType(
        [
            StructField(key, updates.schema[key].dataType, True),
            StructField("row_hash", StringType(), True),
            StructField("valid_from", TimestampType(), True),
        ]
    )
    return spark.createDataFrame([], schema)


def build_scd2_staging(
    updates: DataFrame,
    current: DataFrame,
    *,
    key: str,
    event_ts: str,
    tracked: list[str],
    sk_name: str,
) -> DataFrame:
    """Return the staged rows to MERGE: closers (``_merge_key`` set) and inserts (``_merge_key`` NULL)."""
    incoming = deduplicate(add_row_hash(updates, tracked), [key, event_ts], [event_ts]).select(
        key,
        *tracked,
        "row_hash",
        F.col(event_ts).alias("valid_from"),
        F.lit(False).alias("_is_existing"),
    )
    existing = current.select(key, "row_hash", "valid_from", F.lit(True).alias("_is_existing"))
    combined = incoming.unionByName(existing, allowMissingColumns=True)

    by_key = Window.partitionBy(key)
    by_key_time = by_key.orderBy(F.col("valid_from"), F.col("_is_existing").desc())

    # 1) Drop late or replayed events (at or before the current version).
    combined = combined.withColumn(
        "_current_from", F.max(F.when(F.col("_is_existing"), F.col("valid_from"))).over(by_key)
    ).filter(
        F.col("_is_existing")
        | F.col("_current_from").isNull()
        | (F.col("valid_from") > F.col("_current_from"))
    )

    # 2) Drop versions identical to their predecessor (no attribute change).
    combined = combined.withColumn("_prev_hash", F.lag("row_hash").over(by_key_time))
    new_versions = combined.filter(
        ~F.col("_is_existing") & (F.col("_prev_hash").isNull() | (F.col("row_hash") != F.col("_prev_hash")))
    )

    # 3) Chain the surviving versions: valid_to = next version's valid_from.
    chain = Window.partitionBy(key).orderBy("valid_from")
    new_versions = (
        new_versions.withColumn("valid_to", F.lead("valid_from").over(chain))
        .withColumn("is_current", F.col("valid_to").isNull())
        .withColumn("_version_rank", F.row_number().over(chain))
        .withColumn(sk_name, F.xxhash64(F.col(key), F.col("valid_from")))
    )

    inserts = new_versions.withColumn("_merge_key", F.lit(None).cast(updates.schema[key].dataType))
    closers = new_versions.filter(
        (F.col("_version_rank") == 1) & F.col("_current_from").isNotNull()
    ).withColumn("_merge_key", F.col(key))

    cols = [sk_name, "_merge_key", key, *tracked, "row_hash", "valid_from", "valid_to", "is_current"]
    return inserts.select(*cols).unionByName(closers.select(*cols))


def apply_scd2(
    spark: SparkSession,
    updates: DataFrame,
    target_path: str,
    *,
    key: str,
    event_ts: str,
    tracked: list[str],
    sk_name: str,
    batch_id: int | None = None,
) -> None:
    """MERGE a batch of validated, typed records into an SCD2 Delta table."""
    if DeltaTable.isDeltaTable(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)
        current = target.toDF().filter("is_current").select(key, "row_hash", "valid_from")
    else:
        target = None
        current = _empty_current(spark, updates, key)

    staged = build_scd2_staging(
        updates, current, key=key, event_ts=event_ts, tracked=tracked, sk_name=sk_name
    )

    if target is None:
        initial = (
            staged.filter("_merge_key IS NULL")
            .drop("_merge_key")
            .withColumn("dw_updated_at", F.current_timestamp())
        )
        writer = initial.write.format("delta").mode("append")
        if batch_id is not None:  # replay-safe when called from foreachBatch
            writer = writer.option("txnAppId", f"scd2:{target_path}").option("txnVersion", batch_id)
        writer.save(target_path)
        return

    insert_cols = [sk_name, key, *tracked, "row_hash", "valid_from", "valid_to", "is_current"]
    insert_values = {c: f"s.`{c}`" for c in insert_cols}
    insert_values["dw_updated_at"] = "current_timestamp()"

    (
        target.alias("t")
        .merge(staged.alias("s"), f"t.`{key}` = s._merge_key AND t.is_current = true")
        .whenMatchedUpdate(
            set={
                "valid_to": "s.valid_from",
                "is_current": "false",
                "dw_updated_at": "current_timestamp()",
            }
        )
        .whenNotMatchedInsert(condition="s._merge_key IS NULL", values=insert_values)
        .execute()
    )
