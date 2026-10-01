"""Pure, stateless Silver transformations (parse -> clean -> cast -> validate -> dedup).

Each function takes and returns a DataFrame, so they are unit-testable
without Kafka or Delta.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType

LINEAGE_COLUMNS = ["kafka_partition", "kafka_offset", "kafka_timestamp"]


def parse_payload(bronze_df: DataFrame, schema: StructType) -> DataFrame:
    """Parse the JSON payload, keeping Kafka lineage and the raw payload for quarantine."""
    parsed = F.from_json(F.col("payload"), schema)
    return bronze_df.select(parsed.alias("data"), "payload", *LINEAGE_COLUMNS).select(
        "data.*", "payload", *LINEAGE_COLUMNS
    )


def clean_strings(df: DataFrame, lower_columns: tuple[str, ...] = ("email",)) -> DataFrame:
    """Trim every string column, turn blanks into NULL, lowercase selected columns."""
    out = df
    for field in df.schema.fields:
        if not isinstance(field.dataType, StringType) or field.name == "payload":
            continue
        col = F.trim(F.col(field.name))
        if field.name in lower_columns:
            col = F.lower(col)
        out = out.withColumn(field.name, F.when(col == "", None).otherwise(col))
    return out


def cast_columns(df: DataFrame, cast_map: dict[str, str]) -> DataFrame:
    """Apply explicit types. ``try_cast`` makes malformed values NULL instead of failing the batch."""
    out = df
    for name, dtype in cast_map.items():
        out = out.withColumn(name, F.expr(f"try_cast(`{name}` AS {dtype})"))
    return out


def split_valid_invalid(df: DataFrame, required: list[str]) -> tuple[DataFrame, DataFrame]:
    """Rows missing a required column after casting go to quarantine with a reason."""
    missing = [F.when(F.col(c).isNull(), F.lit(c)) for c in required]
    flagged = df.withColumn("_missing", F.concat_ws(",", *missing))
    valid = flagged.filter(F.col("_missing") == "").drop("_missing")
    invalid = (
        flagged.filter(F.col("_missing") != "")
        .withColumn("dq_reason", F.concat(F.lit("null_or_invalid:"), F.col("_missing")))
        .withColumn("quarantined_at", F.current_timestamp())
        .drop("_missing")
    )
    return valid, invalid


def deduplicate(df: DataFrame, keys: list[str], order_by: list[str]) -> DataFrame:
    """Keep one row per ``keys``: the latest by ``order_by`` (descending).

    Kafka offset is the final tie-breaker, so a producer retry that re-sends
    an identical event resolves deterministically.
    """
    order_cols = [F.col(c).desc_nulls_last() for c in order_by]
    if "kafka_offset" in df.columns and "kafka_offset" not in order_by:
        order_cols.append(F.col("kafka_offset").desc())
    w = Window.partitionBy(*keys).orderBy(*order_cols)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def add_row_hash(df: DataFrame, columns: list[str], name: str = "row_hash") -> DataFrame:
    """SHA-256 over tracked attributes; NULL-safe via a sentinel so NULL != ''."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in columns]
    return df.withColumn(name, F.sha2(F.concat_ws("||", *parts), 256))
