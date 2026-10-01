import json
from datetime import datetime
from decimal import Decimal

from pyspark.sql import functions as F

from medallion.silver.jobs import conform
from medallion.silver.transforms import add_row_hash, deduplicate

TS = datetime(2026, 1, 1)


def bronze(spark, payloads):
    rows = [(json.dumps(p), 0, i, TS) for i, p in enumerate(payloads)]
    return spark.createDataFrame(
        rows, "payload string, kafka_partition int, kafka_offset long, kafka_timestamp timestamp"
    )


def test_conform_cleans_and_casts(spark):
    df = bronze(
        spark,
        [
            {
                "product_id": " 7 ",
                "product_name": "  Lamp ",
                "category": "home",
                "unit_price": "19.999",
                "status": "",
                "updated_at": "2026-02-01T10:00:00Z",
            }
        ],
    )

    valid, invalid = conform(df, "products")
    valid = valid.withColumn("updated_at_utc", F.date_format("updated_at", "yyyy-MM-dd HH:mm:ss"))
    row = valid.collect()[0]

    assert invalid.count() == 0
    assert row.product_id == 7
    assert row.product_name == "Lamp"
    assert row.unit_price == Decimal("20.00")
    assert row.status is None  # blank -> NULL
    assert row.updated_at_utc == "2026-02-01 10:00:00"  # ISO-8601 Z parsed as UTC


def test_conform_lowercases_email(spark):
    df = bronze(spark, [{"customer_id": "1", "email": " A.B@Example.COM ", "updated_at": "2026-01-01"}])
    valid, _ = conform(df, "customers")
    assert valid.collect()[0].email == "a.b@example.com"


def test_conform_quarantines_bad_values(spark):
    df = bronze(
        spark,
        [
            {"product_id": "1", "unit_price": "N/A", "updated_at": "2026-01-01"},
            {"product_id": "abc", "unit_price": "5", "updated_at": "2026-01-01"},
            {"product_id": "2", "unit_price": "5", "updated_at": "not-a-date"},
        ],
    )

    valid, invalid = conform(df, "products")
    reasons = sorted(r.dq_reason for r in invalid.collect())

    assert valid.count() == 0
    assert reasons == [
        "null_or_invalid:product_id",
        "null_or_invalid:unit_price",
        "null_or_invalid:updated_at",
    ]
    assert all(r.payload for r in invalid.collect())  # raw payload kept for triage


def test_deduplicate_keeps_latest_and_breaks_ties_by_offset(spark):
    df = spark.createDataFrame(
        [("A", 1, "old", 10), ("A", 2, "new", 11), ("A", 2, "newest-offset", 12), ("B", 1, "only", 13)],
        "k string, ts int, v string, kafka_offset long",
    )
    result = {r.k: r.v for r in deduplicate(df, ["k"], ["ts"]).collect()}
    assert result == {"A": "newest-offset", "B": "only"}


def test_row_hash_distinguishes_null_from_empty(spark):
    df = spark.createDataFrame([(None,), ("",)], "c string")
    hashes = [r.row_hash for r in add_row_hash(df, ["c"]).collect()]
    assert hashes[0] != hashes[1]
