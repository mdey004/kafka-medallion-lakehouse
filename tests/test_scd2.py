from datetime import datetime

import pytest

from medallion.schemas import SCD2_ENTITIES
from medallion.silver.scd2 import apply_scd2

SPEC = SCD2_ENTITIES["customers"]
COLS = (
    "customer_id bigint, first_name string, last_name string, email string, "
    "city string, country string, segment string, updated_at timestamp"
)


def d(day: int) -> datetime:
    return datetime(2026, 1, day)


def cust(cid, city, day, segment="consumer"):
    return (cid, "Asha", "Patel", f"a{cid}@x.com", city, "UK", segment, d(day))


@pytest.fixture
def target(tmp_path):
    return str(tmp_path / "customers_scd2")


def merge(spark, target, rows):
    apply_scd2(
        spark,
        spark.createDataFrame(rows, COLS),
        target,
        key=SPEC["key"],
        event_ts=SPEC["event_ts"],
        tracked=SPEC["tracked"],
        sk_name="customer_sk",
    )


def history(spark, target, cid=1):
    rows = spark.read.format("delta").load(target).filter(f"customer_id = {cid}").orderBy("valid_from")
    return [(r.city, r.valid_from, r.valid_to, r.is_current) for r in rows.collect()]


def test_initial_load_creates_current_rows(spark, target):
    merge(spark, target, [cust(1, "London", 1), cust(2, "Paris", 1)])
    df = spark.read.format("delta").load(target)
    assert df.count() == 2
    assert df.filter("is_current AND valid_to IS NULL").count() == 2


def test_change_closes_old_version_and_opens_new(spark, target):
    merge(spark, target, [cust(1, "London", 1)])
    merge(spark, target, [cust(1, "Berlin", 5)])
    assert history(spark, target) == [
        ("London", d(1), d(5), False),
        ("Berlin", d(5), None, True),
    ]


def test_unchanged_record_is_noop(spark, target):
    merge(spark, target, [cust(1, "London", 1)])
    merge(spark, target, [cust(1, "London", 9)])
    assert history(spark, target) == [("London", d(1), None, True)]


def test_replaying_same_batch_is_idempotent(spark, target):
    batch = [cust(1, "London", 1), cust(1, "Berlin", 5)]
    merge(spark, target, batch)
    merge(spark, target, batch)
    assert len(history(spark, target)) == 2


def test_multiple_versions_in_one_batch_are_chained(spark, target):
    merge(spark, target, [cust(1, "London", 1)])
    merge(spark, target, [cust(1, "Berlin", 3), cust(1, "Berlin", 4), cust(1, "Paris", 7)])
    assert history(spark, target) == [
        ("London", d(1), d(3), False),
        ("Berlin", d(3), d(7), False),
        ("Paris", d(7), None, True),
    ]


def test_late_event_is_ignored(spark, target):
    merge(spark, target, [cust(1, "London", 10)])
    merge(spark, target, [cust(1, "Paris", 2)])
    assert history(spark, target) == [("London", d(10), None, True)]


def test_exactly_one_current_row_per_key(spark, target):
    merge(spark, target, [cust(1, "London", 1), cust(2, "Paris", 1)])
    merge(spark, target, [cust(1, "Berlin", 2), cust(2, "Paris", 2, segment="corporate"), cust(3, "Rome", 2)])
    df = spark.read.format("delta").load(target)
    counts = df.filter("is_current").groupBy("customer_id").count().collect()
    assert sorted(r.customer_id for r in counts) == [1, 2, 3]
    assert all(r["count"] == 1 for r in counts)
    assert df.select("customer_sk").distinct().count() == df.count()
