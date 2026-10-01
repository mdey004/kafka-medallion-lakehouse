from datetime import datetime

from medallion.bronze import to_bronze


def test_to_bronze_maps_kafka_envelope(spark):
    kafka_df = spark.createDataFrame(
        [(b"1", b'{"customer_id": "1"}', "customers", 0, 42, datetime(2026, 1, 2, 3, 4, 5))],
        "key binary, value binary, topic string, partition int, offset long, timestamp timestamp",
    )

    row = to_bronze(kafka_df).collect()[0]

    assert row.kafka_key == "1"
    assert row.payload == '{"customer_id": "1"}'
    assert (row.topic, row.kafka_partition, row.kafka_offset) == ("customers", 0, 42)
    assert str(row.ingest_date) == "2026-01-02"
    assert row.ingested_at is not None
