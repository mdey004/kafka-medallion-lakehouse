"""Event contracts.

Payloads are parsed with an all-string schema first and cast explicitly in
Silver. That keeps Bronze -> Silver lossless: a bad value becomes NULL after
the cast and is routed to quarantine instead of silently disappearing during
JSON parsing.
"""

from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType


def _all_strings(*names: str) -> StructType:
    return StructType([StructField(n, StringType(), True) for n in names])


RAW_SCHEMAS: dict[str, StructType] = {
    "customers": _all_strings(
        "customer_id", "first_name", "last_name", "email", "city", "country", "segment", "updated_at"
    ),
    "products": _all_strings("product_id", "product_name", "category", "unit_price", "status", "updated_at"),
    "orders": _all_strings(
        "order_id", "customer_id", "product_id", "quantity", "unit_price", "order_status", "order_ts"
    ),
}

# Target Spark SQL types applied in Silver (column -> type).
CAST_MAP: dict[str, dict[str, str]] = {
    "customers": {
        "customer_id": "bigint",
        "first_name": "string",
        "last_name": "string",
        "email": "string",
        "city": "string",
        "country": "string",
        "segment": "string",
        "updated_at": "timestamp",
    },
    "products": {
        "product_id": "bigint",
        "product_name": "string",
        "category": "string",
        "unit_price": "decimal(12,2)",
        "status": "string",
        "updated_at": "timestamp",
    },
    "orders": {
        "order_id": "string",
        "customer_id": "bigint",
        "product_id": "bigint",
        "quantity": "int",
        "unit_price": "decimal(12,2)",
        "order_status": "string",
        "order_ts": "timestamp",
    },
}

# Columns that must be non-null after casting, otherwise the row is quarantined.
REQUIRED_COLUMNS: dict[str, list[str]] = {
    "customers": ["customer_id", "updated_at"],
    "products": ["product_id", "unit_price", "updated_at"],
    "orders": ["order_id", "customer_id", "product_id", "quantity", "unit_price", "order_ts"],
}

# SCD2 configuration: business key, event-time column, tracked attributes.
SCD2_ENTITIES: dict[str, dict] = {
    "customers": {
        "key": "customer_id",
        "event_ts": "updated_at",
        "tracked": ["first_name", "last_name", "email", "city", "country", "segment"],
    },
    "products": {
        "key": "product_id",
        "event_ts": "updated_at",
        "tracked": ["product_name", "category", "unit_price", "status"],
    },
}
