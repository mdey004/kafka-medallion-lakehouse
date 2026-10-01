"""Gold layer: star schema (dimensions + fact) and business aggregates.

All builders are pure DataFrame -> DataFrame functions. ``jobs.py`` handles I/O.

Star schema::

            dim_date
               |
  dim_customer - fct_orders - dim_product
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

UNKNOWN_SK = -1
# Orders with these statuses produce no revenue.
NON_REVENUE_STATUSES = ("cancelled", "returned")


# --------------------------------------------------------------------------- dimensions
def _with_unknown_member(dim: DataFrame, sk: str, key: str) -> DataFrame:
    """Add the -1 'Unknown' row so fact foreign keys are never NULL."""
    spark = dim.sparkSession
    unknown = spark.createDataFrame([(UNKNOWN_SK, UNKNOWN_SK)], f"{sk} bigint, {key} bigint")
    return dim.unionByName(unknown, allowMissingColumns=True)


def build_dim_customer(customers_scd2: DataFrame) -> DataFrame:
    dim = customers_scd2.select(
        F.col("customer_sk").cast("bigint"),
        "customer_id",
        "first_name",
        "last_name",
        F.concat_ws(" ", "first_name", "last_name").alias("full_name"),
        "email",
        "city",
        "country",
        "segment",
        "valid_from",
        "valid_to",
        "is_current",
    )
    return _with_unknown_member(dim, "customer_sk", "customer_id")


def build_dim_product(products_scd2: DataFrame) -> DataFrame:
    dim = products_scd2.select(
        F.col("product_sk").cast("bigint"),
        "product_id",
        "product_name",
        "category",
        F.col("unit_price").alias("list_price"),
        "status",
        "valid_from",
        "valid_to",
        "is_current",
    )
    return _with_unknown_member(dim, "product_sk", "product_id")


def build_dim_date(orders: DataFrame) -> DataFrame:
    """Calendar dimension covering the full range of order dates."""
    bounds = orders.select(
        F.min(F.to_date("order_ts")).alias("start"), F.max(F.to_date("order_ts")).alias("end")
    )
    dates = bounds.select(F.explode(F.sequence("start", "end")).alias("calendar_date"))
    return dates.select(
        F.date_format("calendar_date", "yyyyMMdd").cast("int").alias("date_key"),
        "calendar_date",
        F.year("calendar_date").alias("year"),
        F.quarter("calendar_date").alias("quarter"),
        F.month("calendar_date").alias("month"),
        F.date_format("calendar_date", "MMMM").alias("month_name"),
        F.dayofmonth("calendar_date").alias("day_of_month"),
        F.dayofweek("calendar_date").alias("day_of_week"),
        F.date_format("calendar_date", "EEEE").alias("day_name"),
        F.dayofweek("calendar_date").isin(1, 7).alias("is_weekend"),
    )


# --------------------------------------------------------------------------- fact
def _point_in_time(dim: DataFrame, alias: str, key: str):
    """Join condition picking the dimension version valid at ``o.order_ts``."""
    d = F.col
    return (
        (d(f"o.{key}") == d(f"{alias}.{key}"))
        & (d("o.order_ts") >= d(f"{alias}.valid_from"))
        & (d(f"{alias}.valid_to").isNull() | (d("o.order_ts") < d(f"{alias}.valid_to")))
    )


def build_fct_orders(orders: DataFrame, dim_customer: DataFrame, dim_product: DataFrame) -> DataFrame:
    """Order-line grain fact with point-in-time (SCD2-aware) surrogate keys."""
    o = orders.alias("o")
    c = dim_customer.filter(F.col("customer_sk") != UNKNOWN_SK).alias("c")
    p = dim_product.filter(F.col("product_sk") != UNKNOWN_SK).alias("p")

    is_revenue = ~F.col("o.order_status").isin(*NON_REVENUE_STATUSES)
    gross = (F.col("o.quantity") * F.col("o.unit_price")).cast("decimal(14,2)")

    return (
        o.join(c, _point_in_time(c, "c", "customer_id"), "left")
        .join(p, _point_in_time(p, "p", "product_id"), "left")
        .select(
            F.col("o.order_id"),
            F.date_format("o.order_ts", "yyyyMMdd").cast("int").alias("date_key"),
            F.coalesce("c.customer_sk", F.lit(UNKNOWN_SK)).alias("customer_sk"),
            F.coalesce("p.product_sk", F.lit(UNKNOWN_SK)).alias("product_sk"),
            F.col("o.customer_id"),
            F.col("o.product_id"),
            F.col("o.order_status"),
            F.col("o.order_ts"),
            F.col("o.quantity"),
            F.col("o.unit_price"),
            gross.alias("gross_amount"),
            F.when(is_revenue, gross).otherwise(F.lit(0).cast("decimal(14,2)")).alias("net_revenue"),
            is_revenue.alias("is_revenue"),
        )
    )


# --------------------------------------------------------------------------- aggregates
def build_agg_daily_sales(fct: DataFrame, dim_date: DataFrame) -> DataFrame:
    revenue = fct.filter("is_revenue")
    daily = revenue.groupBy("date_key").agg(
        F.countDistinct("order_id").alias("orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum("quantity").alias("units_sold"),
        F.sum("net_revenue").alias("revenue"),
    )
    return (
        dim_date.select("date_key", "calendar_date", "year", "month", "day_name")
        .join(daily, "date_key", "left")
        .fillna(0, subset=["orders", "unique_customers", "units_sold"])
        .withColumn("revenue", F.coalesce("revenue", F.lit(0).cast("decimal(24,2)")))
        .withColumn(
            "avg_order_value",
            F.when(F.col("orders") > 0, F.round(F.col("revenue") / F.col("orders"), 2)),
        )
        .orderBy("date_key")
    )


def build_agg_customer_ltv(fct: DataFrame, dim_customer: DataFrame) -> DataFrame:
    revenue = fct.filter("is_revenue")
    stats = revenue.groupBy("customer_id").agg(
        F.min(F.to_date("order_ts")).alias("first_order_date"),
        F.max(F.to_date("order_ts")).alias("last_order_date"),
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_units"),
        F.sum("net_revenue").alias("lifetime_revenue"),
    )
    current = dim_customer.filter("is_current").select("customer_id", "full_name", "segment", "country")
    return (
        stats.join(current, "customer_id", "left")
        .withColumn("avg_order_value", F.round(F.col("lifetime_revenue") / F.col("total_orders"), 2))
        .withColumn("ltv_rank", F.dense_rank().over(Window.orderBy(F.col("lifetime_revenue").desc())))
    )


def build_agg_product_performance(fct: DataFrame, dim_product: DataFrame) -> DataFrame:
    revenue = fct.filter("is_revenue")
    stats = revenue.groupBy("product_id").agg(
        F.countDistinct("order_id").alias("orders"),
        F.sum("quantity").alias("units_sold"),
        F.sum("net_revenue").alias("revenue"),
        F.round(F.avg("unit_price"), 2).alias("avg_selling_price"),
    )
    current = dim_product.filter("is_current").select("product_id", "product_name", "category", "list_price")
    return stats.join(current, "product_id", "left").withColumn(
        "rank_in_category",
        F.dense_rank().over(Window.partitionBy("category").orderBy(F.col("revenue").desc())),
    )
