"""Gold job: rebuild the star schema and aggregates from Silver in batch.

Gold is a full refresh (overwrite) because the inputs are small and
point-in-time joins depend on the whole SCD2 history. See docs/ARCHITECTURE.md
for when to switch to incremental MERGE.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from medallion.config import PipelineConfig
from medallion.gold import model
from medallion.logging_utils import get_logger

log = get_logger(__name__)


def _write(df: DataFrame, path: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    log.info("gold table written: %s", path)


def run_gold(spark: SparkSession, cfg: PipelineConfig) -> None:
    read = lambda p: spark.read.format("delta").load(p)  # noqa: E731
    customers = read(cfg.silver_path("customers_scd2"))
    products = read(cfg.silver_path("products_scd2"))
    orders = read(cfg.silver_path("orders"))

    dim_customer = model.build_dim_customer(customers).cache()
    dim_product = model.build_dim_product(products).cache()
    dim_date = model.build_dim_date(orders)
    fct_orders = model.build_fct_orders(orders, dim_customer, dim_product).cache()

    outputs = {
        "dim_customer": dim_customer,
        "dim_product": dim_product,
        "dim_date": dim_date,
        "fct_orders": fct_orders,
        "agg_daily_sales": model.build_agg_daily_sales(fct_orders, dim_date),
        "agg_customer_ltv": model.build_agg_customer_ltv(fct_orders, dim_customer),
        "agg_product_performance": model.build_agg_product_performance(fct_orders, dim_product),
    }
    try:
        for name, df in outputs.items():
            _write(df, cfg.gold_path(name))
    finally:
        for df in (dim_customer, dim_product, fct_orders):
            df.unpersist()
