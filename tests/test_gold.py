from datetime import datetime
from decimal import Decimal

import pytest

from medallion.gold import model

SCD_CUSTOMERS = (
    "customer_sk bigint, customer_id bigint, first_name string, last_name string, email string, "
    "city string, country string, segment string, "
    "valid_from timestamp, valid_to timestamp, is_current boolean"
)
SCD_PRODUCTS = (
    "product_sk bigint, product_id bigint, product_name string, category string, unit_price decimal(12,2), "
    "status string, valid_from timestamp, valid_to timestamp, is_current boolean"
)
ORDERS = (
    "order_id string, customer_id bigint, product_id bigint, quantity int, unit_price decimal(12,2), "
    "order_status string, order_ts timestamp"
)


@pytest.fixture
def star(spark):
    customers = spark.createDataFrame(
        [
            (
                11,
                1,
                "Asha",
                "Patel",
                "a@x.com",
                "London",
                "UK",
                "consumer",
                datetime(2026, 1, 1),
                datetime(2026, 2, 1),
                False,
            ),
            (
                12,
                1,
                "Asha",
                "Patel",
                "a@x.com",
                "Berlin",
                "DE",
                "corporate",
                datetime(2026, 2, 1),
                None,
                True,
            ),
        ],
        SCD_CUSTOMERS,
    )
    products = spark.createDataFrame(
        [
            (21, 100, "Lamp", "home", Decimal("10.00"), "active", datetime(2026, 1, 1), None, True),
        ],
        SCD_PRODUCTS,
    )
    orders = spark.createDataFrame(
        [
            ("O1", 1, 100, 2, Decimal("10.00"), "delivered", datetime(2026, 1, 15, 12)),  # London version
            ("O2", 1, 100, 1, Decimal("10.00"), "shipped", datetime(2026, 2, 10, 12)),  # Berlin version
            ("O3", 1, 100, 5, Decimal("10.00"), "cancelled", datetime(2026, 2, 10, 12)),  # no revenue
            ("O4", 99, 100, 1, Decimal("10.00"), "delivered", datetime(2026, 2, 11, 12)),  # unknown customer
        ],
        ORDERS,
    )

    dim_c = model.build_dim_customer(customers)
    dim_p = model.build_dim_product(products)
    dim_date = model.build_dim_date(orders)
    fct = model.build_fct_orders(orders, dim_c, dim_p)
    return dim_c, dim_p, dim_date, fct


def test_fact_uses_point_in_time_customer_version(star):
    _, _, _, fct = star
    sk = {r.order_id: r.customer_sk for r in fct.collect()}
    assert sk == {"O1": 11, "O2": 12, "O3": 12, "O4": model.UNKNOWN_SK}


def test_fact_grain_and_revenue(star):
    _, _, _, fct = star
    rows = {r.order_id: r for r in fct.collect()}
    assert fct.count() == 4
    assert rows["O1"].gross_amount == Decimal("20.00")
    assert rows["O3"].net_revenue == Decimal("0.00")
    assert rows["O1"].date_key == 20260115


def test_dimensions_have_unknown_member(star):
    dim_c, dim_p, _, _ = star
    assert dim_c.filter(f"customer_sk = {model.UNKNOWN_SK}").count() == 1
    assert dim_p.filter(f"product_sk = {model.UNKNOWN_SK}").count() == 1


def test_dim_date_covers_order_range(star):
    _, _, dim_date, _ = star
    keys = [r.date_key for r in dim_date.orderBy("date_key").collect()]
    assert keys[0] == 20260115 and keys[-1] == 20260211
    assert len(keys) == len(set(keys)) == 28


def test_daily_sales_excludes_cancelled(star):
    _, _, dim_date, fct = star
    day = model.build_agg_daily_sales(fct, dim_date).filter("date_key = 20260210").collect()[0]
    assert (day.orders, day.units_sold, day.revenue) == (1, 1, Decimal("10.00"))


def test_customer_ltv_uses_current_attributes(star):
    _, _, _, fct = star
    dim_c = star[0]
    ltv = {r.customer_id: r for r in model.build_agg_customer_ltv(fct, dim_c).collect()}
    assert ltv[1].lifetime_revenue == Decimal("30.00")
    assert ltv[1].total_orders == 2
    assert ltv[1].segment == "corporate"
    assert ltv[1].ltv_rank == 1


def test_product_performance(star):
    _, dim_p, _, fct = star
    row = model.build_agg_product_performance(fct, dim_p).collect()[0]
    assert (row.product_name, row.orders, row.units_sold, row.revenue) == ("Lamp", 3, 4, Decimal("40.00"))
