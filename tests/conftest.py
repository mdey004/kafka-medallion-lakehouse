from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from medallion.spark import get_spark


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = get_spark("medallion-tests", with_kafka=False)
    session.conf.set("spark.sql.shuffle.partitions", "1")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
