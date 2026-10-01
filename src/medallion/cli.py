"""Command-line entry point.

python -m medallion.cli bronze|silver|gold|all [--continuous]
"""

from __future__ import annotations

import argparse

from medallion.bronze import run_bronze
from medallion.config import PipelineConfig
from medallion.gold.jobs import run_gold
from medallion.silver.jobs import run_silver
from medallion.spark import get_spark


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Kafka -> Delta medallion pipeline")
    parser.add_argument("layer", choices=["bronze", "silver", "gold", "all"])
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Run streams continuously instead of draining available data (availableNow).",
    )
    args = parser.parse_args(argv)

    cfg = PipelineConfig.from_env()
    spark = get_spark(with_kafka=args.layer in ("bronze", "all"))
    available_now = not args.continuous

    if args.layer in ("bronze", "all"):
        run_bronze(spark, cfg, available_now=available_now)
    if args.layer in ("silver", "all"):
        run_silver(spark, cfg, available_now=available_now)
    if args.layer in ("gold", "all"):
        run_gold(spark, cfg)


if __name__ == "__main__":
    main()
