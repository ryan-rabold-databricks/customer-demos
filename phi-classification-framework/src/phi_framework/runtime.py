"""Runtime helpers for job entry points."""
import logging


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def get_spark():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()
