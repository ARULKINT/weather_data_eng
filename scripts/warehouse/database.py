"""
Module: database.py

Purpose
-------
Provide reusable database helper functions for the Data Warehouse.

Why this module?
----------------
Instead of writing Spark JDBC code in every warehouse module,
all database operations are centralized here.

Benefits
--------
- Reusable
- Easier to maintain
- Cleaner warehouse modules
- Consistent logging
- Easier testing
"""

from pyspark.sql import DataFrame, SparkSession

from config.settings import DB_PROPERTIES, DB_URL
from scripts.logger import logger


def read_table(spark: SparkSession, table_name: str) -> DataFrame:
    """
    Read a PostgreSQL table into a Spark DataFrame.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.

    table_name : str
        Name of the PostgreSQL table.

    Returns
    -------
    DataFrame
        Spark DataFrame containing table data.
    """

    logger.info("=" * 80)
    logger.info("Reading table: %s", table_name)

    try:
        dataframe = spark.read.jdbc(
            url=DB_URL,
            table=table_name,
            properties=DB_PROPERTIES,
        )

        logger.info(
            "Successfully loaded '%s' (%d rows).",
            table_name,
            dataframe.count(),
        )

        return dataframe

    except Exception as error:
        logger.exception(
            "Failed to read table '%s'.",
            table_name,
        )
        raise error


def write_table(
    dataframe: DataFrame,
    table_name: str,
    mode: str = "append",
) -> None:
    """
    Write a Spark DataFrame to PostgreSQL.

    Parameters
    ----------
    dataframe : DataFrame
        DataFrame to be written.

    table_name : str
        Destination PostgreSQL table.

    mode : str
        Spark write mode.
        Default = append.

    Returns
    -------
    None
    """

    logger.info("=" * 80)
    logger.info("Writing data to table: %s", table_name)

    try:
        row_count = dataframe.count()

        dataframe.write.jdbc(
            url=DB_URL,
            table=table_name,
            mode=mode,
            properties=DB_PROPERTIES,
        )

        logger.info(
            "Successfully inserted %d rows into '%s'.",
            row_count,
            table_name,
        )

    except Exception as error:
        logger.exception(
            "Failed to write to table '%s'.",
            table_name,
        )
        raise error