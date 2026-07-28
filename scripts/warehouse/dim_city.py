"""
Module: dim_city.py

Purpose
-------
Build and maintain the City Dimension (dim_city).

Responsibilities
----------------
1. Read city information from the weather_data staging table.
2. Remove duplicate city records.
3. Compare against the existing City Dimension.
4. Insert only new city records.
5. Return the newly inserted records.

This module performs an incremental dimension load, making the ETL
pipeline safe to execute multiple times without creating duplicates.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from scripts.logger import logger
from scripts.warehouse.database import read_table, write_table


def load_dim_city(spark: SparkSession) -> DataFrame:
    """
    Load new city records into the City Dimension.

    Workflow
    --------
    1. Read weather_data staging table.
    2. Extract unique city records.
    3. Read existing dim_city table.
    4. Identify new cities using a LEFT ANTI JOIN.
    5. Insert only new city records into dim_city.
    6. Return the newly inserted records.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.

    Returns
    -------
    DataFrame
        DataFrame containing newly inserted city records.
    """

    logger.info("=" * 80)
    logger.info("Starting City Dimension Load")
    logger.info("=" * 80)

    try:
        # ==============================================================
        # STEP 1
        # Read the staging table.
        # ==============================================================
        weather_df = read_table(
            spark=spark,
            table_name="weather_data",
        )

        # ==============================================================
        # STEP 2
        # Extract unique city information.
        # ==============================================================
        city_df = (
            weather_df
            .select(
                "city",
                "latitude",
                "longitude",
            )
            .dropDuplicates()
        )

        logger.info(
            "Unique cities extracted: %d",
            city_df.count(),
        )

        # ==============================================================
        # STEP 3
        # Read the existing City Dimension.
        # ==============================================================
        dim_city_df = read_table(
            spark=spark,
            table_name="dim_city",
        )

        logger.info(
            "Existing cities in dimension: %d",
            dim_city_df.count(),
        )

        # ==============================================================
        # STEP 4
        # Identify only new cities.
        #
        # LEFT ANTI JOIN returns rows from the left DataFrame that have
        # no matching rows in the right DataFrame.
        # ==============================================================
        new_city_df = city_df.join(
            dim_city_df,
            city_df.city == dim_city_df.city_name,
            how="left_anti",
        )

        # ==============================================================
        # STEP 5
        # Rename columns to match the warehouse schema.
        # ==============================================================
        new_city_df = (
            new_city_df
            .withColumnRenamed("city", "city_name")
            .select(
                "city_name",
                "latitude",
                "longitude",
            )
            .orderBy(col("city_name"))
        )

        new_city_count = new_city_df.count()

        logger.info(
            "New cities identified: %d",
            new_city_count,
        )

        # ==============================================================
        # STEP 6
        # Stop if there are no new records.
        # ==============================================================
        if new_city_count == 0:
            logger.info(
                "No new city records found. Skipping insert."
            )

            return new_city_df

        # ==============================================================
        # STEP 7
        # Display the new records before inserting.
        # ==============================================================
        logger.info("Preview of new city records:")

        new_city_df.show(truncate=False)

        # ==============================================================
        # STEP 8
        # Write new records into PostgreSQL.
        # ==============================================================
        write_table(
            dataframe=new_city_df,
            table_name="dim_city",
        )

        logger.info(
            "Successfully loaded %d new city records.",
            new_city_count,
        )

        return new_city_df

    except Exception as error:
        logger.exception(
            "City Dimension loading failed: %s",
            error,
        )
        raise