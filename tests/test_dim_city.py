"""
Module: dim_weather.py

Purpose
-------
Build and maintain the Weather Dimension (dim_weather).

Responsibilities
----------------
1. Read weather information from the weather_data staging table.
2. Remove duplicate weather records.
3. Compare against the existing Weather Dimension.
4. Insert only new weather records.
5. Return the newly inserted records.

This module performs an incremental dimension load, making the ETL
pipeline safe to execute multiple times without creating duplicates.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from scripts.logger import logger
from scripts.warehouse.database import read_table, write_table


def load_dim_weather(spark: SparkSession) -> DataFrame:
    """
    Load new weather records into the Weather Dimension.

    Workflow
    --------
    1. Read weather_data staging table.
    2. Extract unique weather records.
    3. Read existing dim_weather table.
    4. Identify new weather records using a LEFT ANTI JOIN.
    5. Insert only new weather records into dim_weather.
    6. Return the newly inserted records.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.

    Returns
    -------
    DataFrame
        DataFrame containing newly inserted weather records.
    """

    logger.info("=" * 80)
    logger.info("Starting Weather Dimension Load")
    logger.info("=" * 80)

    try:

        # ==========================================================
        # STEP 1
        # Read staging table.
        # ==========================================================
        weather_df = read_table(
            spark=spark,
            table_name="weather_data",
        )

        # ==========================================================
        # STEP 2
        # Extract unique weather information.
        # ==========================================================
        weather_dimension_df = (
            weather_df
            .select(
                "weather",
                "description",
            )
            .dropDuplicates()
        )

        logger.info(
            "Unique weather records extracted: %d",
            weather_dimension_df.count(),
        )

        # ==========================================================
        # STEP 3
        # Read existing Weather Dimension.
        # ==========================================================
        dim_weather_df = read_table(
            spark=spark,
            table_name="dim_weather",
        )

        logger.info(
            "Existing weather records: %d",
            dim_weather_df.count(),
        )

        # ==========================================================
        # STEP 4
        # Find only new weather records.
        #
        # Match using BOTH weather and description because the
        # combination is unique.
        # ==========================================================
        new_weather_df = weather_dimension_df.join(
            dim_weather_df,
            (
                (weather_dimension_df.weather == dim_weather_df.weather)
                &
                (
                    weather_dimension_df.description
                    == dim_weather_df.description
                )
            ),
            how="left_anti",
        )

        # ==========================================================
        # STEP 5
        # Arrange columns.
        # ==========================================================
        new_weather_df = (
            new_weather_df
            .select(
                "weather",
                "description",
            )
            .orderBy(col("weather"))
        )

        new_weather_count = new_weather_df.count()

        logger.info(
            "New weather records identified: %d",
            new_weather_count,
        )

        # ==========================================================
        # STEP 6
        # Exit if no new weather records exist.
        # ==========================================================
        if new_weather_count == 0:

            logger.info(
                "No new weather records found. Skipping insert."
            )

            return new_weather_df

        # ==========================================================
        # STEP 7
        # Preview new weather records.
        # ==========================================s================
        logger.info("Preview of new weather records:")

        new_weather_df.show(truncate=False)

        # ==========================================================
        # STEP 8
        # Insert into PostgreSQL.
        # ==========================================================
        write_table(
            dataframe=new_weather_df,
            table_name="dim_weather",
        )

        logger.info(
            "Successfully loaded %d new weather records.",
            new_weather_count,
        )

        return new_weather_df

    except Exception as error:

        logger.exception(
            "Weather Dimension loading failed: %s",
            error,
        )

        raise