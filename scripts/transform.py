"""
============================================================
File Name : transform.py
Project   : Weather Data Engineering Pipeline
Author    : Arul G

Purpose
-------
Transform raw weather JSON data into a clean Spark DataFrame
ready for loading into PostgreSQL.

Responsibilities
----------------
1. Read a raw weather JSON file.
2. Flatten nested JSON fields.
3. Select only required columns.
4. Apply appropriate data types.
5. Return a clean Spark DataFrame.

Workflow
--------
Raw JSON
    ↓
Read JSON
    ↓
Flatten Nested Fields
    ↓
Select & Cast Required Columns
    ↓
Return Clean DataFrame
============================================================
"""

# ==========================================================
# PySpark Imports
# ==========================================================

from pyspark.sql.functions import col
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
)

# ==========================================================
# Local Imports
# ==========================================================

from scripts.logger import logger


# ==========================================================
# Weather Transformation
# ==========================================================

def transform_weather(spark, raw_file_path):
    """
    Transform a raw weather JSON file into a clean Spark DataFrame.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.

    raw_file_path : Path | str
        Path to the extracted weather JSON file.

    Returns
    -------
    pyspark.sql.DataFrame
        Flattened and cleaned weather data.
    """

    logger.info("Starting weather transformation...")

    try:

        # ==================================================
        # Read Raw JSON File
        # ==================================================

        logger.info(f"Reading raw weather file: {raw_file_path}")

        df = (
            spark.read
            .option("multiLine", "true")
            .json(str(raw_file_path))
        )

        logger.info("Raw JSON loaded successfully.")

        # ==================================================
        # Flatten Nested JSON Structure
        # ==================================================

        logger.info("Transforming weather data...")

        weather_df = (
            df.select(
                col("name")
                .cast(StringType())
                .alias("city"),

                col("coord.lat")
                .cast(DoubleType())
                .alias("latitude"),

                col("coord.lon")
                .cast(DoubleType())
                .alias("longitude"),

                col("main.temp")
                .cast(DoubleType())
                .alias("temperature"),

                col("main.humidity")
                .cast(IntegerType())
                .alias("humidity"),

                col("main.pressure")
                .cast(IntegerType())
                .alias("pressure"),

                col("wind.speed")
                .cast(DoubleType())
                .alias("wind_speed"),

                col("weather")[0]["main"]
                .cast(StringType())
                .alias("weather"),

                col("weather")[0]["description"]
                .cast(StringType())
                .alias("description"),
            )
        )

        logger.info("Weather transformation completed successfully.")

        return weather_df

    except Exception as error:

        logger.exception(
            f"Weather transformation failed: {error}"
        )

        raise


# ==========================================================
# Local Testing
# ==========================================================

if __name__ == "__main__":

    from pathlib import Path
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("Weather Data Transformation")
        .getOrCreate()
    )

    try:

        latest_file = max(
            Path("data/raw").glob("*.json"),
            key=lambda file: file.stat().st_mtime
        )

        transformed_df = transform_weather(
            spark,
            latest_file
        )

        transformed_df.printSchema()
        transformed_df.show(truncate=False)

    finally:

        spark.stop()