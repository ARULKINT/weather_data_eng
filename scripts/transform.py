"""
============================================================
File Name : transform.py
Project   : Weather Data Engineering Pipeline
Author    : Arul G

Purpose:
    Transform raw weather JSON data into a clean
    Spark DataFrame ready for loading into PostgreSQL.

Workflow:
    Read JSON
        ↓
    Flatten Nested Columns
        ↓
    Create Clean DataFrame
        ↓
    Return DataFrame
============================================================
"""

# ==========================================================
# Third-Party Imports
# ==========================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ==========================================================
# Local Imports
# ==========================================================

from scripts.logger import logger


def transform_weather():
    """
    Transform raw weather JSON files.

    Returns
    -------
    pyspark.sql.DataFrame
        Clean and flattened Spark DataFrame.
    """

    logger.info("Weather transformation started.")

    # ------------------------------------------------------
    # Create Spark Session
    # ------------------------------------------------------

    spark = (
        SparkSession.builder
        .appName("Weather Data Transformation")
        .getOrCreate()
    )

    try:

        # --------------------------------------------------
        # Read all JSON files from data/raw directory
        # --------------------------------------------------

        logger.info("Reading raw JSON files...")

        df = (
            spark.read
            .option("multiLine", "true")
            .json("data/raw/*.json")
        )

        logger.info("JSON files loaded successfully.")

        # --------------------------------------------------
        # Select required columns and flatten nested fields
        # --------------------------------------------------

        logger.info("Flattening nested JSON structure...")

        weather_df = df.select(
            col("name").alias("city"),
            col("coord.lat").alias("latitude"),
            col("coord.lon").alias("longitude"),
            col("main.temp").alias("temperature"),
            col("main.humidity").alias("humidity"),
            col("main.pressure").alias("pressure"),
            col("wind.speed").alias("wind_speed"),
            col("weather")[0]["main"].alias("weather"),
            col("weather")[0]["description"].alias("description")
        )

        logger.info("Weather transformation completed successfully.")

        return weather_df

    except Exception as error:

        logger.exception(
            f"Transformation failed: {error}"
        )

        raise



# ==========================================================
# Script Entry Point
# ==========================================================

if __name__ == "__main__":

    transformed_df = transform_weather()

    transformed_df.show(truncate=False)