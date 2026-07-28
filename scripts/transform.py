"""
============================================================
File Name : transform.py
Project   : Weather Data Engineering Pipeline
Author    : Arul G

Purpose:
    Transform raw weather JSON data into a clean
    Spark DataFrame ready for loading into PostgreSQL.

Responsibilities:
    1. Read raw weather JSON files.
    2. Flatten nested JSON fields.
    3. Select only required columns.
    4. Return the transformed Spark DataFrame.

Workflow:
    Read JSON
        ↓
    Flatten Nested Columns
        ↓
    Select Required Fields
        ↓
    Return DataFrame
============================================================
"""

# ==========================================================
# Third-Party Imports
# ==========================================================

from pyspark.sql.functions import col

# ==========================================================
# Local Imports
# ==========================================================

from scripts.logger import logger


# ==========================================================
# Transformation Function
# ==========================================================

def transform_weather(spark):
    """
    Transform raw weather JSON data into a clean Spark DataFrame.

    Parameters
    ----------
    spark : SparkSession
        Active SparkSession created by main.py.

    Returns
    -------
    pyspark.sql.DataFrame
        Clean and flattened weather DataFrame.
    """

    logger.info("Weather transformation started.")

    try:

        # --------------------------------------------------
        # Read all raw JSON files
        # --------------------------------------------------

        logger.info("Reading raw JSON files...")

        df = (
            spark.read
            .option("multiLine", "true")
            .json("data/raw/*.json")
        )

        logger.info("Raw JSON files loaded successfully.")

        # --------------------------------------------------
        # Flatten nested JSON structure
        # --------------------------------------------------

        logger.info("Flattening weather JSON data...")

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

        logger.info("Weather data transformed successfully.")

        # --------------------------------------------------
        # Return transformed DataFrame
        # --------------------------------------------------

        return weather_df

    except Exception as error:

        logger.exception(
            f"Transformation failed: {error}"
        )

        raise


# ==========================================================
# Script Entry Point (For Testing Only)
# ==========================================================

if __name__ == "__main__":

    from pyspark.sql import SparkSession

    # Create SparkSession only when running this file directly
    spark = (
        SparkSession.builder
        .appName("Weather Data Transformation")
        .getOrCreate()
    )

    try:
        transformed_df = transform_weather(spark)
        transformed_df.show(truncate=False)

    finally:
        spark.stop()