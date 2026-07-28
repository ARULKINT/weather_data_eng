"""
============================================================
File Name : main.py
Project   : Weather Data Engineering Pipeline
Author    : Arul G

Purpose
-------
Orchestrate the complete Weather ETL pipeline.

Responsibilities
----------------
1. Create a Spark Session.
2. Extract weather data for all configured cities.
3. Transform each raw JSON file.
4. Load transformed data into PostgreSQL.
5. Stop the Spark Session gracefully.

Workflow
--------
Create Spark Session
        ↓
Extract Weather Data
        ↓
Loop Through Extracted Files
        ↓
Transform Weather Data
        ↓
Load into PostgreSQL
        ↓
Stop Spark Session
============================================================
"""

# ==========================================================
# Third-Party Imports
# ==========================================================

from pyspark.sql import SparkSession

# ==========================================================
# Local Imports
# ==========================================================

from scripts.extract import extract_weather
from scripts.transform import transform_weather
from scripts.load import load_weather
from scripts.logger import logger


# ==========================================================
# Main Function
# ==========================================================

def main():
    """
    Execute the complete Weather ETL pipeline.
    """

    logger.info("=" * 60)
    logger.info("Weather ETL Pipeline Started")
    logger.info("=" * 60)

    spark = None

    try:

        # ==================================================
        # Create Spark Session
        # ==================================================

        logger.info("Creating Spark Session...")

        spark = (
            SparkSession.builder
            .appName("Weather Data Engineering Pipeline")
            .master("local[*]")
            .config(
                "spark.jars",
                "/usr/share/java/postgresql-42.7.10.jar"
            )
            .config(
                "spark.driver.extraClassPath",
                "/usr/share/java/postgresql-42.7.10.jar"
            )
            .getOrCreate()
        )

        logger.info("Spark Session created successfully.")

        # ==================================================
        # Step 1 : Extract
        # ==================================================

        logger.info("Step 1 : Extract")

        raw_files = extract_weather()

        if not raw_files:
            raise RuntimeError(
                "No weather files were extracted."
            )

        logger.info(
            f"{len(raw_files)} weather file(s) extracted."
        )

        # ==================================================
        # Step 2 & 3 : Transform and Load
        # ==================================================

        for raw_file in raw_files:

            logger.info("-" * 60)
            logger.info(f"Processing: {raw_file.name}")

            # ----------------------------------------------
            # Transform
            # ----------------------------------------------

            weather_df = transform_weather(
                spark,
                raw_file
            )

            # ----------------------------------------------
            # Load
            # ----------------------------------------------

            load_weather(weather_df)

            logger.info(
                f"Finished processing {raw_file.name}"
            )

        logger.info("Weather ETL Pipeline completed successfully.")

    except Exception as error:

        logger.exception(
            f"Pipeline failed: {error}"
        )

        raise

    finally:

        # ==================================================
        # Stop Spark Session
        # ==================================================

        if spark:

            logger.info("Stopping Spark Session...")

            spark.stop()

            logger.info("Spark Session stopped.")

        logger.info("=" * 60)
        logger.info("Weather ETL Pipeline Finished")
        logger.info("=" * 60)


# ==========================================================
# Entry Point
# ==========================================================

if __name__ == "__main__":
    main()