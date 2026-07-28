"""
============================================================
File Name : main.py
Project   : Weather Data Engineering Pipeline
Author    : Arul G

Purpose
-------
Execute the complete ETL and Data Warehouse pipeline.

Workflow
--------
Extract Weather Data
        ↓
Transform Data
        ↓
Load Staging Table
        ↓
Build dim_city
        ↓
Build dim_weather
        ↓
Build fact_weather
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

from scripts.warehouse.dim_city import load_dim_city
from scripts.warehouse.dim_weather import load_dim_weather
from scripts.warehouse.fact_weather import load_fact_weather


# ==========================================================
# Main Function
# ==========================================================

def main():

    logger.info("=" * 80)
    logger.info("Weather Data Engineering Pipeline Started")
    logger.info("=" * 80)

    spark = None

    try:

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
        # STEP 1 : EXTRACT
        # ==================================================

        logger.info("=" * 80)
        logger.info("STEP 1 : EXTRACT")
        logger.info("=" * 80)

        raw_files = extract_weather()

        if not raw_files:
            raise RuntimeError("No weather files extracted.")

        logger.info(
            "Extracted %d weather files.",
            len(raw_files)
        )

        # ==================================================
        # STEP 2 : TRANSFORM & LOAD STAGING
        # ==================================================

        logger.info("=" * 80)
        logger.info("STEP 2 : LOAD STAGING TABLE")
        logger.info("=" * 80)

        for raw_file in raw_files:

            logger.info("Processing %s", raw_file.name)

            weather_df = transform_weather(
                spark,
                raw_file
            )

            load_weather(weather_df)

        logger.info("Staging table loaded successfully.")

        # ==================================================
        # STEP 3 : BUILD DIM_CITY
        # ==================================================

        logger.info("=" * 80)
        logger.info("STEP 3 : BUILD DIM_CITY")
        logger.info("=" * 80)

        load_dim_city(spark)

        # ==================================================
        # STEP 4 : BUILD DIM_WEATHER
        # ==================================================

        logger.info("=" * 80)
        logger.info("STEP 4 : BUILD DIM_WEATHER")
        logger.info("=" * 80)

        load_dim_weather(spark)

        # ==================================================
        # STEP 5 : BUILD FACT_WEATHER
        # ==================================================

        logger.info("=" * 80)
        logger.info("STEP 5 : BUILD FACT_WEATHER")
        logger.info("=" * 80)

        load_fact_weather(spark)

        logger.info("=" * 80)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)

    except Exception as error:

        logger.exception(
            "Pipeline failed: %s",
            error
        )
        raise

    finally:

        if spark:

            logger.info("Stopping Spark Session...")

            spark.stop()

            logger.info("Spark Session stopped.")

        logger.info("=" * 80)
        logger.info("Pipeline Finished")
        logger.info("=" * 80)


# ==========================================================
# Entry Point
# ==========================================================

if __name__ == "__main__":
    main()