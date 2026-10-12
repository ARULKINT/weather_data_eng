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

from pathlib import Path
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

        # ==================================================
        # PostgreSQL JDBC Driver
        # ==================================================

        jdbc_path = (
            Path(__file__).resolve().parent.parent
            / "drivers"
            / "postgresql-42.7.13.jar"
        )

        logger.info(f"JDBC Path   : {jdbc_path}")
        logger.info(f"JDBC Exists : {jdbc_path.exists()}")

        if not jdbc_path.exists():
            raise FileNotFoundError(
                f"PostgreSQL JDBC Driver not found:\n{jdbc_path}"
            )

        jdbc_jar = str(jdbc_path.resolve())

        # ==================================================
        # Create Spark Session
        # ==================================================

        spark = (
            SparkSession.builder
            .appName("Weather Data Engineering Pipeline")
            .master("local[*]")

            # PostgreSQL JDBC Driver
            .config("spark.jars", jdbc_jar)
            .config("spark.driver.extraClassPath", jdbc_jar)

            # Spark Configuration
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.warehouse.dir", str(Path.cwd() / "spark-warehouse"))

            # Windows Compatibility
            .config("spark.hadoop.hadoop.native.lib", "false")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")

            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("ERROR")

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

            logger.info(f"Processing {raw_file.name}")

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

        if spark is not None:

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