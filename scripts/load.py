"""
load.py

Purpose:
--------
This module is responsible for loading the transformed weather data
into the PostgreSQL database.

Responsibilities:
-----------------
1. Read the existing weather records from PostgreSQL.
2. Compare incoming records with existing records.
3. Remove duplicate records using Spark Left Anti Join.
4. Insert only new records into PostgreSQL.
5. Log the loading process and statistics.

Author: Arul
Project: Weather Data Engineering Pipeline
"""

# ==========================================================
# Import Database Configuration
# ==========================================================

from config.settings import DB_PROPERTIES, DB_URL

# ==========================================================
# Import Custom Logger
# ==========================================================

from scripts.logger import logger


def load_weather(weather_df):
    """
    Load only new weather records into PostgreSQL.

    Parameters
    ----------
    weather_df : pyspark.sql.DataFrame
        Transformed weather DataFrame.

    Returns
    -------
    None
    """

    try:

        logger.info("Starting data loading process...")

        # ==================================================
        # Read Existing PostgreSQL Table
        # ==================================================

        logger.info("Reading existing weather records from PostgreSQL...")

        existing_df = (
            weather_df.sparkSession.read
            .format("jdbc")
            .option("url", DB_URL)
            .option("dbtable", "weather_data")
            .option("user", DB_PROPERTIES["user"])
            .option("password", DB_PROPERTIES["password"])
            .option("driver", DB_PROPERTIES["driver"])
            .load()
        )

        logger.info("Existing records loaded successfully.")

        # ==================================================
        # Columns Used For Duplicate Detection
        # ==================================================

        join_columns = [
            "city",
            "latitude",
            "longitude",
            "temperature",
            "humidity",
            "pressure",
            "wind_speed",
            "weather",
            "description",
        ]

        # ==================================================
        # Find Only New Records
        # ==================================================

        logger.info("Checking for duplicate records...")

        new_weather_df = weather_df.join(
            existing_df,
            on=join_columns,
            how="left_anti"
        )

        # ==================================================
        # Record Statistics
        # ==================================================

        total_records = weather_df.count()
        new_records = new_weather_df.count()
        duplicate_records = total_records - new_records

        logger.info(f"Incoming Records   : {total_records}")
        logger.info(f"Duplicate Records : {duplicate_records}")
        logger.info(f"New Records       : {new_records}")

        # ==================================================
        # Skip Database Write If No New Records
        # ==================================================

        if new_records == 0:
            logger.info("No new records found. Skipping database load.")
            return

        # ==================================================
        # Write Only New Records
        # ==================================================

        logger.info("Writing new records to PostgreSQL...")

        (
            new_weather_df.write
            .format("jdbc")
            .option("url", DB_URL)
            .option("dbtable", "weather_data")
            .option("user", DB_PROPERTIES["user"])
            .option("password", DB_PROPERTIES["password"])
            .option("driver", DB_PROPERTIES["driver"])
            .mode("append")
            .save()
        )

        logger.info(
            f"Successfully inserted {new_records} new record(s) into PostgreSQL."
        )

    except Exception as error:

        logger.exception(
            f"Failed to load data into PostgreSQL: {error}"
        )

        raise