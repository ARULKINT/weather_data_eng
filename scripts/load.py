"""
load.py

Purpose
-------
Load the transformed weather data into the PostgreSQL database.

Responsibilities
----------------
1. Receive the transformed Spark DataFrame.
2. Count the number of records to be inserted.
3. Write all weather observations into PostgreSQL.
4. Log every stage of the loading process.
5. Handle and log database errors.

Author
------
Arul

Project
-------
Weather Data Engineering Pipeline
"""

# ==========================================================
# Import Database Configuration
# ==========================================================

from config.settings import DB_PROPERTIES, DB_URL

# ==========================================================
# Import Custom Logger
# ==========================================================

from scripts.logger import logger


# ==========================================================
# Load Weather Data into PostgreSQL
# ==========================================================

def load_weather(weather_df):
    """
    Load transformed weather observations into PostgreSQL.

    This function writes every transformed weather record
    into the PostgreSQL database.

    Since the project now supports historical weather storage,
    every ETL execution is stored as a new observation.

    Parameters
    ----------
    weather_df : pyspark.sql.DataFrame
        Spark DataFrame containing transformed weather data.

    Returns
    -------
    None
    """

    try:

        # ==================================================
        # Start Loading Process
        # ==================================================

        logger.info("Starting data loading process...")

        # ==================================================
        # Count Incoming Records
        # ==================================================

        total_records = weather_df.count()

        logger.info(
            f"Total records to insert: {total_records}"
        )

        # ==================================================
        # Write Data into PostgreSQL
        # ==================================================

        logger.info(
            "Writing weather records into PostgreSQL..."
        )

        (
            weather_df.write
            .format("jdbc")
            .option("url", DB_URL)
            .option("dbtable", "weather_data")
            .option("user", DB_PROPERTIES["user"])
            .option("password", DB_PROPERTIES["password"])
            .option("driver", DB_PROPERTIES["driver"])
            .mode("append")
            .save()
        )

        # ==================================================
        # Successful Load
        # ==================================================

        logger.info(
            f"Successfully inserted {total_records} record(s) into PostgreSQL."
        )

    except Exception as error:

        # ==================================================
        # Log Database Error
        # ==================================================

        logger.exception(
            f"Failed to load data into PostgreSQL: {error}"
        )

        raise