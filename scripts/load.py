"""
load.py

Purpose:
--------
This module is responsible for loading the transformed weather data
into the PostgreSQL database.

Responsibilities:
-----------------
1. Receive the transformed Spark DataFrame.
2. Write the DataFrame to PostgreSQL using JDBC.
3. Log the loading process.
4. Raise exceptions if loading fails.

Author: Arul
Project: Weather Data Engineering Pipeline
"""

# Import database configuration
from config.settings import DB_PROPERTIES, DB_URL

# Import custom logger
from scripts.logger import logger


def load_weather(weather_df):
    """
    Load the transformed weather data into PostgreSQL.

    Parameters
    ----------
    weather_df : pyspark.sql.DataFrame
        The cleaned and transformed Spark DataFrame.

    Returns
    -------
    None
    """

    try:
        logger.info("Starting data loading process...")

        # Write the DataFrame into PostgreSQL
        (
            weather_df.write
            .format("jdbc")
            .option("url", DB_URL)
            .option("dbtable", "weather_data")
            .option("user", DB_PROPERTIES["user"])
            .option("password", DB_PROPERTIES["password"])
            .option("driver", DB_PROPERTIES["driver"])
            .mode("append")          # Append new records without overwriting existing data
            .save()
        )

        logger.info("Data successfully loaded into PostgreSQL.")

    except Exception as error:
        # Log the error and re-raise it for the calling module (main.py)
        logger.error(f"Failed to load data into PostgreSQL: {error}")
        raise