"""
============================================================
File Name : extract.py
Project   : Weather Data Engineering Pipeline
Author    : Arul G
Purpose   : Extract weather data from the OpenWeather API
            and store the raw JSON response locally.
============================================================
"""

# Standard Library Imports
import json
from pathlib import Path
from datetime import datetime

# Third-Party Imports
import requests

# Local Imports
from config.settings import API_KEY
from scripts.logger import logger


# ----------------------------------------------------------
# API Configuration
# ----------------------------------------------------------

CITY = "Chennai"

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"


def extract_weather():
    """
    Extract weather data from the OpenWeather API.

    Workflow:
    1. Send API request
    2. Receive JSON response
    3. Save JSON into data/raw folder
    4. Return saved file path

    Returns
    -------
    Path | None
        Returns the saved JSON file path on success.
        Returns None if extraction fails.
    """

    logger.info("Weather extraction started.")

    # Request parameters sent to OpenWeather
    params = {
        "q": CITY,
        "appid": API_KEY,
        "units": "metric"
    }

    try:

        # Send GET request to API
        response = requests.get(
            BASE_URL,
            params=params,
            timeout=10
        )

        # Raise an exception if request failed
        response.raise_for_status()

        # Convert response into Python dictionary
        weather_data = response.json()

        # Create raw data directory if missing
        raw_directory = Path("data/raw")
        raw_directory.mkdir(
            parents=True,
            exist_ok=True
        )

        # Generate unique filename using timestamp
        timestamp = datetime.now().strftime(
            "%Y%m%d_%H%M%S"
        )

        file_path = (
            raw_directory /
            f"weather_{timestamp}.json"
        )

        # Save JSON response to disk
        with open(
            file_path,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                weather_data,
                file,
                indent=4
            )

        logger.info(
            f"Raw weather data saved successfully: {file_path}"
        )

        return file_path

    except requests.exceptions.RequestException as error:

        logger.exception(
            f"API request failed: {error}"
        )

        return None

    except Exception as error:

        logger.exception(
            f"Unexpected extraction error: {error}"
        )

        return None


# ----------------------------------------------------------
# Script Entry Point
# ----------------------------------------------------------

if __name__ == "__main__":

    extract_weather()