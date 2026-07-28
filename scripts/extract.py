"""
============================================================
File Name : extract.py
Project   : Weather Data Engineering Pipeline
Author    : Arul G

Purpose
-------
Extract weather data for multiple cities from the
OpenWeather API, validate the responses, and store
the raw JSON files locally.

Responsibilities
----------------
1. Read configured city names.
2. Request weather data from OpenWeather API.
3. Validate API responses.
4. Save raw JSON files.
5. Return all saved file paths.

Workflow
--------
Read Cities
      ↓
Loop Through Cities
      ↓
API Request
      ↓
Validate Response
      ↓
Save Raw JSON
      ↓
Return Saved File Paths
============================================================
"""

# ==========================================================
# Standard Library Imports
# ==========================================================

import json
from pathlib import Path
from datetime import datetime

# ==========================================================
# Third-Party Imports
# ==========================================================

import requests

# ==========================================================
# Local Imports
# ==========================================================

from config.settings import API_KEY, CITIES
from scripts.logger import logger
from scripts.validator import validate_weather_data

# ==========================================================
# API Configuration
# ==========================================================

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# ==========================================================
# Extract Weather Data
# ==========================================================


def extract_weather():
    """
    Extract weather data for all configured cities.

    Workflow
    --------
    1. Loop through every configured city.
    2. Send API request.
    3. Validate API response.
    4. Save raw JSON into data/raw.
    5. Return all saved file paths.

    Returns
    -------
    list[Path]
        List containing the saved JSON file paths.
    """

    logger.info("Weather extraction started.")

    # ======================================================
    # Create Raw Data Directory
    # ======================================================

    raw_directory = Path("data/raw")

    raw_directory.mkdir(
        parents=True,
        exist_ok=True
    )

    # ======================================================
    # Store Saved File Paths
    # ======================================================

    saved_files = []

    # ======================================================
    # Process Each City
    # ======================================================

    for city in CITIES:

        logger.info(f"Extracting weather data for: {city}")

        params = {
            "q": city,
            "appid": API_KEY,
            "units": "metric"
        }

        try:

            # --------------------------------------------------
            # Send API Request
            # --------------------------------------------------

            response = requests.get(
                BASE_URL,
                params=params,
                timeout=10
            )

            response.raise_for_status()

            logger.info(
                f"Weather data received successfully for {city}."
            )

            # --------------------------------------------------
            # Convert API Response
            # --------------------------------------------------

            weather_data = response.json()

            # --------------------------------------------------
            # Validate Weather Data
            # --------------------------------------------------

            validate_weather_data(weather_data)

            # --------------------------------------------------
            # Generate File Name
            # --------------------------------------------------

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            safe_city = (
                city.lower()
                .replace(" ", "_")
            )

            file_path = (
                raw_directory /
                f"{safe_city}_{timestamp}.json"
            )

            # --------------------------------------------------
            # Save Raw JSON
            # --------------------------------------------------

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
                f"{city} weather data saved: {file_path}"
            )

            saved_files.append(file_path)

        except requests.exceptions.RequestException as error:

            logger.exception(
                f"API request failed for {city}: {error}"
            )

        except ValueError as error:

            logger.exception(
                f"Validation failed for {city}: {error}"
            )

        except Exception as error:

            logger.exception(
                f"Unexpected extraction error for {city}: {error}"
            )

    # ======================================================
    # Extraction Summary
    # ======================================================

    logger.info(
        f"Weather extraction completed. "
        f"{len(saved_files)} file(s) saved."
    )

    return saved_files


# ==========================================================
# Script Entry Point (For Testing Only)
# ==========================================================

if __name__ == "__main__":

    extracted_files = extract_weather()

    print("\nSaved Files:")

    for file in extracted_files:
        print(file)