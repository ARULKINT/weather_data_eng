"""
============================================================
File Name : validator.py
Project   : Weather Data Engineering Pipeline

Purpose:
    Validate weather data before transformation and loading.
============================================================
"""

# ==========================================================
# Local Imports
# ==========================================================

from scripts.logger import logger


def validate_weather_data(weather_data: dict) -> bool:
    """
    Validate raw weather data received from the API.

    Parameters
    ----------
    weather_data : dict
        Raw JSON response from the weather API.

    Returns
    -------
    bool
        True if the data is valid.

    Raises
    ------
    ValueError
        If any validation fails.
    """

    logger.info("Validating weather data...")

    # -----------------------------
    # City Name
    # -----------------------------
    city = weather_data.get("name")

    if not city:
        raise ValueError("City name is missing.")

    # -----------------------------
    # Coordinates
    # -----------------------------
    coord = weather_data.get("coord", {})

    latitude = coord.get("lat")
    longitude = coord.get("lon")

    if latitude is None or not (-90 <= latitude <= 90):
        raise ValueError("Invalid latitude.")

    if longitude is None or not (-180 <= longitude <= 180):
        raise ValueError("Invalid longitude.")

    # -----------------------------
    # Main Weather Information
    # -----------------------------
    main = weather_data.get("main", {})

    temperature = main.get("temp")
    humidity = main.get("humidity")
    pressure = main.get("pressure")

    if temperature is None:
        raise ValueError("Temperature is missing.")

    if humidity is None or not (0 <= humidity <= 100):
        raise ValueError("Invalid humidity.")

    if pressure is None or pressure <= 0:
        raise ValueError("Invalid pressure.")

    # -----------------------------
    # Wind
    # -----------------------------
    wind = weather_data.get("wind", {})

    wind_speed = wind.get("speed")

    if wind_speed is None or wind_speed < 0:
        raise ValueError("Invalid wind speed.")

    # -----------------------------
    # Weather Description
    # -----------------------------
    weather = weather_data.get("weather", [])

    if not weather:
        raise ValueError("Weather information is missing.")

    if not weather[0].get("main"):
        raise ValueError("Weather type is missing.")

    if not weather[0].get("description"):
        raise ValueError("Weather description is missing.")

    logger.info("Weather data validation successful.")

    return True