"""
============================================================
File Name : test_validator.py
Project   : Weather Data Engineering Pipeline
Author    : Arul G

Purpose:
    Unit tests for validate_weather_data() in scripts/validator.py.
    Validates boundary conditions, missing fields, and type rules.
============================================================
"""

import unittest
from scripts.validator import validate_weather_data


class TestWeatherValidator(unittest.TestCase):
    """Test suite for weather data validation rules."""

    def setUp(self):
        """Standard valid weather payload for baseline testing."""
        self.valid_payload = {
            "name": "Chennai",
            "coord": {"lat": 13.0827, "lon": 80.2707},
            "main": {
                "temp": 32.5,
                "humidity": 75,
                "pressure": 1012,
            },
            "wind": {"speed": 4.2},
            "weather": [
                {
                    "main": "Rain",
                    "description": "moderate rain",
                }
            ],
        }

    def test_valid_payload_success(self):
        """A complete, accurate weather payload passes validation."""
        self.assertTrue(validate_weather_data(self.valid_payload))

    def test_missing_city_name(self):
        """Missing or empty city name raises ValueError."""
        payload = self.valid_payload.copy()
        payload["name"] = ""
        with self.assertRaises(ValueError) as context:
            validate_weather_data(payload)
        self.assertIn("City name", str(context.exception))

    def test_invalid_latitude_boundaries(self):
        """Latitude outside [-90, 90] raises ValueError."""
        payload = self.valid_payload.copy()
        payload["coord"] = {"lat": 95.0, "lon": 80.0}
        with self.assertRaises(ValueError) as context:
            validate_weather_data(payload)
        self.assertIn("latitude", str(context.exception).lower())

        payload["coord"] = {"lat": -95.0, "lon": 80.0}
        with self.assertRaises(ValueError):
            validate_weather_data(payload)

    def test_invalid_longitude_boundaries(self):
        """Longitude outside [-180, 180] raises ValueError."""
        payload = self.valid_payload.copy()
        payload["coord"] = {"lat": 13.0, "lon": 185.0}
        with self.assertRaises(ValueError) as context:
            validate_weather_data(payload)
        self.assertIn("longitude", str(context.exception).lower())

    def test_missing_temperature(self):
        """Missing temperature field raises ValueError."""
        payload = self.valid_payload.copy()
        payload["main"] = {"humidity": 60, "pressure": 1010}
        with self.assertRaises(ValueError) as context:
            validate_weather_data(payload)
        self.assertIn("temperature", str(context.exception).lower())

    def test_invalid_humidity_percentage(self):
        """Humidity outside [0, 100] raises ValueError."""
        payload = self.valid_payload.copy()
        payload["main"] = {"temp": 28.0, "humidity": 120, "pressure": 1010}
        with self.assertRaises(ValueError) as context:
            validate_weather_data(payload)
        self.assertIn("humidity", str(context.exception).lower())

        payload["main"] = {"temp": 28.0, "humidity": -5, "pressure": 1010}
        with self.assertRaises(ValueError):
            validate_weather_data(payload)

    def test_invalid_pressure(self):
        """Pressure <= 0 or missing raises ValueError."""
        payload = self.valid_payload.copy()
        payload["main"] = {"temp": 28.0, "humidity": 50, "pressure": 0}
        with self.assertRaises(ValueError) as context:
            validate_weather_data(payload)
        self.assertIn("pressure", str(context.exception).lower())

    def test_invalid_wind_speed(self):
        """Wind speed < 0 raises ValueError."""
        payload = self.valid_payload.copy()
        payload["wind"] = {"speed": -2.0}
        with self.assertRaises(ValueError) as context:
            validate_weather_data(payload)
        self.assertIn("wind", str(context.exception).lower())

    def test_missing_weather_condition(self):
        """Empty or malformed weather condition raises ValueError."""
        payload = self.valid_payload.copy()
        payload["weather"] = []
        with self.assertRaises(ValueError) as context:
            validate_weather_data(payload)
        self.assertIn("weather", str(context.exception).lower())

        payload["weather"] = [{"main": "", "description": "clear"}]
        with self.assertRaises(ValueError):
            validate_weather_data(payload)


if __name__ == "__main__":
    unittest.main()
