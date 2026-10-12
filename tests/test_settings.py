"""
============================================================
File Name : test_settings.py
Project   : Weather Data Engineering Pipeline
Author    : Arul G

Purpose:
    Unit tests for configuration variables in config/settings.py.
============================================================
"""

import unittest
from config import settings


class TestSettings(unittest.TestCase):
    """Test suite for pipeline configuration."""

    def test_configured_cities_exist(self):
        """Pipeline must have a defined non-empty list of target cities."""
        self.assertIsInstance(settings.CITIES, list)
        self.assertGreater(len(settings.CITIES), 0)
        self.assertIn("Chennai", settings.CITIES)
        self.assertIn("Delhi", settings.CITIES)

    def test_db_properties_structure(self):
        """DB properties dictionary must contain required JDBC attributes."""
        self.assertIn("driver", settings.DB_PROPERTIES)
        self.assertEqual(settings.DB_PROPERTIES["driver"], "org.postgresql.Driver")
        self.assertIn("user", settings.DB_PROPERTIES)
        self.assertIn("password", settings.DB_PROPERTIES)

    def test_db_url_format(self):
        """JDBC URL must conform to postgresql connection format."""
        self.assertTrue(settings.DB_URL.startswith("jdbc:postgresql://"))


if __name__ == "__main__":
    unittest.main()
