"""
============================================================
File Name : test_transform.py
Project   : Weather Data Engineering Pipeline
Author    : Arul G

Purpose:
    Unit tests for transform_weather() in scripts/transform.py.
    Verifies that raw JSON structures are correctly flattened,
    cast to appropriate Spark types, and column-selected.
============================================================
"""

import json
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import SparkSession
from scripts.transform import transform_weather


class TestWeatherTransform(unittest.TestCase):
    """Test suite for PySpark weather transformations."""

    @classmethod
    def setUpClass(cls):
        """Initialize lightweight local Spark session."""
        cls.spark = (
            SparkSession.builder
            .appName("UnitTest-Transform")
            .master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.hadoop.hadoop.native.lib", "false")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        """Stop Spark session."""
        if cls.spark:
            cls.spark.stop()

    def setUp(self):
        """Prepare sample JSON payload and temp file."""
        self.sample_payload = {
            "name": "Bangalore",
            "coord": {"lat": 12.9716, "lon": 77.5946},
            "main": {
                "temp": 24.5,
                "humidity": 68,
                "pressure": 1015,
            },
            "wind": {"speed": 3.6},
            "weather": [
                {
                    "main": "Clouds",
                    "description": "scattered clouds",
                }
            ],
        }

        self.temp_file = tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".json",
            delete=False,
            encoding="utf-8",
        )
        json.dump(self.sample_payload, self.temp_file)
        self.temp_file.close()

    def tearDown(self):
        """Clean up temp file."""
        Path(self.temp_file.name).unlink(missing_ok=True)

    def test_transform_columns_and_schema(self):
        """Ensure transformed DataFrame contains expected columns and row count."""
        df = transform_weather(self.spark, Path(self.temp_file.name))

        self.assertEqual(df.count(), 1)
        expected_columns = {
            "city",
            "latitude",
            "longitude",
            "temperature",
            "humidity",
            "pressure",
            "wind_speed",
            "weather",
            "description",
        }
        self.assertEqual(set(df.columns), expected_columns)

        row = df.first()
        self.assertEqual(row["city"], "Bangalore")
        self.assertAlmostEqual(row["latitude"], 12.9716, places=4)
        self.assertAlmostEqual(row["longitude"], 77.5946, places=4)
        self.assertAlmostEqual(row["temperature"], 24.5, places=2)
        self.assertEqual(row["humidity"], 68)
        self.assertEqual(row["pressure"], 1015)
        self.assertAlmostEqual(row["wind_speed"], 3.6, places=2)
        self.assertEqual(row["weather"], "Clouds")
        self.assertEqual(row["description"], "scattered clouds")


if __name__ == "__main__":
    unittest.main()
