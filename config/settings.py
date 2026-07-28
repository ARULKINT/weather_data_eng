"""
============================================================
File Name : settings.py
Project   : Weather Data Engineering Pipeline
Author    : Arul G

Purpose:
    Store all project configuration variables.
============================================================
"""

from dotenv import load_dotenv
import os

# ==========================================================
# Load Environment Variables
# ==========================================================

load_dotenv()

# ==========================================================
# Weather API Configuration
# ==========================================================

API_KEY = os.getenv("OPENWEATHER_API_KEY")

# ==========================================================
# PostgreSQL Configuration
# ==========================================================

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# ==========================================================
# PostgreSQL JDBC URL
# ==========================================================

DB_URL = (
    f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ==========================================================
# PostgreSQL JDBC Properties
# ==========================================================

DB_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# ==========================================================
# Weather Locations
# ==========================================================

CITIES = [
    "Chennai",
    "Delhi",
    "Mumbai",
    "Bangalore",
    "Hyderabad",
    "Pune",
    "Kolkata"
]