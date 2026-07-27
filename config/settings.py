from dotenv import load_dotenv
import os

# Load variables from .env
load_dotenv()

# Weather API
API_KEY = os.getenv("OPENWEATHER_API_KEY")

# PostgreSQL
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")